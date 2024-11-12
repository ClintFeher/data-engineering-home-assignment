"""
This is the main program of the assignment to perform the desired transformations.
Assumption
* Null in the close is valid and each function has its own assumptions for handling the nulls.
* In general, I could make a pre-prcessing function to create 2 data frames:
  1. Valid dataframe containing only rows where all the columns are not null.
  2. Error dataframe that can be sent to a Kafka error topic.
"""

import os
from typing import Callable, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

import consts.constants as consts
from data.params import (
    aws_access_key_id,
    aws_secret_access_key,
    incoming_table_folder,
    base_folder,
)
from log.logger import logger


def calculate_daily_returns(df: DataFrame, **kwargs) -> DataFrame:
    """
    A transformation to the daily returns
    :param df: Data frame with columns date, ticker, close
    :return: A dataframe with the column return.

    Assumptions:
    Close is null - This can be the current value or the previous value:
       * In case of the previous value, I will take the latest previous column that has a close value.
          * If no previous value found that is not null, the return will be null
       * In case of the current value null, the return value will null.
    """
    window_spec = (
        Window.partitionBy(consts.TICKER)
        .orderBy(consts.DATE)
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    filled = df.withColumn(
        "prev_close_filled", F.last(consts.CLOSE, ignorenulls=True).over(window_spec)
    )
    daily_return = filled.withColumn(
        consts.RETURN,
        F.when(
            F.col("prev_close_filled").isNull() | F.col(consts.CLOSE).isNull(), None
        ).otherwise(
            (F.col(consts.CLOSE) - F.col("prev_close_filled"))
            / F.col("prev_close_filled")
        ),
    )
    daily_return = daily_return.drop("prev_close_filled")
    return daily_return


def calculate_average_daily_return(df: DataFrame, **kwargs) -> DataFrame:
    """
    A transformation to compute:
       1. Compute the average daily return of all stocks for every date
    :param df: A dataframe containing the columns: date, return
    :return: A dataframe with desired calculated column average_return
    * Assumption
       * Ignore null return values.
       * If the whole date has only null values, it will show as null in the output.
    """
    return df.groupby(consts.DATE).agg(
        F.avg(consts.RETURN).alias(consts.AVERAGE_RETURN)
    )


def calculate_stock_with_highest_worth(df: DataFrame, **kwargs) -> DataFrame:
    """
    A transformation to compute:
       2. Which stock was traded with the highest worth -
          as measured by closing price * volume - on average?
    :param df: A dataframe containing the columns: ticker, close, volume
    :return: A dataframe with the desired calculated column value
    * Assumption
       * No null worths in the output
    """
    df = df.withColumn("worth", F.col(consts.CLOSE) * F.col(consts.VOLUME))
    df = df.groupBy(consts.TICKER).agg(F.max("worth").alias("maxWorth"))
    df = df.filter(F.col("maxWorth").isNotNull())
    df = df.orderBy(F.desc("maxWorth")).limit(1).withColumnRenamed("maxWorth", "value")
    df = df.drop("worth")
    return df


def calculate_most_volatile(df: DataFrame, **kwargs) -> DataFrame:
    """
    A transformation to compute:
       3. Which stock was the most volatile as measured by the annualized standard
          deviation of daily returns?
    :param df: A dataframe containing the columns: ticker, return
    :return: A dataframe with the desired calculated column standard_deviation

    * Assumption
      * Null values should be ignored.
    """
    df = df.groupBy(consts.TICKER).agg(
        F.expr("stddev(return) * sqrt(252)").alias("ann_std")
    )
    df = (
        df.orderBy(F.desc("ann_std"))
        .limit(1)
        .withColumnRenamed("ann_std", "standard_deviation")
    )
    return df


def top30_day_return(df: DataFrame, days: int = 30, top: int = 3) -> DataFrame:
    """
    A transformation to compute:
       4. What were the top three 30-day return dates as measured by % increase
          in closing price compared to the closing price 30 days prior? present
          the top three ticker and date combinations.
    :param df: A dataframe containing the columns: ticker, date, close
    :param days: Number of days to use(30 days default)
    :param top: The desired top.
    :return: A dataframe filtered to the top rows where the difference of close
             in the current days interval is greater than the previous days interval.
    * Assumptions:
      1. These are not 30 calendar days, these are 30 trading days in
      comparison to the previous 30 trading days.
      2. If there are no 2 consecutive intervals of 30 days, the row is filtered.
    """

    spec_ticker_date = Window.partitionBy(consts.TICKER).orderBy(consts.DATE)
    close_prev_interval_start = F.lag(consts.CLOSE, days * 2 - 1).over(spec_ticker_date)
    close_prev_interval_end = F.lag(consts.CLOSE, days).over(spec_ticker_date)
    close_cur_interval_start = F.lag(consts.CLOSE, days - 1).over(spec_ticker_date)
    close_cur_interval_end = F.col(consts.CLOSE)
    window_return = df.withColumn(
        "X_day_diff",
        (close_cur_interval_end - close_cur_interval_start)
        - (close_prev_interval_end - close_prev_interval_start),
    )
    window_return = window_return.filter(F.col("X_day_diff").isNotNull())
    spec_top_x = Window.partitionBy(consts.TICKER).orderBy(F.desc("X_day_diff"))
    rowed_window_return = window_return.withColumn(
        "top", F.row_number().over(spec_top_x)
    )
    top_window_return = rowed_window_return.filter(F.expr(f"top <= {top}"))
    top_window_return = top_window_return.drop("top", "X_day_diff")
    return top_window_return


def create_table(
    func: Callable, cols: Optional[list[str]], table_name: str, df: DataFrame, **kwargs
) -> Optional[DataFrame]:
    """
    A decorator to run a transformation and select only the relevant columns for the desired table.
    :param func: The transformation function to run.
    :param table_name: The desired table name to be created.
    :param df: The input dataframe
    :param cols: The desired columns in the output dataframe.
    :return: The processed dataframe with the desired columns.
    """
    data_loc = os.path.join(base_folder, table_name)
    try:
        if df:
            logger.info(
                "Creating table: %s, its data will be located at %s.",
                table_name,
                data_loc,
            )
            df = func(df, **kwargs)
            if cols:
                df = df.select(*cols)
    except Exception:
        logger.exception("Error creating table %s, transformation failed !", table_name)
        return None
    try:
        df.write.mode("overwrite").parquet(data_loc)
        logger.info("Table %s created successfully", table_name)
    except Exception:
        logger.error(
            "Error creating table %s, couldn't create data files of the table",
            table_name,
        )
    return df


if __name__ == "__main__":
    spark = (
        SparkSession.builder.config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2"
        )
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
        .getOrCreate()
    )
    try:
        logger.info("Loading data from %s.", incoming_table_folder)
        incoming_data = spark.read.option("header", "true").csv(incoming_table_folder)
    except Exception as ex:
        logger.exception(
            "Error reading data from incoming folder %s", incoming_table_folder
        )
        raise ex
    incoming_data = incoming_data.repartition(consts.TICKER).sort(consts.DATE)
    daily_return_df = create_table(
        calculate_daily_returns, None, "daily_return", incoming_data
    )
    avg_daily_return = create_table(
        calculate_average_daily_return,
        [consts.DATE, consts.AVERAGE_RETURN],
        "avg_daily_return",
        daily_return_df,
    )
    highest_worth = create_table(
        calculate_stock_with_highest_worth,
        [consts.TICKER, consts.VALUE],
        "highest_worth",
        incoming_data,
    )
    most_volatile = create_table(
        calculate_most_volatile,
        [consts.TICKER, consts.STDDEV],
        "most_volatile",
        daily_return_df,
    )
    top30_day_return = create_table(
        top30_day_return, [consts.TICKER, consts.DATE], "top30", daily_return_df
    )
