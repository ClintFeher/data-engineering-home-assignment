"""
This module handles the parsing of rdd.
"""

from typing import Tuple

from pyspark import RDD

from consts import constants as consts
from data.parsers import try_parse_date, try_parse_float, try_parse_int
from log.logger import logger


def validate_row(row: str, header: str):
    """
    This function validates a row:
    * Checks that the header matches the required columns including order and uppercases the header.
    * Checks that the date conforms to the required format.
    * Checks that the values of open, high, low and close are floating points and volume is int.
    * Checks that the ticker has only alpha-numeric characters.
    :param row: The row to be checked.
    :param header: The header row.
    :return: The row and the errors in the row.
    """
    header_row = header == row
    columns = row.split(",")
    if len(columns) != 7:
        return row, "Expected 7 columns"
    expected_header_columns = [
        consts.DATE,
        consts.OPEN,
        consts.HIGH,
        consts.LOW,
        consts.CLOSE,
        consts.VOLUME,
        consts.TICKER,
    ]
    parse_func = (
        [try_parse_date]
        + [try_parse_float] * 4
        + [try_parse_int, lambda value: value if value.isalpha() else None]
    )
    for index, (column, expected_column, parse_func) in enumerate(
        zip(columns, expected_header_columns, parse_func)
    ):
        columns[index] = column = column.strip()
        if header_row:
            columns[index] = column = column.upper()
            if column != expected_column:
                return row, "Invalid values in column"
        else:
            value = parse_func(column)
            if value is None:
                return row, "Invalid values in column"
            else:
                columns[index] = value
    return columns, None


def preprocess_rdd(rdd: RDD[str]) -> Tuple[str, RDD[str], RDD[str]]:
    """
    This function checks whether the lines in the RDD are valid.
    :param rdd: The rdd to be checked.
    :return: Tuple(rdd with valid lines, rdd with error lines)
    """
    header = rdd.first()
    upper_case_header = header.upper().split(",")
    rdd = rdd.map(lambda x: validate_row(x, header))
    if rdd.first()[1]:
        logger.error(
            "Found errors in header row, maybe the file is invalid: %s", header
        )
        raise Exception(
            "Found error in header row, this might indicate problems in the format of the file, "
            "terminating."
        )
    error_rows = rdd.filter(lambda x: x[1] is not None)
    valid_rows = rdd.filter(lambda x: x[1] is None and x[0] != upper_case_header)
    valid_rows = valid_rows.map(lambda x: x[0])
    return upper_case_header, valid_rows, error_rows
