"""
This module has parser functions for the relevant columns.
"""

import datetime
from typing import Optional


def try_parse_date(date_value: str, format="%Y-%m-%d") -> Optional[datetime.date]:
    """
    Returns the parsed date or None if error.
    :param date_value: Date value to be parsed
    :param format: The format for parsing the date
    :return: Parsed date or None if error.
    """
    try:
        return datetime.datetime.strptime(date_value, format).date()
    except ValueError:
        return None


def try_parse_float(value: str) -> Optional[float]:
    """
    Returns the parsed float value.
    :param value: The value to be parsed.
    :return: The float value or None if error.
    """
    try:
        return float(value)
    except ValueError:
        return None


def try_parse_int(value: str) -> Optional[int]:
    """
    Returns the parsed int value.
    :param value: The value to be parsed.
    :return: The int value or None if error.
    """
    try:
        return int(value)
    except ValueError:
        return None
