"""Helper functions"""

from typing import Union, Any
from dateutil.parser import parse
from datetime import datetime, timezone
import pytz


def timestamp_to_integer(ts: Union[datetime, str]) -> int:
    """Convert timestamp datetime/ string into integer

    :param Union[datetime, str] ts: Timestamp value
    :return int: Converted timestamp integer
    """
    if isinstance(ts, str):
        ts = parse(ts)

    ts = ts.astimezone(pytz.timezone("utc"))
    delta = ts - datetime(1970, 1, 1, tzinfo=timezone.utc)
    ts_value = int(delta.total_seconds()) * 1000000 + int(delta.microseconds)
    return ts_value


def enlist(data: Any):
    """Enlist data

    :param Any data: _description_
    :return _type_: _description_
    """
    if data is None:
        return []

    if not isinstance(data, list):
        if data and not isinstance(data, str):
            data = str(data)
        data = [data]
    return data
