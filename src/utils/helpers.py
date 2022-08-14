"""Helper functions"""

import pytz
from random import shuffle
from typing import Union, Any
from dateutil.parser import parse
from datetime import datetime, timezone, timedelta, date
from core.logger import logger as log


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
    log.debug(f"Converted {ts} to {ts_value}")
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

    log.debug(f"Enlisted into data len: {len(data)}")
    return data


def build_search_entities(
    entities: list[str],
    start_date: date,
    end_date: date,
    max_chunk_entities: int = 10,
) -> list[str]:
    """Build search entities with start end date and into chunks

    :param list[str] entities: _description_
    :param date start_date: _description_
    :param date end_date: _description_
    :raises ValueError: if start_date is not >= end_date
    :return _type_: list
    """
    # NOTE: shuffle the entities & check dates
    shuffle(entities)
    if start_date >= end_date:
        raise ValueError(
            f"start_date has to be >= end_date by at least 1 day difference"
        )

    search_entities, chunk = [], []
    entities_count = 0
    delta_days = int((end_date - start_date).days)
    start = start_date

    for _ in range(delta_days):
        end = start + timedelta(days=1)

        log.info(f"Building entity with start_date: {start}, end_date: {end}")
        for entity in entities:
            final_entity = insert_start_end_date(entity, start, end)
            chunk.append(final_entity)
            entities_count += 1

            if len(chunk) % max_chunk_entities == 0:
                search_entities.append(chunk)
                chunk = []

        start += timedelta(days=1)
    search_entities.append(chunk)

    log.info(
        f"Created total of search entities: {entities_count}, chunked into {len(search_entities)}"
    )
    return search_entities


def insert_start_end_date(entity: str, start_date: date, end_date: date):
    """Append start end date to search entity

    :param str entity: _description_
    :param date start_date: _description_, defaults to None
    :param date end_date: _description_, defaults to None
    :return str: search entity
    """
    start, end = start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    entity = f"{entity} until:{end} since:{start}"
    log.debug(f"Created search entity: {entity}")
    return entity


def init_start_end_date():
    """Initialize start end date

    :return _type_: _description_
    """
    date_now = datetime.now().astimezone(pytz.UTC).date()
    start_date, end_date = (date_now - timedelta(days=1)), date_now
    log.info(f"Initialized start & end date to {start_date}, {end_date}")
    return start_date, end_date
