"""_summary_
"""
import os
import sys

from typing import ClassVar
from attrs import define, field, validators
from abc import ABC, abstractmethod
from loguru import logger
from snscrape.modules.twitter import (
    TwitterSearchScraper,
    TwitterTweetScraper,
    TwitterUserScraper,
)


#
from base.logger import LOGGER

# NOTE:
# inputs type
# 1. Twitter
# 2. Kafka
#
# outputs type
# 1. Kafka
# 2. BigQuery


class Input:
    pass


class Output:
    pass


# NOTE: common task model
@define
class Task(ABC):
    _log: logger = LOGGER

    def __attrs_post_init__(self):
        self._log.info(f"config: {self.__dict__}")

    @abstractmethod
    def run(self):
        pass


@define(slots=False)
class Crawler(Task):
    dest: str = field(kw_only=True)
    scraper_id: str = field(kw_only=True, converter=str.upper)
    query: list[str] = field(
        kw_only=True,
        converter=lambda x: [x] if not isinstance(x, list) else x,
        validator=validators.instance_of(list),
    )
    max_results: int = field(default=10, converter=int, validator=validators.ge(1))

    def _get_scraper(self) -> object:
        scraper_id = self.scraper_id.strip().upper()
        scraper_ids = {
            "SEARCH": TwitterSearchScraper,
            "TWEET": TwitterTweetScraper,
            "USER": TwitterUserScraper,
        }
        assert (
            scraper_id in scraper_ids.keys()
        ), f"Unsupported scraper type: {scraper_id}"
        return scraper_ids[scraper_id]
