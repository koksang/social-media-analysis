"""Data model for all modules
"""
import os
import sys
import yaml

from typing import ClassVar, Union
from attrs import define, field, validators
from abc import ABC, abstractmethod
from snscrape.modules.twitter import (
    TwitterSearchScraper,
    TwitterTweetScraper,
    TwitterUserScraper,
)

#
from base.logger import LOGGER as log
from util.helpers import convert_config, clean_str

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
    def __attrs_post_init__(self):
        log.info(f"config: {self.__dict__}")

    @abstractmethod
    def run(self):
        pass


@define(slots=False)
class Crawler(Task):
    @define
    class mode:
        SEARCH: ClassVar[str] = "SEARCH"
        TWEET: ClassVar[str] = "TWEET"
        USER: ClassVar[str] = "USER"

    dest: str = field(kw_only=True)
    scraper_id: str = field(kw_only=True, converter=clean_str)
    query: list[str] = field(
        kw_only=True,
        converter=lambda x: [x] if not isinstance(x, list) else x,
        validator=validators.instance_of(list),
    )
    max_results: int = field(default=10, converter=int, validator=validators.ge(1))

    @scraper_id.validator
    def check_scraper_id(self, _, value):
        if value not in [self.mode.SEARCH, self.mode.TWEET, self.mode.USER]:
            raise ValueError(f"Unsupported scraper_id: {value}")

    def _get_scraper(self) -> object:
        scraper_id = self.scraper_id
        scraper_ids = {
            self.mode.SEARCH: TwitterSearchScraper,
            self.mode.TWEET: TwitterTweetScraper,
            self.mode.USER: TwitterUserScraper,
        }
        assert (
            scraper_id in scraper_ids.keys()
        ), f"Unsupported scraper type: {scraper_id}"
        return scraper_ids[scraper_id]


@define(slots=False)
class Queue(Task):
    @define
    class mode:
        PRODUCE: ClassVar[str] = "PRODUCE"
        CONSUME: ClassVar[str] = "CONSUME"

    config: Union[dict, str] = field(kw_only=True, converter=convert_config)
    run_mode: str = field(kw_only=True, converter=clean_str)

    @config.validator
    def check_config(self, _, value):
        if not isinstance(value, dict):
            raise ValueError(f"Unsupported config: {value} - {type(value)}")

        kafka_keys = {"default", "consumer"}
        if not value.keys() >= kafka_keys:
            raise KeyError(f"Config must have keys: {kafka_keys}")

    @run_mode.validator
    def check_run_mode(self, _, value):
        if value not in [self.mode.PRODUCE, self.mode.CONSUME]:
            raise ValueError(f"Unsupported mode: {value}")
        pass
