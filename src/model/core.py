"""Data model for all core modules
"""
from typing import ClassVar, Union
from abc import ABC, abstractmethod
from attrs import define, field, validators

#
from core.logger import logger as log
from utils.helpers import convert_config, clean_str

# NOTE:
# inputs type
# 1. Twitter
# 2. Kafka
#
# outputs type
# 1. Kafka
# 2. BigQuery


# NOTE: common task model
@define
class Task(ABC):
    """Task baseclass

    :param _type_ ABC: _description_
    """

    def __attrs_post_init__(self):
        log.info(f"config: {self.__dict__}")

    @abstractmethod
    def run(self):
        """Standard run function"""


@define(slots=False)
class Crawler(Task):
    """Crawler class

    :param _type_ Task: _description_
    :raises ValueError: _description_
    """

    @define
    class Mode:
        """Constant class for Crawler mode"""

        SEARCH: ClassVar[str] = "SEARCH"
        TWEET: ClassVar[str] = "TWEET"
        USER: ClassVar[str] = "USER"

    dest: str = field(kw_only=True)
    scraper_id: str = field(kw_only=True, converter=clean_str)
    query: list[str] = field(
        kw_only=True,
        # converter=lambda x: [x] if not isinstance(x, list) else x,
        validator=validators.instance_of(list),
    )
    max_results: int = field(default=10, converter=int, validator=validators.ge(1))

    @scraper_id.validator
    def check_scraper_id(self, _, value):
        """Validator"""
        if value not in [self.Mode.SEARCH, self.Mode.TWEET, self.Mode.USER]:
            raise ValueError(f"Unsupported scraper_id: {value}")


@define(slots=False)
class Queue(Task):
    """Queue class

    :param _type_ Task: _description_
    :raises ValueError: _description_
    :raises KeyError: _description_
    :raises ValueError: _description_
    :raises NotImplementedError: _description_
    :return _type_: _description_
    """

    @define
    class Mode:
        """Constant class for Queue Mode"""

        PRODUCER: ClassVar[str] = "producer"
        CONSUMER: ClassVar[str] = "consumer"
        DEFAULT: ClassVar[str] = "default"

    config: Union[dict, str] = field(kw_only=True, converter=convert_config)
    run_mode: str = field(kw_only=True, converter=str.lower)

    @config.validator
    def check_config(self, _, value):
        """Validator"""
        if not isinstance(value, dict):
            raise ValueError(f"Unsupported config: {value} - {type(value)}")

        config_keys = set(self.get_modes())
        if set(value.keys()).difference(config_keys):
            print(self.run_mode)
            print(value)
            raise KeyError(f"Config must have keys: {config_keys}")

    @run_mode.validator
    def check_run_mode(self, _, value):
        """Validator"""
        if value not in [self.Mode.PRODUCER, self.Mode.CONSUMER]:
            raise ValueError(f"Unsupported mode: {value}")

    def get_modes(self):
        """Get queue all modes

        :return list[str]: list of available modes
        """
        return list([self.Mode.PRODUCER, self.Mode.CONSUMER, self.Mode.DEFAULT])
