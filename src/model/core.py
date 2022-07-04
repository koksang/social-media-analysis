"""Data model for all core modules
"""
from typing import ClassVar, Union
from attrs import define, field, validators

#
from model.base import Task
from utils.helpers import convert_config

# NOTE:
# inputs type
# 1. Twitter
# 2. Kafka
#
# outputs type
# 1. Kafka
# 2. BigQuery


@define
class Crawler(Task):
    """Crawler class

    :param _type_ Task: _description_
    :raises ValueError: _description_
    """

    @define
    class Mode:
        """Constant class for Crawler mode"""

        SEARCH: ClassVar[str] = "search"
        TWEET: ClassVar[str] = "tweet"
        USER: ClassVar[str] = "user"

    dest: str = field(kw_only=True)
    mode: str = field(kw_only=True, converter=str.lower)
    query: list[str] = field(
        kw_only=True,
        # converter=lambda x: [x] if not isinstance(x, list) else x,
        validator=validators.instance_of(list),
    )
    max_limits: int = field(default=10, converter=int, validator=validators.ge(1))

    @mode.validator
    def check_mode(self, _, value):
        """Validator"""
        if value not in self.list_modes():
            raise ValueError(f"Unsupported scraper_id: {value}")


@define
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
    mode: str = field(kw_only=True, converter=str.lower)

    @config.validator
    def check_config(self, _, value):
        """Validator"""
        if not isinstance(value, dict):
            raise ValueError(f"Unsupported config: {value} - {type(value)}")

        config_keys = set(self.list_modes())
        if set(value.keys()).difference(config_keys):
            raise KeyError(f"Config must have keys: {config_keys}")

    @mode.validator
    def check_mode(self, _, value):
        """Validator"""
        if value not in self.list_modes():
            raise ValueError(f"Unsupported mode: {value}")
