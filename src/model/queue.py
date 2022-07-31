"""Data model for sink core modules
"""
import yaml
from enum import Enum
from typing import Union
from attrs import define, field, validators
from confluent_kafka import Producer, Consumer
from model.base import BaseModel


def convert_config(config: Union[dict, str]):
    if not isinstance(config, dict):
        if isinstance(config, str) and (
            config.endswith(".yaml") or config.endswith(".yml")
        ):
            return yaml.safe_load(open(config))

        else:
            raise ValueError(f"Unsupported config: {config} - {type(config)}")
    else:
        return config


@define
class Queue(BaseModel):
    """Queue class

    :param _type_ BaseModel: _description_
    :raises ValueError: _description_
    :raises KeyError: _description_
    :raises ValueError: _description_
    :raises NotImplementedError: _description_
    :return _type_: _description_
    """

    class Mode(Enum):
        """Constant class for Queue Mode"""

        PRODUCER = producer = Producer
        CONSUMER = consumer = Consumer
        # DEFAULT = default = "default"

    config: Union[dict, str] = field(kw_only=True, converter=convert_config)
    mode: Mode = field(
        kw_only=True,
        converter=lambda x: getattr(Queue.Mode, x.upper()) if isinstance(x, str) else x,
        validator=validators.in_(Mode),
    )

    @config.validator
    def check_config(self, _, value):
        """Validator"""
        if not isinstance(value, dict):
            raise ValueError(f"Unsupported config: {value} - {type(value)}")

        config_keys = set(["producer", "consumer", "default"])
        if set(value.keys()).difference(config_keys):
            raise KeyError(f"Config must have keys: {config_keys}")
