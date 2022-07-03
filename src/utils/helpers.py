"""Helper functions"""
import yaml
from typing import Union


def clean_str(text: str) -> str:
    return text.strip().upper()


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
