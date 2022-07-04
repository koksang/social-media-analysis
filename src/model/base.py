"""Data model for all core modules
"""
from abc import ABC, abstractmethod
from typing import Iterable
from attrs import define

#
from core.logger import logger as log

# NOTE: common task model
@define
class Task(ABC):
    """Task baseclass

    :param _type_ ABC: _description_
    """

    def __attrs_post_init__(self):
        conf = {k: getattr(self, k) for k in self.__slots__}
        log.info(f"Init config: {conf}")

    @classmethod
    def list_modes(cls, mode_classname: str = "Mode") -> Iterable[str]:
        """Get all modes

        :return list[str]: list of available modes
        """
        mode = getattr(cls, mode_classname)
        keys = filter(lambda x: not x.startswith("__"), dir(mode))
        return (getattr(mode, key) for key in keys)

    @abstractmethod
    def run(self):
        """Standard run function"""
