"""Data model for all core modules
"""
from abc import ABC, abstractmethod
from attrs import define
from core.logger import logger as log

# NOTE: common task model
@define
class BaseModel(ABC):
    """Base class for model

    :param _type_ ABC: _description_
    """

    def __attrs_post_init__(self):
        conf = {k: getattr(self, k) for k in self.__slots__}
        log.info(f"Init config: {conf}")


class BaseTask(ABC):
    """Base class for task

    :param _type_ ABC: _description_
    """

    @abstractmethod
    def run(self, **kwargs):
        pass
