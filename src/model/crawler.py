"""Data model for source core modules
"""
#
from enum import Enum
from attrs import define, field, validators
from snscrape.modules.twitter import (
    TwitterSearchScraper,
    TwitterTweetScraper,
    TwitterUserScraper,
)
from model.base import BaseModel


@define
class Crawler(BaseModel):
    """Crawler class

    :param _type_ Source: _description_
    :raises ValueError: _description_
    """

    class Mode(Enum):
        """Constant class for Crawler mode"""

        SEARCH = TwitterSearchScraper
        TWEET = TwitterTweetScraper
        USER = TwitterUserScraper

    mode: Mode = field(
        kw_only=True,
        converter=lambda x: getattr(Crawler.Mode, x.upper())
        if isinstance(x, str)
        else x,
        validator=validators.in_(Mode),
    )
    query: list[str] = field(kw_only=True, validator=validators.instance_of(list))
    max_limits: int = field(default=10, converter=int, validator=validators.ge(1))
    is_stopped: bool = field(default=False)
