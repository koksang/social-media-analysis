"""Model for http response 
"""
from typing import Union
from datetime import datetime
from attrs import define, field
from attrs.validators import instance_of
from utils.helpers import timestamp_to_integer, enlist


@define
class User:
    id: str = field(kw_only=True, validator=instance_of(str))
    username: str = field(kw_only=True, validator=instance_of(str))
    display_name: str = field(kw_only=True, validator=instance_of(str))
    description: str = field(kw_only=True, validator=instance_of(str))
    created_timestamp: Union[datetime, int, str] = field(
        kw_only=True,
        converter=lambda x: timestamp_to_integer(x) if isinstance(x, datetime) else x,
        validator=instance_of(int),
    )
    verified: bool = field(kw_only=True, validator=instance_of(bool))
    location: str = field(kw_only=True, validator=instance_of(str))
    followers_count: int = field(kw_only=True, validator=instance_of(int))
    friends_count: int = field(kw_only=True, validator=instance_of(int))
    statuses_count: int = field(kw_only=True, validator=instance_of(int))
    favourites_count: int = field(kw_only=True, validator=instance_of(int))
    label: Union[str, None] = field(
        kw_only=True, validator=instance_of((str, type(None)))
    )
    data_ts: int = field(kw_only=True, validator=instance_of(int))


@define
class Tweet:
    id: str = field(kw_only=True, validator=instance_of(str))
    url: str = field(kw_only=True, validator=instance_of(str))
    content: str = field(kw_only=True, validator=instance_of(str))
    created_timestamp: Union[datetime, int, str] = field(
        kw_only=True,
        converter=lambda x: timestamp_to_integer(x) if isinstance(x, datetime) else x,
        validator=instance_of(int),
    )
    user_id: str = field(kw_only=True, validator=instance_of(str))
    retweets_count: int = field(kw_only=True, validator=instance_of(int))
    quote_tweets_count: int = field(kw_only=True, validator=instance_of(int))
    likes_count: int = field(kw_only=True, validator=instance_of(int))
    source_label: str = field(kw_only=True, validator=instance_of(str))
    hashtags: Union[list, None] = field(
        kw_only=True,
        default=None,
        converter=enlist,
        validator=instance_of(list),
    )
    replies: Union[list, None] = field(
        kw_only=True,
        default=None,
        converter=enlist,
        validator=instance_of(list),
    )
    entity: str = field(kw_only=True, validator=instance_of(str))
    data_ts: int = field(kw_only=True, validator=instance_of(int))
