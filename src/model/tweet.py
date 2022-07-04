"""Model for http response 
"""
from typing import Union
from datetime import datetime
from attrs import define


@define
class User:
    id: str
    username: str
    display_name: str
    description: str
    created_timestamp: datetime
    verified: bool
    location: str
    followers_count: int
    friends_count: int
    statuses_count: int
    favourites_count: int
    latest_ten_tweets: Union[list, None] = None


@define
class Tweet:
    id: str
    url: str
    content: str
    created_timestamp: datetime
    user: User
    retweets_count: int
    quote_tweets_count: int
    likes_count: int
    hashtags: Union[list, None] = None
    replies: Union[list, None] = None
