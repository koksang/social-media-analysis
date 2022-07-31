"""Producer service"""

import ray
import json
from datetime import datetime, timezone
from attrs import asdict

from core.crawler import Crawler
from core.queue import Queue
from model.twitter import Tweet, User
from core.logger import logger as log
from utils.helpers import timestamp_to_integer

SEND_LIMIT = 200


@ray.remote
class App:
    def __init__(self, crawler_conf: dict, queue_conf: dict, **kwargs) -> None:
        self.crawler = Crawler(**crawler_conf)
        self.queue = Queue(**queue_conf, mode="producer")

    def run(self):
        """Run producer app"""
        messages = []
        for item in self.crawler.run():
            try:
                tweet = Tweet(
                    id=str(item.id),
                    url=item.url,
                    content=item.renderedContent,
                    created_timestamp=item.date,
                    user_id=str(item.user.id),
                    retweets_count=item.retweetCount,
                    quote_tweets_count=item.quoteCount,
                    likes_count=item.likeCount,
                    hashtags=item.hashtags,
                    replies=item.inReplyToTweetId,
                    source_label=item.sourceLabel,
                    data_ts=timestamp_to_integer(
                        datetime.now().astimezone(timezone.utc)
                    ),
                )
                user = User(
                    id=str(item.user.id),
                    username=item.user.username,
                    display_name=item.user.displayname,
                    description=item.user.description,
                    created_timestamp=item.user.created,
                    verified=item.user.verified,
                    location=item.user.location,
                    followers_count=item.user.followersCount,
                    friends_count=item.user.friendsCount,
                    statuses_count=item.user.statusesCount,
                    favourites_count=item.user.favouritesCount,
                    label=item.user.label.longDescription if item.user.label else None,
                    data_ts=timestamp_to_integer(
                        datetime.now().astimezone(timezone.utc)
                    ),
                )
                message_tweet = {
                    "topic": "tweet",
                    "key": "tweet",
                    "value": json.dumps(asdict(tweet)),
                }
                message_user = {
                    "topic": "user",
                    "key": "user",
                    "value": json.dumps(asdict(user)),
                }
                messages.extend([message_tweet, message_user])

                if len(messages) % SEND_LIMIT == 0:
                    self.queue.run(messages=messages)
                    messages = []
            except Exception as msg:
                log.error(f"Failed to ingest: {item}, error: {msg}")
                continue

        if messages:
            self.queue.run(messages=messages)
            messages = []

        log.info(f"{self} completed!")
