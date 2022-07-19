"""Producer service"""

import os
import sys
import click

sys.path.insert(0, "/Users/klim/Projects/twitter-trends-crawling/src")

from core.crawler import TwitterCrawler
from core.sink.queue import KafkaQueue
from core.logger import logger as log
from services.producer.app import App


@click.command()
@click.argument("mode", type=click.Choice(TwitterCrawler.get_modes(), case_sensitive=False))  # type: ignore
@click.option(
    "--max_limits",
    "-m",
    type=int,
    help="Maximum limits to crawl",
    default=10,
)
def main(**kwargs) -> None:
    app = App(**kwargs)
    app.run()


if __name__ == "__main__":
    main()
