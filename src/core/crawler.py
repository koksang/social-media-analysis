"""Crawler module"""

import os
from typing import Iterable, Type
import ray
from snscrape.modules.twitter import (
    TwitterSearchScraper,
    TwitterTweetScraper,
    TwitterUserScraper,
)
from model.core import Crawler

SCRAPER_IDS: dict[str, Type] = {
    Crawler.Mode.SEARCH: TwitterSearchScraper,
    Crawler.Mode.TWEET: TwitterTweetScraper,
    Crawler.Mode.USER: TwitterUserScraper,
}


class TwitterCrawler(Crawler):
    """Twitter Crawler class

    :param _type_ Crawler: _description_
    """

    def _get_scraper(
        self,
        scraper_ids: dict[str, Type] = SCRAPER_IDS,
    ) -> Type:
        """Get specific type of scraper

        :return object: _description_
        """
        scraper_id = self.scraper_id
        assert scraper_id in scraper_ids, f"Unsupported scraper type: {scraper_id}"
        return scraper_ids[scraper_id]

    def _get_scrapers(self) -> Iterable[object]:
        """Get all scrapers based on queries

        :return Iterable[object]: list of scrapers
        """
        yield [self._get_scraper()(query) for query in self.query]

    def run(self) -> list[object]:
        """Run app

        :return list[object]: list of tweet object
        """

        @ray.remote
        def scrape(scraper: Type, max_results: int) -> list[object]:
            """Scrape tweet

            :param object scraper: Scraper object
            :param int max_results: Max results to scrape per scraper
            :return list[object]: list of tweet object
            """
            items = []
            for i, item in enumerate(scraper.get_items()):
                if i > max_results:
                    break
                items.append(item)
            return items

        if not ray.is_initialized():
            ray.init(
                num_cpus=os.cpu_count(),
                ignore_reinit_error=True,
            )

        tasks = [
            scrape.remote(scraper, self.max_results) for scraper in self._get_scrapers()
        ]
        return ray.get(tasks)
