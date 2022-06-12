"""Crawler module
"""

import os
import sys
import ray

from pathlib import Path
from tqdm import tqdm

#
from base.model import Crawler
from .constants import MAX_TWEETS


class App(Crawler):
    def _get_scrapers(self) -> list[object]:
        """Get all scrapers

        :return list[object]: list of scrapers
        """
        scraper = self._get_scraper()
        return [scraper(item) for item in tqdm(self.query)]

    def run(self) -> list[object]:
        """Run app

        :return list[object]: list of tweet object
        """

        @ray.remote
        def scrape(scraper: object, max_results: int) -> list[object]:
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
                num_cpus=os.cpu_count() - 2,
                ignore_reinit_error=True,
            )

        tasks = [
            scrape.remote(scraper, self.max_results) for scraper in self._get_scrapers()
        ]
        return ray.get(tasks)


if __name__ == "__main__":
    app = App(max_results=MAX_TWEETS)
    output = app.run()
    
