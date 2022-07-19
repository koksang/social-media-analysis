"""Consumer service"""

import os
import sys
import ray
from ray.util.iter import ParallelIteratorWorker

from core.logger import logger as log
from core.crawler import Crawler
from core.queue import Queue
from model.crawler import Crawler as CrawlerModel


@ray.remote
class App(ParallelIteratorWorker):
    def __init__(self, crawler_conf: dict, queue_conf: dict, **kwargs) -> None:
        self.crawler = Crawler(**crawler_conf)
        self.producer_queue = Queue(**queue_conf, mode="PRODUCER")
        self.consumer_queue = Queue(**queue_conf, mode="CONSUMER")

    def get_src(self):
        if self.crawler.mode.name != CrawlerModel.Mode.SEARCH.name:
            src_name = f"Queued {self.crawler.mode.name} crawler"
            src = self.consumer_queue
        else:
            src_name = f"{self.crawler.mode.name} crawler"
            src = self.crawler

        log.info(f"Initiating src from {src_name}")
        return src

    def run(self):
        src = self.get_src()
        for item in src.run():
            pass
        pass
