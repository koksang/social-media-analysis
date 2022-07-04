"""Producer service"""

import os
import sys

from core.crawler import TwitterCrawler
from core.queue import KafkaQueue
from core.logger import logger as log


class App:
    def __init__(self, **kwargs) -> None:
        pass

    def run(self):
        pass