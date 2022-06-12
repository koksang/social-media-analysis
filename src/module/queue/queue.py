"""Crawler module
"""

import os
import sys
import ray

from pathlib import Path
from tqdm import tqdm
from confluent_kafka

#
from base.model import Queue


class App(Queue):
    def run(self) -> list[object]:
        """Run app

        :return list[object]: list of tweet object
        """
        if self.run_mode == self.mode.PRODUCE:
            pass

        return


if __name__ == "__main__":
    app = App(max_results=10)
    output = app.run()
