"""Queue module"""

from typing import Iterable, Generator
from confluent_kafka import KafkaError
from core.logger import logger as log
from model.base import BaseModel
from model.queue import Queue as QueueModel


class Queue(BaseModel):
    """Kafka Queue

    :param _type_ Queue: _description_
    """

    def __init__(self, **kwargs) -> None:
        model = QueueModel(**kwargs)
        self.config = self.get_config(model.config)
        self.mode = model.mode
        self.topic = model.topic
        self.runner = self.mode.value(self.config)

    def get_config(self, config: dict) -> dict[str, str]:
        runner_config = config["default"].copy()
        runner_config.update(config[self.mode.name.lower()])
        return runner_config

    def produce(self, messages: Iterable[dict]) -> None:
        """Producer producing messages

        :param Iterable[dict] messages: _description_
        """

        def callback(error, message):
            if error:
                nonlocal failed_count
                failed_count += 1
                log.error(f"Failed delivery: {error}")
            else:
                topic, key, value = (
                    message.topic(),
                    message.key().decode("utf-8"),
                    message.value().decode("utf-8"),
                )
                log.debug(
                    f"Produced event - topic: {topic}, key: {key}, value: {value}"
                )

        failed_count = 0
        msg_count = 0
        for message in messages:
            msg_count += 1
            topic, key, value = message["topic"], message["key"], message["value"]
            self.runner.produce(topic, key, value, callback=callback)
            self.runner.poll(0)

        self.runner.flush()
        log.info(f"Produced total events: {msg_count}, failed: {failed_count}")

    def consume(self):
        """Consumer consuming messages

        :param Iterable[dict] messages: _description_
        :raises ValueError: _description_
        """
        if not self.topic:
            raise ValueError(f"Topic is empty")

        self.runner.subscribe(self.topic)
        msg_count, failed_count = 0, 0
        while self.running:
            msg = self.runner.poll(1)
            if not msg:
                continue

            msg_count += 1
            if msg.error():
                # NOTE: end of partition event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    self.running = False
                    topic, part, offset = msg.topic(), msg.partition(), msg.offset()
                    log.info(f"{topic} [{part}] reached end at offset {offset}")
                # general error
                elif msg.error():
                    failed_count += 1
                    log.error(msg.error())
            else:
                self.runner.commit(asynchronous=False)
                yield msg

    def run(self, **kwargs) -> Generator:
        """Run app

        :return list[object]: list of tweet object
        """
        self.running = True

        try:
            if self.mode.name == QueueModel.Mode.PRODUCER.name:
                self.produce(**kwargs)
            elif self.mode.name == QueueModel.Mode.CONSUMER.name:
                for item in self.consume():
                    yield item
        finally:
            self.running = False
            self.runner.close()
