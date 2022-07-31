"""Queue module"""

from typing import Iterable, Generator, Union
from confluent_kafka import KafkaError
from core.logger import logger as log
from model.base import BaseTask
from model.queue import Queue as QueueModel


class Queue(BaseTask):
    """Kafka Queue

    :param _type_ Queue: _description_
    """

    def __init__(self, **kwargs) -> None:
        model = QueueModel(**kwargs)
        self.mode = model.mode
        self.config = self.get_config(model.config)
        self.runner = self.mode.value(self.config)

    def get_config(self, config: dict) -> dict[str, str]:
        """get config

        :param dict config: queue config
        :return dict[str, str]: _description_
        """
        runner_config = config["default"].copy()
        if self.mode.name.lower() in config:
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

    def consume(self, topic: list[str]):
        """Consumer consuming messages

        :param Iterable[dict] messages: _description_
        :raises ValueError: _description_
        """

        msg_count, failed_count, empty_count = 0, 0, 0
        try:
            self.runner.subscribe(topic)
            while self.running:
                msg = self.runner.poll(1)
                # if empty_count % 10 == 0:
                #     log.info(f"No message found in queue")
                #     break

                if not msg:
                    empty_count += 1
                    continue

                # NOTE: reset empty message count
                # empty_count = 0
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
        finally:
            self.running = False
            self.runner.close()

    def run(self, **kwargs):
        """Run app

        :return list[object]: list of tweet object
        """
        self.running = True
        if self.mode.name == QueueModel.Mode.PRODUCER.name:
            self.produce(messages=kwargs["messages"])
        elif self.mode.name == QueueModel.Mode.CONSUMER.name:
            return self.consume(topic=kwargs["topic"])
