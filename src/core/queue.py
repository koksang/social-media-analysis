"""Queue module"""

from typing import Iterable, Type
from tqdm import tqdm
from confluent_kafka import Producer, Consumer
from core.logger import logger as log
from model.core import Queue


class KafkaQueue(Queue):
    """Kafka Queue

    :param _type_ Queue: _description_
    """

    def _get_runner(self) -> Type:
        """Get queue runner

        :return _type_: _description_
        """
        config = self.config[self.Mode.DEFAULT]
        if self.mode in self.config:
            config.update(self.config[self.mode])

        runners = {
            self.Mode.PRODUCER: Producer,
            self.Mode.CONSUMER: Consumer,
        }
        return runners[self.mode](config)

    def produce(self, runner: Type[Producer], messages: Iterable[dict]) -> None:
        def callback(error, message):
            if error:
                log.error(f"Failed delivery: {error}")
            else:
                topic, key, value = (
                    message.topic(),
                    message.key().decode("utf-8"),
                    message.value().decode("utf-8"),
                )
                log.info(f"Produced event - topic: {topic}, key: {key}, value: {value}")

        for message in tqdm(messages):
            topic, key, value = message["topic"], message["key"], message["value"]
            runner.produce(topic, key, value, callback=callback)

        runner.flush()

    def consume(self, runner: Type[Producer], messages: Iterable[dict]) -> None:
        pass

    def run(self) -> None:
        """Run app

        :return list[object]: list of tweet object
        """
        runner = self._get_runner()
        if self.mode == self.Mode.PRODUCER:
            pass
