"""Queue module"""


from tqdm import tqdm

#
from core.logger import LOGGER as log
from model.core import Queue


class KafkaQueue(Queue):
    def _produce(self) -> None:
        def callback(error, message):
            if error:
                log.error(f"Failure delivery: {error}")
            else:
                topic, key, value = (
                    message.topic(),
                    message.key().decode("utf-8"),
                    message.value().decode("utf-8"),
                )
                log.info(
                    f"Produced event to topic: {topic}, key: {key}, value: {value}"
                )

        for message in tqdm(self.messages):
            topic, key, value = message["topic"], message["key"], message["value"]
            self.runner.produce(topic, key, value, callback=callback)

        self.runner.flush()

    def run(self) -> None:
        """Run app

        :return list[object]: list of tweet object
        """
        runner = self._get_runner()
        if self.run_mode == self.mode.PRODUCE:
            self._produce()
