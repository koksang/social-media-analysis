"""Main module"""

import ray
import hydra
from omegaconf import DictConfig, OmegaConf
from services.producer.app import App as Producer
from services.consumer.app import App as Consumer
from core.logger import logger as log
from utils.helpers import append_start_end_date


@hydra.main(version_base=None, config_path="../conf", config_name="main")
def main(conf: DictConfig) -> None:
    log.debug(OmegaConf.to_yaml(conf))

    run_mode = conf.run_mode.lower()
    producer_conf = OmegaConf.to_object(conf.producer)
    consumer_conf = OmegaConf.to_object(conf.consumer)
    entities = conf.entity
    kafka_conf = OmegaConf.to_object(conf.kafka)
    bigquery_conf = OmegaConf.to_object(conf.bigquery)

    start_date = None
    if "start_date" in producer_conf:
        start_date = producer_conf["start_date"]

    if not ray.is_initialized():
        ray.init()

    actors = []
    if run_mode == "producer":
        for entity in entities:
            crawler_conf = producer_conf.copy()
            crawler_conf["entity"] = append_start_end_date(entity, start_date)
            actors.append(
                Producer.options(memory=150 * 1024 * 1024).remote(
                    crawler_conf=crawler_conf,
                    queue_conf=dict(config=kafka_conf),
                )
            )
    elif run_mode == "consumer": 
        for topic in consumer_conf["topics"]:
            bq_conf = bigquery_conf.copy()
            bq_conf["table"], bq_conf["model"] = topic, topic
            actors.append(
                Consumer.options(num_cpus=2, memory=250 * 1024 * 1024).remote(
                    topic=[topic],
                    bq_conf=bq_conf,
                    queue_conf=dict(config=kafka_conf),
                )
            )
    else:
        raise ValueError(f"Unsupported run_mode: {run_mode}")

    log.info(f"Creating {len(actors)} actors")
    result_ids = [actor.run.remote() for actor in actors]
    while len(result_ids):
        done_ids, result_ids = ray.wait(result_ids)

    # results = ray.get(result_ids)
    ray.shutdown()


if __name__ == "__main__":
    main()
