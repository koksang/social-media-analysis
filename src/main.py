"""Main module"""

import ray
import hydra
from dateutil.parser import parse
from omegaconf import DictConfig, OmegaConf
from services.producer.app import App as Producer
from services.consumer.app import App as Consumer
from core.logger import logger as log
from utils.helpers import init_start_end_date, build_search_entities


@hydra.main(version_base=None, config_path="../conf", config_name="main")
def main(conf: DictConfig) -> None:
    log.debug(OmegaConf.to_yaml(conf))

    run_mode = conf.run_mode.lower()
    producer_conf = OmegaConf.to_object(conf.producer)
    start_date, end_date = init_start_end_date()

    if not ray.is_initialized():
        ray.init()

    actors = []
    if run_mode == "producer":
        if "start_date" in conf.producer.keys() and conf.producer.start_date:
            start_date = parse(conf.producer.start_date).date()
            producer_conf.pop("start_date", None)
        if "end_date" in conf.producer.keys() and conf.producer.end_date:
            end_date = parse(conf.producer.end_date).date()
            producer_conf.pop("end_date", None)

        log.info(f"Running with start_date: {start_date}, end_date: {end_date}")
        search_entities = build_search_entities(conf.entity, start_date, end_date)

        exit()

        for entity in search_entities:
            crawler_conf = producer_conf.copy()
            crawler_conf.update({"entity": entity})
            actors.append(
                Producer.options(memory=150 * 1024 * 1024).remote(
                    crawler_conf=crawler_conf,
                    queue_conf=dict(config=OmegaConf.to_object(conf.kafka)),
                )
            )
    elif run_mode == "consumer":
        for topic in conf.consumer.topics:
            bq_conf = OmegaConf.to_object(conf.bigquery).copy()
            bq_conf.update({"table": topic, "model": topic})
            actors.append(
                Consumer.options(memory=250 * 1024 * 1024).remote(
                    topic=[topic],
                    bq_conf=bq_conf,
                    queue_conf=dict(config=OmegaConf.to_object(conf.kafka)),
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
