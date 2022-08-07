"""Testing module"""

import ray
from services.consumer.main import App as Consumer

TOPICS = ["tweet", "user"]

CONFIG = "../conf/kafka.yaml"

PROJECT, DATASET = "area51-lab", "raw"
queue_conf = dict(config=CONFIG)

consumers = []
if not ray.is_initialized():
    ray.init()

queue_conf = dict(config=CONFIG)

consumers = []
for topic in TOPICS:
    bq_conf = dict(project=PROJECT, dataset=DATASET, table=topic, model=topic)
    consumers.append(
        Consumer.options(num_cpus=1, memory=500 * 1024 * 1024).remote(
            topic=[topic],
            bq_conf=bq_conf,
            queue_conf=queue_conf,
        )
    )

tasks = [consumer.run.remote() for consumer in consumers]
results = ray.get(tasks)

ray.shutdown()
