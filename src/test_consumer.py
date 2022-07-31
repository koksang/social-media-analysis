"""Testing module"""

import ray
from services.consumer.main import App as Consumer

MODE = "search"
TOPICS = ["tweet", "user"]
MAX_LIMITS = 50

CONFIG = "../conf/kafka.yaml"
PROJECT, DATASET, TABLE = "area51-lab", "raw", "test"
queue_conf = dict(config=CONFIG)

consumers = []
if not ray.is_initialized():
    ray.init()

queue_conf = dict(config=CONFIG)

consumers = []
for topic in TOPICS:
    bq_conf = dict(project=PROJECT, dataset=DATASET, table=TABLE, model=topic)
    consumers.append(
        Consumer.options(num_cpus=2).remote(
            topic=[topic],
            bq_conf=bq_conf,
            queue_conf=queue_conf,
        )
    )

tasks = [consumer.run.remote() for consumer in consumers]
results = ray.get(tasks)

print(results)

ray.shutdown()
