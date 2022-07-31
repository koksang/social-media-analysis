"""Testing module"""

from asyncio import tasks
import ray
from services.producer.main import App as Producer
from services.consumer.main import App as Consumer

MODE = "search"
QUERIES = (
    ['"data engineering"', "axwell", "bitcoin"],
    ['"elon musk"', "remote", "mlops"],
    ["ergo", "palantir", '"swedish house mafia"'],
    ["nft", "tesla", '"heat nation"'],
    ["ukraine", '"machine learning"', "dog"],
)
MAX_LIMITS = 200

CONFIG = "../conf/kafka.yaml"
TOPIC = "tweet"
PROJECT, DATASET, TABLE = "area51-lab", "raw", "test"

if not ray.is_initialized():
    ray.init()


queue_conf = dict(config=CONFIG)
bq_conf = dict(
    project=PROJECT,
    dataset=DATASET,
    table=TABLE,
)

producers = []
for query in QUERIES:
    crawler_conf = dict(
        mode=MODE,
        query=query,
        max_limits=MAX_LIMITS,
    )
    producers.append(
        Producer.options(num_cpus=2).remote(
            crawler_conf=crawler_conf,
            queue_conf=queue_conf,
        )
    )

tasks = [producer.run.remote() for producer in producers]
results = ray.get(tasks)
# print(output)

ray.shutdown()
