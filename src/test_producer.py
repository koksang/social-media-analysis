"""Testing module"""

from asyncio import tasks
import ray
from services.producer.main import App as Producer
from services.consumer.main import App as Consumer

MODE = "search"
ENTITIES = (
    ["cryptocurrency", "bitcoin", "coinbase"],
    ["web3", "defi", "nft", '"nansen ai"'],
    ["eth", "ethereum", '"vitalik buterin"'],
    ['"charles hoskinson"', "ergo", "cardano", "ada", "erg"],
    ['"elon musk"', "solana", "sol", "matic"],
    ["bnb", "binance", '"binance smart chain"', '"binance coin"', '"cz peng"'],
)
MAX_LIMITS = 2000

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
for entity in ENTITIES:
    crawler_conf = dict(
        mode=MODE,
        entity=entity,
        max_limits=MAX_LIMITS,
    )
    producers.append(
        Producer.options(num_cpus=1, memory=250 * 1024 * 1024).remote(
            crawler_conf=crawler_conf,
            queue_conf=queue_conf,
        )
    )

tasks = [producer.run.remote() for producer in producers]
results = ray.get(tasks)

ray.shutdown()
