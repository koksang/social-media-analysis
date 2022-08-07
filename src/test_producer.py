"""Testing module"""

import ray
import pytz
from datetime import datetime as dt, timedelta as td
from services.producer.app import App as Producer

MODE = "search"

DATE = dt.now().astimezone(pytz.UTC).strftime("%Y-%m-%d")
NEXT_DATE = (dt.now() + td(days=1)).astimezone(pytz.UTC).strftime("%Y-%m-%d")

# search KOLs
KOLS = (
    [
        f'"elon musk" (#elonmusk OR #elon) until:{NEXT_DATE} since:{DATE}',
        f'"cz peng" (#czbinance OR #cz) until:{NEXT_DATE} since:{DATE}',
        f'"vitalik buterin" (#vitalik OR #vitalikbuterin) until:{NEXT_DATE} since:{DATE}',
    ],
    [
        f'"charles hoskinson" (#charleshoskinson) until:{NEXT_DATE} since:{DATE}',
        f'"michael saylor" (#michaelsaylor) until:{NEXT_DATE} since:{DATE}',
        f'"jack dorsey" (#jackdorsey) until:{NEXT_DATE} since:{DATE}',
    ],
    [
        f'"brian amstrong" (#brianamstrong) until:{NEXT_DATE} since:{DATE}',
        f'"sam bankman" OR "sbf" (#sbf OR #sambankman) until:{NEXT_DATE} since:{DATE}',
        f'"cathy wood" (#cathywood) until:{NEXT_DATE} since:{DATE}',
        f'"justin sun" (#justinsun) until:{NEXT_DATE} since:{DATE}',
    ],
)

# search tokens
TOKENS = (
    [
        f'"btc" OR "bitcoin" (#btc OR #bitcoin) until:{NEXT_DATE} since:{DATE}',
        f'"eth" OR "ethereum" (#eth OR #ethereum) until:{NEXT_DATE} since:{DATE}',
        f'"ada" OR "cardano" (#ada OR #cardano) until:{NEXT_DATE} since:{DATE}',
        f'"erg" OR "ergo" (#erg OR #ergo) until:{NEXT_DATE} since:{DATE}',
    ],
    [
        f'"bnb" OR "binance coin" OR "bsc" OR "binance smart chain" (#bnb OR #bsc OR #binancecoin OR #binancesmartchain) until:{NEXT_DATE} since:{DATE}',
        f'"sol" OR "solana" (#sol OR #solana) until:{NEXT_DATE} since:{DATE}',
        f'"matic" OR "polygon" (#matic OR #polygon) until:{NEXT_DATE} since:{DATE}',
        f'"avax" OR "avalanche" (#avax OR #avalanche) until:{NEXT_DATE} since:{DATE}',
    ],
    [
        f'"doge" OR "dogecoin" (#doge OR #dogecoin) until:{NEXT_DATE} since:{DATE}',
        f'"shib" OR "shibatoken" (#shib OR #shibatoken) until:{NEXT_DATE} since:{DATE}',
        f'"chainlink" (#link OR #chainlink) until:{NEXT_DATE} since:{DATE}',
        f'"xrp" OR "ripple" (#xrp OR #ripple) until:{NEXT_DATE} since:{DATE}',
    ],
)

# search topics
TOPICS = (
    [
        f'"crypto" OR "cryptocurrency" (#crypto OR #cryptocurrency) until:{NEXT_DATE} since:{DATE}'
        f'"blockchain" (#blockchain) until:{NEXT_DATE} since:{DATE}',
        f'"web3" (#web3) until:{NEXT_DATE} since:{DATE}',
    ],
    [
        f'"nft" OR "nfts" OR "metaverse" (#nft OR #nfts OR #metaverse) until:{NEXT_DATE} since:{DATE}',
        f'"defi" OR "gamefi" (#defi OR #gamefi) until:{NEXT_DATE} since:{DATE}',
        f'"dao" (#dao) until:{NEXT_DATE} since:{DATE}',
    ],
    [
        f'"coinbase" OR "$COIN" (#coin OR #coinbase) until:{NEXT_DATE} since:{DATE}',
        f'"binance" OR "binance.us" (#binance) until:{NEXT_DATE} since:{DATE}',
        f'"ftx" OR "ftx.us" (#ftx) until:{NEXT_DATE} since:{DATE}',
    ],
    [
        f'"ark" OR "ark invest" (#ark OR #arkinvest) until:{NEXT_DATE} since:{DATE}',
        f'"nansen ai" (#nansenai) until:{NEXT_DATE} since:{DATE}',
    ],
)


ENTITIES = [KOLS, TOKENS, TOPICS]
MAX_LIMITS = 1000

CONFIG = "../conf/kafka.yaml"
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
for entity_list in ENTITIES:
    for entity in entity_list:
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
