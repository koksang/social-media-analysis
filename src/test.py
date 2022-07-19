"""Testing module"""
import ray
from ray.util.iter import from_actors
from core.crawler import Crawler

MODE = "search"
QUERY = ["gg", "axwell", "house music"]

if not ray.is_initialized():
    ray.init()

crawler = Crawler.remote(mode=MODE, query=QUERY)
print(type(crawler), crawler, crawler)

it = from_actors([crawler, crawler])
it = it.gather_async()  # .for_each(lambda x: x)  # .batch_across_shards()

for i in it:
    print(i.__dict__)
    print(type(i))
    print("\n\n")


# task = crawler.run.remote()
# tasks = [task for _ in range(3)]
# output = ray.get(tasks)
# print(output)

# ray.shutdown()
