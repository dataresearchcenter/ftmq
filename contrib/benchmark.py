import os
import time
from contextlib import contextmanager
from shutil import rmtree

from ftmq.io import smart_read_proxies
from ftmq.query import Query
from ftmq.store import get_store

DATASET = "ec_meetings"


def get_proxies():
    yield from smart_read_proxies("./tests/fixtures/ec_meetings.ftm.json")


@contextmanager
def measure(*msg: str):
    start = time.time()
    try:
        yield None
    finally:
        end = time.time()
        print(*msg, round(end - start, 2))


def benchmark(uri: str):
    store = get_store(uri, dataset=DATASET)
    prefix = store.__class__.__name__
    print(prefix, uri)

    with measure(prefix, "write"):
        with store.writer() as bulk:
            for proxy in get_proxies():
                bulk.add_entity(proxy)

    with measure(prefix, "iterate"):
        _ = [p for p in store.iterate()]

    view = store.query()
    q = Query().where(dataset=DATASET, schema="Event", date__gte=2023)
    with measure(prefix, "query"):
        _ = [p for p in view.entities(q)]


if __name__ == "__main__":
    os.mkdir(".benchmark")
    benchmark("memory:///")
    benchmark("leveldb://.benchmark/leveldb")
    benchmark("sqlite:///.benchmark/sqlite")
    benchmark("postgresql:///ftm")
    benchmark("lake+./.benchmark/lake")
    rmtree(".benchmark", ignore_errors=True)
