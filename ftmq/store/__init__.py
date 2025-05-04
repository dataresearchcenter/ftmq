from functools import cache
from pathlib import Path
from typing import TypeVar
from urllib.parse import urlparse

from nomenklatura import Resolver, settings
from nomenklatura.db import get_metadata

from ftmq.model.dataset import Catalog, Dataset
from ftmq.store.aleph import AlephStore
from ftmq.store.base import Store, View
from ftmq.store.memory import MemoryStore
from ftmq.store.sql import SQLStore
from ftmq.types import PathLike

S = TypeVar("S", bound=Store)


@cache
def get_store(
    uri: PathLike | None = settings.DB_URL,
    catalog: Catalog | None = None,
    dataset: Dataset | str | None = None,
    linker: Resolver | None = None,
) -> Store:
    """
    Get an initialized [Store][ftmq.store.base.Store]. The backend is inferred
    by the scheme of the store uri.

    Example:
        ```python
        from ftmq.store import get_store

        # an in-memory store:
        get_store("memory://")

        # a leveldb store:
        get_store("leveldb:///var/lib/data")

        # a redis (or kvrocks) store:
        get_store("redis://localhost")

        # a sqlite store
        get_store("sqlite:///data/followthemoney.db")
        ```

    Args:
        uri: The store backend uri
        catalog: A `ftmq.model.Catalog` instance to limit the scope to
        dataset: A `ftmq.model.Dataset` instance to limit the scope to
        linker: A `nomenklatura.Resolver` instance with linked / deduped data

    Returns:
        The initialized store. This is a cached object.
    """
    if isinstance(dataset, str):
        dataset = Dataset(name=dataset)
    uri = str(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "memory":
        return MemoryStore(catalog, dataset, linker=linker)
    if parsed.scheme == "leveldb":
        path = uri.replace("leveldb://", "")
        path = Path(path).absolute()
        try:
            from ftmq.store.level import LevelDBStore

            return LevelDBStore(catalog, dataset, path=path, linker=linker)
        except ImportError:
            raise ImportError("Can not load LevelDBStore. Install `plyvel`")
    if parsed.scheme == "redis":
        try:
            from ftmq.store.redis import RedisStore

            return RedisStore(catalog, dataset, linker=linker)
        except ImportError:
            raise ImportError("Can not load RedisStore. Install `redis`")
    if parsed.scheme == "clickhouse":
        try:
            from ftm_columnstore import get_store as get_cstore

            return get_cstore(catalog, dataset, linker=linker)
        except ImportError:
            raise ImportError("Can not load ClickhouseStore. Install `ftm-columnstore`")
    if "sql" in parsed.scheme:
        get_metadata.cache_clear()
        return SQLStore(catalog, dataset, uri=uri, linker=linker)
    if "aleph" in parsed.scheme:
        return AlephStore.from_uri(uri, catalog=catalog, dataset=dataset, linker=linker)
    raise NotImplementedError(uri)


__all__ = ["get_store", "S", "Store", "View", "MemoryStore", "SQLStore", "AlephStore"]
