from functools import cache
from pathlib import Path
from urllib.parse import urlparse

from anystore.types import Uri
from followthemoney.dataset.dataset import Dataset
from nomenklatura import Resolver, settings

from ftmq.store.base import Store, View
from ftmq.store.memory import MemoryStore


@cache
def get_store(
    uri: Uri | None = settings.DB_URL,
    dataset: Dataset | str | None = None,
    linker: Resolver | None = None,
    cast_types: bool = True,
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

        # a duckdb store
        get_store("duckdb:///data/followthemoney.duckdb")
        ```

    Args:
        uri: The store backend uri
        dataset: A `followthemoney.Dataset` instance to limit the scope to
        linker: A `nomenklatura.Resolver` instance with linked / deduped data
        cast_types: Normalize statement values on write (see
            [`ftmq.statements`][ftmq.statements])

    Returns:
        The initialized store. This is a cached object.
    """
    uri = str(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "memory":
        return MemoryStore(dataset, linker=linker, cast_types=cast_types)
    if parsed.scheme == "leveldb":
        path = uri.replace("leveldb://", "")
        path = Path(path).absolute()
        try:
            from ftmq.store.level import LevelDBStore

            return LevelDBStore(
                dataset, path=path, linker=linker, cast_types=cast_types
            )
        except ImportError:
            raise ImportError("Can not load LevelDBStore. Install `plyvel`")
    if parsed.scheme == "redis":
        try:
            from ftmq.store.redis import RedisStore

            return RedisStore(dataset, linker=linker, cast_types=cast_types)
        except ImportError:
            raise ImportError("Can not load RedisStore. Install `redis`")
    if parsed.scheme == "duckdb":
        try:
            from ftmq.store.duckdb import DuckDBStore

            return DuckDBStore(dataset, uri=uri, linker=linker, cast_types=cast_types)
        except ImportError:
            raise ImportError("Can not load DuckDBStore. Install `duckdb-engine`")
    if "sql" in parsed.scheme:
        try:
            from ftmq.store.sql import SQLStore

            return SQLStore(dataset, uri=uri, linker=linker, cast_types=cast_types)
        except ImportError:
            raise ImportError("Can not load SqlStore. Install sql dependencies.")
    if "aleph" in parsed.scheme:
        try:
            from ftmq.store.aleph import AlephStore

            # no `cast_types`: the aleph writer posts entity proxies to the
            # remote api, no statements pass through it
            return AlephStore.from_uri(uri, dataset=dataset, linker=linker)
        except ImportError:
            raise ImportError("Can not load AlephStore. Install `alephclient`")
    if uri.startswith("lake+"):
        try:
            from ftmq.store.lake import LakeStore

            uri = str(uri)[5:]
            return LakeStore(
                uri=uri, dataset=dataset, linker=linker, cast_types=cast_types
            )
        except ImportError:
            raise ImportError("Can not load LakeStore. Install `[lake]` dependencies")
    if uri.startswith("fragments+"):
        uri = str(uri)[10:]
        raise NotImplementedError(uri)
    raise NotImplementedError(uri)


__all__ = [
    "get_store",
    "Store",
    "View",
    "MemoryStore",
]
