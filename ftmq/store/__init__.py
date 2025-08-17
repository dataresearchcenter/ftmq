from pathlib import Path
from urllib.parse import urlparse

from anystore.functools import weakref_cache as cache
from anystore.types import Uri
from followthemoney.dataset.dataset import Dataset
from nomenklatura import Resolver, settings
from nomenklatura.db import get_metadata

from ftmq.store.base import Store, View
from ftmq.store.memory import MemoryStore


@cache
def get_store(
    uri: Uri | None = settings.DB_URL,
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
        dataset: A `followthemoney.Dataset` instance to limit the scope to
        linker: A `nomenklatura.Resolver` instance with linked / deduped data

    Returns:
        The initialized store. This is a cached object.
    """
    uri = str(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "memory":
        return MemoryStore(dataset, linker=linker)
    if parsed.scheme == "leveldb":
        path = uri.replace("leveldb://", "")
        path = Path(path).absolute()
        try:
            from ftmq.store.level import LevelDBStore

            return LevelDBStore(dataset, path=path, linker=linker)
        except ImportError:
            raise ImportError("Can not load LevelDBStore. Install `plyvel`")
    if parsed.scheme == "redis":
        try:
            from ftmq.store.redis import RedisStore

            return RedisStore(dataset, linker=linker)
        except ImportError:
            raise ImportError("Can not load RedisStore. Install `redis`")
    if "sql" in parsed.scheme:
        try:
            from ftmq.store.sql import SQLStore

            get_metadata.cache_clear()
            return SQLStore(dataset, uri=uri, linker=linker)
        except ImportError:
            raise ImportError("Can not load SqlStore. Install sql dependencies.")
    if "aleph" in parsed.scheme:
        try:
            from ftmq.store.aleph import AlephStore

            return AlephStore.from_uri(uri, dataset=dataset, linker=linker)
        except ImportError:
            raise ImportError("Can not load AlephStore. Install `alephclient`")
    if uri.startswith("lake+"):
        try:
            from ftmq.store.lake import LakeStore

            uri = str(uri)[5:]
            return LakeStore(uri=uri, dataset=dataset, linker=linker)
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
