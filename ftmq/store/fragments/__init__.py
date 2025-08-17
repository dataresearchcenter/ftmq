from anystore.functools import weakref_cache as cache

from ftmq.store.fragments.dataset import Fragments
from ftmq.store.fragments.settings import Settings
from ftmq.store.fragments.store import Store
from ftmq.store.fragments.utils import NULL_ORIGIN


@cache
def get_store(database_uri: str | None = None, **config) -> Store:
    settings = Settings()
    return Store(database_uri=database_uri or settings.database_uri, **config)


@cache
def get_fragments(
    name: str,
    origin: str | None = NULL_ORIGIN,
    database_uri: str | None = None,
    **config,
) -> Fragments:
    settings = Settings()
    uri = database_uri or settings.database_uri
    store = get_store(uri, **config)
    return store.get(name, origin=origin or NULL_ORIGIN)
