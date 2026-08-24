from functools import cache
from typing import Any
from urllib.parse import urlparse

from anystore.logging import get_logger
from anystore.util import ensure_uri

from ftmq.search.settings import Settings
from ftmq.search.store.base import BaseStore
from ftmq.search.store.sqlite import SQliteStore

log = get_logger(__name__)


@cache
def get_store(**kwargs: Any) -> BaseStore:
    settings = Settings()
    uri = kwargs.pop("uri", None)
    if uri is None:
        if settings.yaml_uri is not None:
            store = BaseStore.from_yaml_uri(settings.yaml_uri, **kwargs)
            return get_store(**store.model_dump())
        if settings.json_uri is not None:
            store = BaseStore.from_json_uri(settings.json_uri, **kwargs)
            return get_store(**store.model_dump())
        uri = settings.uri
    uri = ensure_uri(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "sqlite":
        return SQliteStore(uri=uri, **kwargs)
    if parsed.scheme in ("tantivy", "memory"):
        try:
            from ftmq.search.store.tantivy import TantivyStore

            return TantivyStore(uri=uri, memory=parsed.scheme == "memory")
        except ImportError as e:
            raise ImportError(
                "Can not load TantivyStore. Install `tantivy` (`ftmq[search]`)"
            ) from e
    raise NotImplementedError(f"Store scheme: `{parsed.scheme}`")
