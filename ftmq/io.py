from typing import Any, Iterable, Type

import orjson
from anystore.io import Uri, smart_open, smart_stream
from banal import is_listish
from followthemoney import E, StatementEntity, ValueEntity

from ftmq.logging import get_logger
from ftmq.query import Query
from ftmq.store import Store, get_store
from ftmq.types import Entities, Entity
from ftmq.util import ensure_entity, make_entity

log = get_logger(__name__)

DEFAULT_MODE = "rb"


def smart_get_store(uri: Uri, **kwargs) -> Store | None:
    try:
        return get_store(uri, **kwargs)
    except NotImplementedError:
        return


def smart_read_proxies(
    uri: Uri | Iterable[Uri],
    query: Query | None = None,
    entity_type: Type[E] | None = ValueEntity,
    **store_kwargs: Any,
) -> Entities:
    """
    Stream proxies from an arbitrary source

    Example:
        ```python
        from ftmq import Query
        from ftmq.io import smart_read_proxies

        # remote file-like source
        for proxy in smart_read_proxies("s3://data/entities.ftm.json"):
            print(proxy.schema)

        # multiple files
        for proxy in smart_read_proxies("./1.json", "./2.json"):
            print(proxy.schema)

        # nomenklatura store
        for proxy in smart_read_proxies("redis://localhost", dataset="default"):
            print(proxy.schema)

        # apply a query to sql storage
        q = Query(dataset="my_dataset", schema="Person")
        for proxy in smart_read_proxies("sqlite:///data/ftm.db", query=q):
            print(proxy.schema)
        ```

    Args:
        uri: File-like uri or store uri or multiple uris
        query: Filter `Query` object
        **store_kwargs: Pass through configuration to statement store

    Yields:
        A generator of `Entity` instances
    """
    entity_type = entity_type or ValueEntity
    if is_listish(uri):
        for u in uri:
            yield from smart_read_proxies(u, query, entity_type)
        return

    store = smart_get_store(uri, **store_kwargs)
    if store is not None:
        view = store.view()
        yield from view.query(query)
        return

    q = query or Query()
    lines = smart_stream(uri)
    lines = (orjson.loads(line) for line in lines)
    proxies = (make_entity(line, entity_type) for line in lines)
    yield from q.apply_iter(proxies)


def smart_write_proxies(
    uri: Uri,
    proxies: Iterable[Entity],
    mode: str | None = "wb",
    **store_kwargs: Any,
) -> int:
    """
    Write a stream of proxies (or data dicts) to an arbitrary target.

    Example:
        ```python
        from ftmq.io import smart_write_proxies

        proxies = [...]

        # to a remote cloud storage
        smart_write_proxies("s3://data/entities.ftm.json", proxies)

        # to a redis statement store
        smart_write_proxies("redis://localhost", proxies, dataset="my_dataset")
        ```

    Args:
        uri: File-like uri or store uri
        proxies: Iterable of proxy data
        mode: Open mode for file-like targets (default: `wb`)
        **store_kwargs: Pass through configuration to statement store

    Returns:
        Number of written proxies
    """
    ix = 0
    if not proxies:
        return ix

    store = smart_get_store(uri, **store_kwargs)
    if store is not None:
        proxies = (
            ensure_entity(p, StatementEntity, store_kwargs.get("dataset"))
            for p in proxies
        )
        with store.writer() as bulk:
            for proxy in proxies:
                ix += 1
                bulk.add_entity(proxy)
        return ix

    with smart_open(uri, mode=mode) as fh:
        for proxy in proxies:
            ix += 1
            data = proxy.to_dict()
            fh.write(orjson.dumps(data, option=orjson.OPT_APPEND_NEWLINE))
    return ix
