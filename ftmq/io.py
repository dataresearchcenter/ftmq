import io
from typing import IO, Any, BinaryIO, Iterable, Type, cast

import orjson
from anystore.io import smart_open, smart_stream
from anystore.logging import get_logger
from anystore.types import Uri
from banal import is_listish
from followthemoney import E, Statement, StatementEntity, ValueEntity
from followthemoney.statement import CSV, PACK, read_statements
from followthemoney.statement.serialize import get_statement_writer

from ftmq.query import Query
from ftmq.store import Store, get_store
from ftmq.store.base import get_preserving_linker
from ftmq.types import Entities, Entity, Statements
from ftmq.util import ensure_entity, make_entity

log = get_logger(__name__)


class KeepOpen(io.RawIOBase):
    """Pass writes through to a handle that outlives this wrapper.

    The csv / pack statement writers wrap the output in a `TextIOWrapper`,
    which closes what it wraps as soon as it is collected - tearing down a
    shared stdout for whatever else runs in the same process. Closing the real
    handle is `smart_open`'s job.
    """

    def __init__(self, fh: IO[bytes]) -> None:
        self.fh = fh

    def writable(self) -> bool:
        return True

    def write(self, data: Any) -> int:
        return self.fh.write(data)

    def flush(self) -> None:
        # the statement writers only flush their `TextIOWrapper`, never close
        # it, so it flushes once more when it is finally collected - by then
        # `smart_open` has long closed the handle underneath
        if not self.fh.closed:
            self.fh.flush()


def _warn_pack(uri: Uri) -> None:
    log.warning(
        "The `pack` format can not carry canonical ids - a merged entity "
        "reads back as its referents.",
        uri=str(uri),
    )


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
        from ftmq import Query, M
        from ftmq.io import smart_read_proxies

        # remote file-like source
        for proxy in smart_read_proxies("s3://data/entities.ftm.json"):
            print(proxy.schema)

        # multiple files
        for proxy in smart_read_proxies(["./1.json", "./2.json"]):
            print(proxy.schema)

        # nomenklatura store
        for proxy in smart_read_proxies("redis://localhost", dataset="default"):
            print(proxy.schema)

        # apply a query to sql storage
        q = Query().where(M(dataset="my_dataset"), M(schema="Person"))
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
        # the *default* view: `view()` without a scope falls back to the
        # store's explicit `dataset`, which for a store opened without one is
        # the "default" writer scope - so a store uri without `dataset=` would
        # read back nothing (see `Store.scope`)
        view = store.default_view()
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


def smart_read_statements(
    uri: Uri, format: str = CSV, **store_kwargs: Any
) -> Statements:
    """
    Stream raw statements from a store or a statement stream file.

    Example:
        ```python
        from ftmq.io import smart_read_statements

        for stmt in smart_read_statements("sqlite:///followthemoney.store"):
            print(stmt.canonical_id, stmt.prop, stmt.value)
        ```

    Args:
        uri: Store uri or file-like uri of a statement stream
        format: Statement stream format (`csv`, `json` or `pack`), for a
            file-like uri only
        **store_kwargs: Pass through configuration to statement store

    Yields:
        A generator of `followthemoney.Statement` instances
    """
    # not a generator function: a store that can't dump its statements has to
    # say so before the caller has written a header into its output
    store = smart_get_store(uri, **store_kwargs)
    if store is not None:
        return store.statements(store_kwargs.get("dataset"))
    if format == PACK:
        _warn_pack(uri)
    return _stream_statements(uri, format)


def _stream_statements(uri: Uri, format: str) -> Statements:
    with smart_open(uri, mode="rb") as fh:
        yield from read_statements(cast(BinaryIO, fh), format=format)


def smart_write_statements(
    uri: Uri,
    statements: Iterable[Statement],
    format: str = CSV,
    **store_kwargs: Any,
) -> int:
    """
    Write a stream of statements to a store or a statement stream file.

    A statement is written as it comes in: its `canonical_id` is preserved
    verbatim, never re-derived from the target store's linker (see
    [`PreservingLinker`][ftmq.store.base.PreservingLinker]), so a stream that
    was resolved with `nomenklatura apply-statements` stays resolved. For a
    SQL-family store the write is an upsert keyed on the statement id, so
    loading a resolved dump back into the store it came from updates those rows
    in place.

    Example:
        ```python
        from ftmq.io import smart_read_statements, smart_write_statements

        statements = smart_read_statements("statements.csv")
        smart_write_statements("sqlite:///followthemoney.store", statements)
        ```

    Args:
        uri: Store uri or file-like uri of a statement stream
        statements: Iterable of `followthemoney.Statement` instances
        format: Statement stream format (`csv`, `json` or `pack`), for a
            file-like uri only
        **store_kwargs: Pass through configuration to statement store

    Returns:
        Number of written statements
    """
    ix = 0
    linker = get_preserving_linker()
    store = smart_get_store(uri, linker=linker, **store_kwargs)
    if store is not None:
        with store.writer() as bulk:
            for stmt in statements:
                linker.preserve(stmt)
                ix += 1
                bulk.add_statement(stmt)
        return ix

    if format == PACK:
        _warn_pack(uri)
    with smart_open(uri, mode="wb") as out_fh:
        fh = cast(BinaryIO, KeepOpen(cast(IO[bytes], out_fh)))
        with get_statement_writer(fh, format) as writer:
            for stmt in statements:
                ix += 1
                writer.write(stmt)
    return ix
