from functools import cache
from typing import Any, Iterable, TypeAlias
from urllib.parse import urlparse

from anystore.io import smart_stream
from anystore.logging import get_logger
from anystore.types import Uri
from followthemoney import Statement
from followthemoney.dataset.dataset import Dataset
from nomenklatura import db as nk_db
from nomenklatura import settings
from nomenklatura import store as nk
from nomenklatura.db import Session
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Edge, Linker, Resolver
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from ftmq.model.stats import Collector, DatasetStats
from ftmq.query import Query
from ftmq.query.aggregations import AggregatorResult
from ftmq.statements import cast_statement
from ftmq.types import StatementEntities, StatementEntity, Statements
from ftmq.util import ensure_dataset

log = get_logger(__name__)

DEFAULT_ORIGIN = "default"


def _memory_engine(url: str = "sqlite:///:memory:") -> Engine:
    """An in-memory sqlite engine on one shared connection usable from any thread."""
    return create_engine(
        url, connect_args={"check_same_thread": False}, poolclass=StaticPool
    )


def get_engine(uri: str | None = None) -> Engine:
    """The process-wide engine for a sql database uri.

    An in-memory sqlite url gets one shared, thread-safe engine, so every store
    and resolver on it sees the same database.

    Args:
        uri: A sql database uri, defaults to `NOMENKLATURA_DB_URL`
    """
    uri = uri or settings.DB_URL
    if uri.startswith("sqlite") and uri.endswith(":memory:"):
        return _shared_memory_engine(uri)
    return nk_db.get_engine(uri)


@cache
def _shared_memory_engine(url: str) -> Engine:
    return _memory_engine(url)


def _is_sql_uri(uri: str) -> bool:
    return "sql" in urlparse(uri).scheme


def _sql_resolver(session: Session) -> Resolver[StatementEntity]:
    return Resolver[StatementEntity](session, create=True)


def _resolver_session(uri: str | None = None) -> Session:
    """A session on the given sql database, or an ephemeral in-memory one."""
    engine = get_engine(uri) if uri and _is_sql_uri(uri) else _memory_engine()
    return Session(engine)


@cache
def get_resolver(uri: str | None = None) -> Resolver[StatementEntity]:
    """The read/write `Resolver` backed by a sql `resolver` table.

    Args:
        uri: A sql database uri. Anything else (or nothing) gets an ephemeral
            in-memory table.

    Returns:
        The resolver, with its decisions loaded. Cached and never refreshed:
            call `load_into_memory()` to see another session's writes.
    """
    resolver = _sql_resolver(_resolver_session(uri))
    # a fresh `Resolver` resolves nothing until its judgements are indexed
    resolver.load_into_memory()
    return resolver


@cache
def get_linker(uri: Uri) -> Linker[StatementEntity]:
    """A read-only `Linker` (merge decisions only) for read paths such as the api.

    Args:
        uri: A sql database uri with a nomenklatura `resolver` table, or any
            anystore uri of a json lines edge dump (`Resolver.dump()` /
            `nomenklatura dump-resolver`)

    Returns:
        The linker. This is a cached object.
    """
    uri = str(uri)
    if _is_sql_uri(uri):
        with _resolver_session(uri) as session:
            return _sql_resolver(session).get_linker()
    linker: Linker[StatementEntity] = Linker({})
    merges = 0
    for line in smart_stream(uri, mode="r"):
        line = line.strip()
        if not line:
            continue
        edge = Edge.from_line(line)
        # the dump also holds negative and unsure judgements; only positive merge
        if edge.judgement == Judgement.POSITIVE and edge.deleted_at is None:
            linker.add(edge.source.id, edge.target.id)
            merges += 1
    log.info(f"Loaded `{merges}` merges.", uri=uri)
    return linker


class PreservingLinker(Linker[StatementEntity]):
    """A linker that answers with the canonical id a statement already carries.

    Backend writers stamp `canonical_id` from the store's linker: point the store
    at this one and [`preserve`][ftmq.store.base.PreservingLinker.preserve] each
    statement before writing it to keep its canonical id. Any other id (such as
    entity-typed values) maps to itself.
    """

    def __init__(self) -> None:
        super().__init__({})
        self._stmt: Statement | None = None

    def preserve(self, stmt: Statement) -> None:
        """Resolve this statement's entity id to the canonical id it carries."""
        self._stmt = stmt

    def get_canonical(self, entity_id: str) -> str:
        if self._stmt is not None and entity_id == self._stmt.entity_id:
            return self._stmt.canonical_id
        return entity_id


@cache
def get_preserving_linker() -> PreservingLinker:
    """The process-wide [`PreservingLinker`][ftmq.store.base.PreservingLinker].

    Cached, as `get_store` keys its cache on the linker. It tracks one statement
    at a time, so concurrent statement writes into two stores are not supported.
    """
    return PreservingLinker()


@cache
def _casting_writer(cls: type[Any]) -> type[Any]:
    """A backend writer class that casts statement values on the way in."""

    class CastingWriter(cls):  # type: ignore[misc]
        # no attributes of its own, so an instance can be reblessed into it
        __slots__ = ()

        def add_statement(self, stmt: Statement, *args: Any, **kwargs: Any) -> None:
            # a value that doesn't parse is written as it came in
            super().add_statement(cast_statement(stmt) or stmt, *args, **kwargs)

    CastingWriter.__name__ = f"Casting{cls.__name__}"
    return CastingWriter


Writer: TypeAlias = nk.Writer[Dataset, StatementEntity]


class Store(nk.Store[Dataset, StatementEntity]):
    """Feature add-ons to `nomenklatura.store.Store`."""

    def __init__(
        self,
        dataset: Dataset | str | None = None,
        linker: Linker | None = None,
        cast_types: bool = True,
        **kwargs,
    ) -> None:
        """Initialize a store, use [`get_store`][ftmq.store.get_store] instead.

        Args:
            dataset: A `followthemoney.Dataset` instance to limit the scope to
            linker: A `nomenklatura.Linker` instance with linked / deduped data,
                defaults to [`get_resolver`][ftmq.store.base.get_resolver]
            cast_types: Normalize statement values on write (see
                [`ftmq.statements`][ftmq.statements])
        """
        # without a `dataset` the store spans all datasets, resolved in `scope`
        self._implicit_scope = dataset is None
        self.cast_types = cast_types
        # only the SQL family passes a `uri`; nomenklatura's stores don't take one
        uri = kwargs.pop("uri", None)
        linker = linker or get_resolver(uri)
        super().__init__(dataset=ensure_dataset(dataset), linker=linker, **kwargs)

    def writer(self, *args: Any, **kwargs: Any) -> Writer:
        """The backend writer, casting statement values on write.

        Values are cast into their property type's canonical format (see
        [`ftmq.statements`][ftmq.statements]), values that don't parse pass
        through unchanged. Disable with the store's `cast_types=False`.
        """
        return self.casting_writer(super().writer(*args, **kwargs))

    def casting_writer(self, writer: Writer) -> Writer:
        """Rebless a backend writer so it casts statement values on write.

        A store that builds its writer itself has to route it through here.
        """
        if self.cast_types:
            cls: type[Any] = type(writer)
            writer.__class__ = _casting_writer(cls)
        return writer

    def get_scope(self) -> Dataset:
        """Return the implicit `Dataset` spanning all datasets in the store."""
        raise NotImplementedError

    @property
    def scope(self) -> Dataset:
        """The read scope: the explicit `dataset`, or all datasets in the store."""
        return self.get_scope() if self._implicit_scope else self.dataset

    view_class: type["View"]

    def view(self, scope: Dataset | None = None, external: bool = False) -> "View":
        return self.view_class(self, scope or self.dataset, external=external)

    def default_view(self, external: bool = False) -> "View":
        return self.view(self.scope, external)

    def statements(self, dataset: str | Dataset | None = None) -> Statements:
        """Iterate the raw statements in this store, as they are stored.

        Unlike [`iterate`][ftmq.store.base.Store.iterate] (entity assembly
        rewrites values and synthesizes an `id` statement), a dump of these
        loads back onto its own rows. Includes external statements, unordered.
        SQL family only.

        Args:
            dataset: `Dataset` instance or name to limit scope to

        Yields:
            Generator of `followthemoney.Statement`
        """
        raise NotImplementedError

    def iterate(self, dataset: str | Dataset | None = None) -> StatementEntities:
        """Iterate all the entities, optionally limited to a dataset.

        Args:
            dataset: `Dataset` instance or name to limit scope to

        Yields:
            Generator of `nomenklatura.entity.CompositeEntity`
        """
        if dataset is not None:
            view = self.view(ensure_dataset(dataset))
        else:
            view = self.default_view()
        yield from view.entities()


class View(nk.View[Dataset, StatementEntity]):
    """Feature add-ons to `nomenklatura.store.base.View`."""

    def query(self, query: Query | None = None) -> StatementEntities:
        """Get the entities of the view, optionally filtered by a [`Query`][ftmq.Query].

        Args:
            query: The Query filter object

        Yields:
            Generator of `followthemoney.StatementEntity`
        """
        if query:
            yield from query.apply_iter(self.entities())
        else:
            yield from self.entities()

    def get_adjacents(
        self, proxies: Iterable[StatementEntity], inverted: bool | None = False
    ) -> set[StatementEntity]:
        return {
            adjacent
            for proxy in proxies
            for _, adjacent in self.get_adjacent(proxy, inverted=bool(inverted))
        }

    def stats(self, query: Query | None = None) -> DatasetStats:
        c = Collector()
        cov = c.collect_many(self.query(query))
        return cov

    def count(self, query: Query | None = None) -> int:
        return self.stats(query).entity_count or 0

    def aggregations(self, query: Query) -> AggregatorResult | None:
        if not query.aggregations:
            return
        _ = [x for x in self.query(query)]
        if query.aggregator:
            res = dict(query.aggregator.result)
            return res
