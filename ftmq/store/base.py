from functools import cache, wraps
from typing import Any, Iterable, TypeAlias
from urllib.parse import urlparse

from anystore.logging import get_logger
from followthemoney import Statement
from followthemoney.dataset.dataset import Dataset
from nomenklatura import db as nk_db
from nomenklatura import store as nk
from nomenklatura.db import Session, get_engine
from nomenklatura.resolver import Resolver
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from ftmq.model.stats import Collector, DatasetStats
from ftmq.query import Query
from ftmq.query.aggregations import AggregatorResult
from ftmq.statements import cast_statement
from ftmq.types import StatementEntities, StatementEntity
from ftmq.util import ensure_dataset

log = get_logger(__name__)

DEFAULT_ORIGIN = "default"


def _memory_engine(url: str = "sqlite:///:memory:") -> Engine:
    """A thread-safe in-memory sqlite engine.

    One shared connection (``StaticPool``) reachable from any thread
    (``check_same_thread=False``). nomenklatura's default factory omits both,
    so under a threaded server (granian ``mt`` / the anyio threadpool) a
    connection opened on one worker thread is closed on another and raises
    ``sqlite3.ProgrammingError: SQLite objects created in a thread can only be
    used in that same thread``. Mirrors the lakehouse ``SqlJournalStore``.
    """
    return create_engine(
        url, connect_args={"check_same_thread": False}, poolclass=StaticPool
    )


_PATCHED = False


def _patch_nomenklatura_sqlite_engines() -> None:
    """Route nomenklatura's in-memory sqlite engines through :func:`_memory_engine`.

    The resolver and every ``LakeStore`` (via ``SQLStore.__init__`` →
    ``get_engine(uri)``) share one process-cached engine per URL. ``LakeStore``
    fakes ``sqlite:///:memory:`` and exposes no seam to configure that engine,
    so the factory is wrapped once at import. Only sqlite ``:memory:`` URLs are
    intercepted; file, postgres and duckdb engines keep nomenklatura's
    behaviour (``check_same_thread`` is a sqlite connect arg - passing it to
    another driver raises).
    """
    global _PATCHED
    if _PATCHED:
        return
    _orig = nk_db._make_engine
    _engines: dict[str, Engine] = {}

    @wraps(_orig)
    def _make_engine(url: str) -> Engine:
        if url.startswith("sqlite") and url.endswith(":memory:"):
            if url not in _engines:
                _engines[url] = _memory_engine(url)
            return _engines[url]
        return _orig(url)

    nk_db._make_engine = _make_engine
    _PATCHED = True


_patch_nomenklatura_sqlite_engines()


@cache
def get_resolver(uri: str | None = None) -> Resolver[StatementEntity]:
    if uri and "sql" in urlparse(uri).scheme:
        engine = get_engine(uri)
    else:
        engine = _memory_engine()
    return Resolver[StatementEntity](Session(engine), create=True)


_CASTING_WRITERS: dict[type[Any], type[Any]] = {}


def _casting_writer(cls: type[Any]) -> type[Any]:
    """A backend writer class that casts statement values on the way in."""
    if cls not in _CASTING_WRITERS:

        class CastingWriter(cls):  # type: ignore[misc]
            # no attributes of its own, so an instance can be reblessed into it
            __slots__ = ()

            def add_statement(self, stmt: Statement, *args: Any, **kwargs: Any) -> None:
                # a value that doesn't parse is written as it came in
                super().add_statement(cast_statement(stmt) or stmt, *args, **kwargs)

        CastingWriter.__name__ = f"Casting{cls.__name__}"
        _CASTING_WRITERS[cls] = CastingWriter
    return _CASTING_WRITERS[cls]


Writer: TypeAlias = nk.Writer[Dataset, StatementEntity]


class Store(nk.Store[Dataset, StatementEntity]):
    """
    Feature add-ons to `nomenklatura.store.Store`
    """

    def __init__(
        self,
        dataset: Dataset | str | None = None,
        linker: Resolver | None = None,
        cast_types: bool = True,
        **kwargs,
    ) -> None:
        """
        Initialize a store. This should be called via
        [`get_store`][ftmq.store.get_store]

        Args:
            dataset: A `followthemoney.Dataset` instance to limit the scope to
            linker: A `nomenklatura.Resolver` instance with linked / deduped data
            cast_types: Normalize statement values on write (see
                [`ftmq.statements`][ftmq.statements])
        """
        # An unscoped store (no explicit `dataset`) implicitly spans every
        # dataset present in the backend. nomenklatura scopes a view to
        # `dataset.leaf_names`, so without this the store would only surface
        # entities literally tagged `dataset="default"`. Resolved lazily (see
        # `scope`) so opening a store never queries the backend.
        self._implicit_scope = dataset is None
        self.cast_types = cast_types
        linker = linker or get_resolver(kwargs.get("uri"))
        super().__init__(dataset=ensure_dataset(dataset), linker=linker, **kwargs)

    def writer(self, *args: Any, **kwargs: Any) -> Writer:
        """The backend writer, normalizing statement values on the way in.

        Values are cast into the canonical format of their property type (see
        [`ftmq.statements`][ftmq.statements]); values that don't parse are
        passed through unchanged (`ftmq statements cast-types --drop-invalid`
        cleans those out of an existing dump). Disable with the store's
        `cast_types=False`.
        """
        return self.casting_writer(super().writer(*args, **kwargs))

    def casting_writer(self, writer: Writer) -> Writer:
        """Rebless a backend writer so it casts statement values on write (see
        [`writer`][ftmq.store.base.Store.writer]). A store that builds its
        writer itself has to route it through here."""
        if self.cast_types:
            cls: type[Any] = type(writer)
            writer.__class__ = _casting_writer(cls)
        return writer

    def get_scope(self) -> Dataset:
        """
        Return implicit `Dataset` computed from current datasets in store
        """
        raise NotImplementedError

    @property
    def scope(self) -> Dataset:
        """The effective read scope: the store's explicit `dataset`, or all
        datasets present in the backend when it was opened without one."""
        return self.get_scope() if self._implicit_scope else self.dataset

    def view(self, scope: Dataset | None = None, external: bool = False) -> "View":
        raise NotImplementedError

    def default_view(self, external: bool = False) -> "View":
        return self.view(self.scope, external)

    def iterate(self, dataset: str | Dataset | None = None) -> StatementEntities:
        """
        Iterate all the entities, optional filter for a dataset.

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
    """
    Feature add-ons to `nomenklatura.store.base.View`
    """

    def query(self, query: Query | None = None) -> StatementEntities:
        """
        Get the entities of a store, optionally filtered by a
        [`Query`][ftmq.Query] object.

        Args:
            query: The Query filter object

        Yields:
            Generator of `followthemoney.StatementEntity`
        """
        view = self.store.view(self.scope)
        if query:
            yield from query.apply_iter(view.entities())
        else:
            yield from view.entities()

    def get_adjacents(
        self, proxies: Iterable[StatementEntity], inverted: bool | None = False
    ) -> set[StatementEntity]:
        seen: set[StatementEntity] = set()
        for proxy in proxies:
            for _, adjacent in self.get_adjacent(proxy, inverted=bool(inverted)):
                if adjacent.id not in seen:
                    seen.add(adjacent)
        return seen

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
