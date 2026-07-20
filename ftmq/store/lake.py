"""
https://openaleph.org/docs/lib/ftm-datalake/rfc/#basic-layout

A file-like "datalake" statement store based on parquet files and
[deltalake](https://delta-io.github.io/delta-rs/)

Backend has to be local filesystem, s3 or anything else compatible with
`deltalake`

Layout:
    ```
    ./data/
        _delta_log/
            [ix].json
        bucket=[bucket]/  # things, intervals, documents, mentions
            origin=[origin]/
                [uid].parquet
    ```
"""

from contextlib import contextmanager
from functools import cache, cached_property
from pathlib import Path
from typing import Any, Callable, Generator, Iterator, cast
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from anystore.interface.lock import Lock
from anystore.logging import get_logger
from anystore.store import Store as FSStore
from anystore.types import SDict
from anystore.util import clean_dict
from deltalake import (
    BloomFilterProperties,
    ColumnProperties,
    DeltaTable,
    WriterProperties,
    write_deltalake,
)
from deltalake._internal import TableNotFoundError
from deltalake.table import FilterConjunctionType
from followthemoney import EntityProxy, StatementEntity, model
from followthemoney.dataset.dataset import Dataset
from followthemoney.statement import Statement, StatementDict
from nomenklatura import settings as nks
from nomenklatura import store as nk
from nomenklatura.db import get_metadata
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Boolean, DateTime, column, select, table
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement

from ftmq.query import Query
from ftmq.query.sql import SqlSource
from ftmq.store.base import DEFAULT_ORIGIN, Store
from ftmq.store.sql import SQLQueryView, SQLStore
from ftmq.types import StatementEntities
from ftmq.util import apply_dataset, ensure_entity, get_scope_dataset

log = get_logger(__name__)

Z_ORDER = ["canonical_id", "prop"]  # don't add more columns here
TARGET_SIZE = 50 * 10_485_760  # 500 MB
PARTITION_BY = ["dataset", "bucket", "origin"]
BUCKET_MENTION = "mention"  # abstract schema
BUCKET_PAGE = "page"  # abstract schema
BUCKET_DOCUMENT = "document"
BUCKET_INTERVAL = "interval"
BUCKET_THING = "thing"
_STATS_BLOOM = ColumnProperties(
    bloom_filter_properties=BloomFilterProperties(
        set_bloom_filter_enabled=True, fpp=0.01
    ),
    statistics_enabled="CHUNK",
    dictionary_enabled=True,
)
_STATS = ColumnProperties(statistics_enabled="CHUNK", dictionary_enabled=True)
_STATS_NO_DICT = ColumnProperties(statistics_enabled="CHUNK", dictionary_enabled=False)

_COMMON_COLUMNS = {
    "id": _STATS,
    "canonical_id": _STATS,
    "entity_id": _STATS,
    "schema": _STATS,
    "prop": _STATS_BLOOM,
    "dataset": _STATS,
    "lang": _STATS,
    "fragment": _STATS_BLOOM,
    "first_seen": ColumnProperties(statistics_enabled="CHUNK"),
    "last_seen": ColumnProperties(statistics_enabled="CHUNK"),
}
WRITER_SMALL = WriterProperties(
    compression="SNAPPY",
    data_page_size_limit=2 * 1024 * 1024,
    dictionary_page_size_limit=1 * 1024 * 1024,
    max_row_group_size=1_000_000,
    column_properties={**_COMMON_COLUMNS, "value": _STATS_BLOOM},
)
WRITER_LARGE = WriterProperties(
    compression="SNAPPY",
    data_page_size_limit=16 * 1024 * 1024,
    dictionary_page_size_limit=1 * 1024 * 1024,
    max_row_group_size=10_000,
    column_properties={**_COMMON_COLUMNS, "value": _STATS_NO_DICT},
)


def writer_for_bucket(bucket: str) -> WriterProperties:
    return WRITER_LARGE if bucket in (BUCKET_DOCUMENT, BUCKET_PAGE) else WRITER_SMALL


SA_TO_ARROW: dict[type, pa.DataType] = {
    Boolean: pa.bool_(),
    DateTime: pa.timestamp("us"),
}

TABLE = table(
    nks.STATEMENT_TABLE,
    column("id"),
    column("entity_id"),
    column("canonical_id"),
    column("dataset"),
    column("bucket"),
    column("origin"),
    column("source"),
    column("schema"),
    column("prop"),
    column("prop_type"),
    column("value"),
    column("original_value"),
    column("lang"),
    column("external", Boolean),
    column("first_seen", DateTime),
    column("last_seen", DateTime),
    column("fragment"),
)

ARROW_SCHEMA = pa.schema(
    [(col.name, SA_TO_ARROW.get(type(col.type), pa.string())) for col in TABLE.columns]
)


class LakeStatement(Statement):
    """A :class:`followthemoney.statement.Statement` extended with the lake
    row field ``fragment`` – the supersession group key consumers key
    merge-on-read semantics on (a later emission of the same ``(entity_id,
    prop, fragment)`` replaces the earlier one). The empty string is the
    "no fragment" sentinel everywhere – storage never holds NULL fragments.

    Identity semantics (``__eq__`` / ``__hash__`` by ``id``) are unchanged:
    the same content under two fragments is the same statement id, but two
    distinct storage rows. ``clone()`` returns a plain ``Statement`` and
    drops the fragment – re-stamp via :meth:`from_statement` after cloning.
    """

    __slots__ = ["fragment"]

    def __init__(self, *args: Any, fragment: str | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.fragment = fragment or ""

    @property
    def dedupe_key(self) -> str:
        """Stable sort/dedupe key: ``id`` and ``fragment``, tab-joined.

        The same content-addressed ``id`` under distinct fragments is
        distinct storage rows, so anything deduplicating or keying
        statements downstream must key on both. Tab-joining follows the
        :class:`LakeWriter` batch-key idiom and sorts a non-fragment row
        before fragment rows of the same id.
        """
        return f"{self.id}\t{self.fragment}"

    @classmethod
    def from_statement(
        cls, stmt: Statement, fragment: str | None = None
    ) -> "LakeStatement":
        """Upgrade ``stmt`` to a :class:`LakeStatement`, stamping ``fragment``.

        ``None`` preserves the fragment of a passed ``LakeStatement`` (and
        means non-fragment for a plain ``Statement``); pass a string –
        including ``""`` – to set it explicitly. Plain statements are
        copied, lake statements are stamped in place and returned as-is.
        """
        if isinstance(stmt, cls):
            if fragment is not None:
                stmt.fragment = fragment
            return stmt
        return cls(fragment=fragment, **stmt.to_dict())

    @classmethod
    def from_dict(cls, data: StatementDict) -> "LakeStatement":
        stmt = cast("LakeStatement", super().from_dict(data))
        stmt.fragment = cast(dict[str, Any], data).get("fragment") or ""
        return stmt

    @classmethod
    def from_db_row(cls, row: Any) -> "LakeStatement":
        stmt = cast("LakeStatement", super().from_db_row(row))
        stmt.fragment = getattr(row, "fragment", None) or ""
        return stmt


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", secrets_dir="/run/secrets"
    )

    key: str | None = Field(default=None, alias="aws_access_key_id")
    secret: str | None = Field(default=None, alias="aws_secret_access_key")
    endpoint: str | None = Field(
        default=None,
        validation_alias=AliasChoices("aws_endpoint_url", "fsspec_s3_endpoint_url"),
    )

    @property
    def allow_http(self) -> bool:
        if self.endpoint:
            return not self.endpoint.startswith("https")
        return False

    @property
    def duckdb_endpoint(self) -> str | None:
        if not self.endpoint:
            return
        scheme = urlparse(self.endpoint).scheme
        return self.endpoint[len(scheme) + len("://") :]


storage_settings = StorageSettings()


@cache
def storage_options() -> SDict:
    return clean_dict(
        {
            "AWS_ACCESS_KEY_ID": storage_settings.key,
            "AWS_SECRET_ACCESS_KEY": storage_settings.secret,
            "AWS_ENDPOINT_URL": storage_settings.endpoint,
            "AWS_ALLOW_HTTP": str(storage_settings.allow_http),
            "aws_conditional_put": "etag",
        }
    )


@cache
def setup_duckdb_storage() -> None:
    if storage_settings.secret:
        duckdb.query(f"""CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER config,
            KEY_ID '{storage_settings.key}',
            SECRET '{storage_settings.secret}',
            ENDPOINT '{storage_settings.duckdb_endpoint}',
            URL_STYLE 'path',
            USE_SSL '{not storage_settings.allow_http}'
            );""")


@cache
def get_schema_bucket(schema_name: str) -> str:
    s = model[schema_name]
    if s.is_a("Page"):
        return BUCKET_PAGE
    if s.is_a("Mention"):
        return BUCKET_MENTION
    if s.is_a("Document"):
        return BUCKET_DOCUMENT
    if s.is_a("Interval"):
        return BUCKET_INTERVAL
    return BUCKET_THING


def pack_statement(stmt: Statement, source: str | None = None) -> SDict:
    data = stmt.to_db_row()
    data["bucket"] = get_schema_bucket(data["schema"])
    data["source"] = source
    data["origin"] = data["origin"] or DEFAULT_ORIGIN
    data["fragment"] = stmt.fragment if isinstance(stmt, LakeStatement) else ""
    return data


ViewSqlBuilder = Callable[[DeltaTable], str]
"""Returns the SELECT body for a view registered on the LakeStore
connection. The body will be wrapped as ``CREATE OR REPLACE VIEW <name>
AS <body>`` at connection-init time."""


def default_view_sql(dt: DeltaTable) -> str:
    """Default ``view_sqls`` builder for the ``statement`` view.

    Returns a plain ``SELECT * FROM delta_scan('<uri>')`` so the view
    surfaces the raw Delta rows. Consumers that want a deduped view
    (e.g. ``ftm_lakehouse``) pass their own builder via the
    :class:`LakeStore` ``view_sqls`` kwarg.

    Single quotes in the URI are doubled to keep the SQL literal safe
    (the URI is interpolated rather than bound because DuckDB's
    ``delta_scan`` does not accept parameter binding for its path
    argument).
    """
    table_uri = dt.table_uri.replace("'", "''")
    return f"SELECT * FROM delta_scan('{table_uri}')"


class Row:
    """Fake sqlalchemy row-like class yielded by :meth:`LakeStore._execute`.

    Wraps a dict of column → value with both attribute and index access
    so downstream code (built around sqlalchemy ``Row`` objects) keeps
    working unchanged.
    """

    def __init__(self, data: SDict) -> None:
        for key, value in data.items():
            setattr(self, key, value)

    def __iter__(self) -> Generator[Any, None, None]:
        yield from self.__dict__.values()

    def __getitem__(self, i: int) -> Any:
        return list(self.__iter__())[i]


class LakeQueryView(SQLQueryView):
    def query(self, query: Query | None = None) -> StatementEntities:
        if query:
            query = self.ensure_scoped_query(query)
            yield from self.store._iterate(self._sql(query).statements)
        else:
            yield from super().query(query)


class LakeStore(SQLStore[LakeQueryView]):
    @property
    def source(self) -> SqlSource:
        """The lake statement view, with schema-filter -> `bucket` partition
        pruning folded into every compiled query."""
        return SqlSource(
            self.table, prune={"schema": get_schema_bucket}, prune_column="bucket"
        )

    def __init__(self, *args, **kwargs) -> None:
        self._backend = FSStore(uri=kwargs.pop("uri"))
        self._partition_by = kwargs.pop("partition_by", PARTITION_BY)
        self._lock: Lock = kwargs.pop("lock", Lock(self._backend))
        self._enforce_dataset = kwargs.pop("enforce_dataset", False)
        self._view_filter: ColumnElement | None = kwargs.pop("view_filter", None)
        view_sqls: dict[str, ViewSqlBuilder] | None = kwargs.pop("view_sqls", None)
        self._view_sqls: dict[str, ViewSqlBuilder] = view_sqls or {
            nks.STATEMENT_TABLE: default_view_sql,
        }
        self._duckdb_config: dict[str, str] = kwargs.pop("duckdb_config", None) or {}
        kwargs["uri"] = "sqlite:///:memory:"  # fake it till you make it
        get_metadata.cache_clear()
        super().__init__(*args, **kwargs)
        self.table = TABLE
        self.uri = self._backend.uri
        setup_duckdb_storage()

    @property
    def deltatable(self) -> DeltaTable:
        return DeltaTable(self.uri, storage_options=storage_options())

    @property
    def exists(self) -> bool:
        try:
            self.deltatable.version()
            return True
        except TableNotFoundError:
            return False

    @cached_property
    def _duckdb(self) -> duckdb.DuckDBPyConnection:
        """Persistent DuckDB connection with all configured views registered.

        The Delta extension is auto-installed / auto-loaded on first
        ``delta_scan`` use thanks to the connection-level flags; no
        explicit ``INSTALL`` / ``LOAD`` is needed. Views in
        :attr:`_view_sqls` are registered at first access of this
        property and persist for the lifetime of the ``LakeStore``.

        DuckDB connections are not thread-safe; callers must use
        :meth:`cursor` to get a thread-isolated child connection that
        shares the catalog and registered views.

        The session timezone is forced to UTC – DuckDB otherwise renders
        TIMESTAMPTZ in the host timezone, leaking local-time datetimes to
        consumers. Set GLOBAL so :meth:`cursor` sessions inherit it.
        """
        config = {
            "autoinstall_known_extensions": "true",
            "autoload_known_extensions": "true",
            **self._duckdb_config,
        }
        con = duckdb.connect(":memory:", config=config)
        # icu ships bundled with the duckdb wheel, so this works offline
        con.execute("LOAD icu; SET GLOBAL TimeZone='UTC'")
        dt = self.deltatable
        for name, builder in self._view_sqls.items():
            con.sql(f"CREATE OR REPLACE VIEW {name} AS {builder(dt)}")
        return con

    @contextmanager
    def cursor(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield a thread-isolated cursor on :attr:`_duckdb`.

        Use as ``with store.cursor() as cur:`` for any synchronous
        query. Generators that need the cursor alive while streaming
        must pin it in their closure so it isn't closed before
        consumption finishes.
        """
        cur = self._duckdb.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def _apply_filters(self, q: Select) -> Select:
        """Hook for subclasses to inject additional WHERE clauses.

        Called before every query execution. Override to add partition
        filters, tenant scoping, etc. The default implementation is a
        no-op — filters return the query unchanged.
        """
        return q

    def _execute(self, q: Select, stream: bool = True) -> Generator[Any, None, None]:
        if not self.exists:
            return
        q = self._apply_filters(q)
        if self._view_filter is not None:
            q = q.where(self._view_filter)
        sql = str(q.compile(compile_kwargs={"literal_binds": True}))
        with self.cursor() as cur:
            res = cur.execute(sql)
            cols = (
                res.columns
                if hasattr(res, "columns")
                else [d[0] for d in res.description]
            )
            while rows := res.fetchmany(100_000):
                for row in rows:
                    yield Row(dict(zip(cols, row)))

    def get_scope(self) -> Dataset:
        if "dataset" not in self._partition_by:
            return super().get_scope()
        names: set[str] = set()
        for child in self._backend._fs.ls(self._backend.uri):
            name = Path(child).name
            if name.startswith("dataset="):
                names.add(name.split("=")[1])
        return get_scope_dataset(*names)

    def view(
        self, scope: Dataset | None = None, external: bool = False
    ) -> LakeQueryView:
        scope = scope or self.dataset
        return LakeQueryView(self, scope, external)

    def writer(
        self, origin: str | None = DEFAULT_ORIGIN, source: str | None = None
    ) -> "LakeWriter":
        return LakeWriter(self, origin=origin or DEFAULT_ORIGIN, source=source)

    def get_origins(self) -> set[str]:
        q = select(self.table.c.origin).distinct()
        return set([r.origin for r in self._execute(q)])


class LakeWriter(nk.Writer):
    store: LakeStore
    BATCH_STATEMENTS = 1_000_000

    def __init__(
        self,
        store: Store,
        origin: str | None = DEFAULT_ORIGIN,
        source: str | None = None,
    ):
        super().__init__(store)
        self.batch: dict[str, tuple[Statement, str | None]] = {}
        self.origin = origin or DEFAULT_ORIGIN
        self.source = source

    def add_statement(self, stmt: Statement, source: str | None = None) -> None:
        if stmt.entity_id is None:
            return
        stmt.origin = stmt.origin or self.origin
        canonical_id = self.store.linker.get_canonical(stmt.entity_id)
        stmt.canonical_id = canonical_id
        dedupe = stmt.dedupe_key if isinstance(stmt, LakeStatement) else stmt.id
        key = f"{canonical_id}\t{dedupe}"
        self.batch[key] = (stmt, source or self.source)

    def add_entity(
        self,
        entity: EntityProxy,
        origin: str | None = None,
        source: str | None = None,
    ) -> None:
        e = ensure_entity(entity, StatementEntity, self.store.dataset)
        if self.store._enforce_dataset:
            e = apply_dataset(e, self.store.dataset, replace=True)
        for stmt in e.statements:
            if origin:
                stmt.origin = origin
            self.add_statement(stmt, source=source)
        # we check here instead of in `add_statement` as this will keep entities
        # together in the same parquet files
        if len(self.batch) >= self.BATCH_STATEMENTS:
            self.flush()

    def _build_table(self) -> pa.Table:
        rows: list[SDict] = []
        for key in sorted(self.batch):
            stmt, source = self.batch[key]
            rows.append(pack_statement(stmt, source))
        return pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)

    def flush(self) -> None:
        if not self.batch:
            self.batch = {}
            return
        log.info(
            f"Write {len(self.batch)} statements to deltalake ...",
            uri=self.store.uri,
        )
        table = self._build_table()
        with self.store._lock:
            for bucket in table.column("bucket").unique().to_pylist():
                split = table.filter(pc.equal(table.column("bucket"), bucket)).sort_by(
                    [
                        ("entity_id", "ascending"),
                        ("prop", "ascending"),
                    ]
                )
                write_deltalake(
                    str(self.store.uri),
                    split,
                    partition_by=self.store._partition_by,
                    mode="append",
                    schema_mode="merge",
                    writer_properties=writer_for_bucket(bucket),
                    target_file_size=TARGET_SIZE,
                    storage_options=storage_options(),
                    configuration={"delta.enableChangeDataFeed": "true"},
                )
        self.batch = {}

    def pop(self, entity_id: str) -> list[Statement]:
        q = select(TABLE)
        q = q.where(TABLE.c.canonical_id == entity_id)
        statements: list[Statement] = []
        for row in self.store._execute(q):
            statements.append(LakeStatement.from_db_row(row))

        self.store.deltatable.delete(f"canonical_id = '{entity_id}'")
        return statements

    def optimize(
        self,
        vacuum: bool | None = False,
        vacuum_keep_hours: int | None = 0,
        dataset: str | None = None,
        bucket: str | None = None,
        origin: str | None = None,
    ) -> None:
        """
        Optimize the storage: Z-Ordering and compacting

        Args:
            vacuum: Run vacuum after optimization
            vacuum_keep_hours: Retention hours for vacuum
            dataset: Filter optimization to specific dataset partition
            bucket: Filter optimization to specific bucket partition
            origin: Filter optimization to specific origin partition
        """
        base_filters: FilterConjunctionType = []
        if dataset is not None:
            base_filters.append(("dataset", "=", dataset))
        if origin is not None:
            base_filters.append(("origin", "=", origin))

        with self.store._lock:
            if bucket is not None:
                filters = list(base_filters) + [("bucket", "=", bucket)]
                self.store.deltatable.optimize.z_order(
                    Z_ORDER,
                    writer_properties=writer_for_bucket(bucket),
                    target_size=TARGET_SIZE,
                    partition_filters=filters or None,
                )
            else:
                all_buckets = [
                    BUCKET_THING,
                    BUCKET_INTERVAL,
                    BUCKET_MENTION,
                    BUCKET_DOCUMENT,
                ]
                for b in all_buckets:
                    filters = list(base_filters) + [("bucket", "=", b)]
                    self.store.deltatable.optimize.z_order(
                        Z_ORDER,
                        writer_properties=writer_for_bucket(b),
                        target_size=TARGET_SIZE,
                        partition_filters=filters,
                    )
            if vacuum:
                self.store.deltatable.vacuum(
                    retention_hours=vacuum_keep_hours,
                    enforce_retention_duration=False,
                    dry_run=False,
                    full=True,
                )
