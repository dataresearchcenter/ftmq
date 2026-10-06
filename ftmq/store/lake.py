"""A deltalake statement store (parquet, partitioned by `PARTITION_BY`), via duckdb.

Layout: https://openaleph.org/docs/lib/ftm-datalake/rfc/#basic-layout
"""

from contextlib import contextmanager
from datetime import datetime
from functools import cache, cached_property
from pathlib import Path
from typing import Any, Callable, Generator, Iterable, Iterator, cast
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
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Boolean, DateTime, column, select, table
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement

from ftmq.query.sql import PruneFn, SqlSource, prune_by_schema
from ftmq.store.base import DEFAULT_ORIGIN, Store
from ftmq.store.sql import SQLStore
from ftmq.util import apply_dataset, ensure_entity, get_scope_dataset, iso_datetime

log = get_logger(__name__)

Z_ORDER = ["canonical_id", "prop"]  # don't add more columns here
TARGET_SIZE = 50 * 10_485_760  # 500 MB
PARTITION_BY = ["dataset", "bucket", "origin"]
BUCKET_MENTION = "mention"  # abstract schema
BUCKET_PAGE = "page"  # abstract schema
BUCKET_DOCUMENT = "document"
BUCKET_INTERVAL = "interval"
BUCKET_THING = "thing"
ALL_BUCKETS = [
    BUCKET_THING,
    BUCKET_INTERVAL,
    BUCKET_MENTION,
    BUCKET_DOCUMENT,
    BUCKET_PAGE,
]
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
    compression="ZSTD",
    compression_level=3,
    data_page_size_limit=2 * 1024 * 1024,
    dictionary_page_size_limit=1 * 1024 * 1024,
    max_row_group_size=1_000_000,
    column_properties={**_COMMON_COLUMNS, "value": _STATS_BLOOM},
)
WRITER_LARGE = WriterProperties(
    compression="ZSTD",
    compression_level=3,
    data_page_size_limit=16 * 1024 * 1024,
    dictionary_page_size_limit=1 * 1024 * 1024,
    max_row_group_size=10_000,
    column_properties={**_COMMON_COLUMNS, "value": _STATS_NO_DICT},
)


def writer_for_bucket(bucket: str) -> WriterProperties:
    return WRITER_LARGE if bucket in (BUCKET_DOCUMENT, BUCKET_PAGE) else WRITER_SMALL


SA_TO_ARROW: dict[type, pa.DataType] = {
    Boolean: pa.bool_(),
    DateTime: pa.timestamp("us", tz="UTC"),
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
    """A `Statement` with the lake row field `fragment` (`""` for none, never NULL).

    Identity stays by `id`; `clone()` returns a plain `Statement` without it.
    """

    __slots__ = ["fragment"]

    def __init__(self, *args: Any, fragment: str | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.fragment = fragment or ""

    @property
    def dedupe_key(self) -> str:
        """Sort / dedupe key of a storage row: `id`, `origin` and `fragment`."""
        return f"{self.id}\t{self.origin or DEFAULT_ORIGIN}\t{self.fragment}"

    @classmethod
    def from_statement(
        cls, stmt: Statement, fragment: str | None = None
    ) -> "LakeStatement":
        """Upgrade `stmt` to a `LakeStatement`, stamping `fragment` unless `None`.

        A plain statement is copied, a lake statement is stamped in place.
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


SECRETS_DIR = Path("/run/secrets")


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        secrets_dir=str(SECRETS_DIR) if SECRETS_DIR.is_dir() else None,
    )

    key: str | None = Field(default=None, alias="aws_access_key_id")
    secret: str | None = Field(default=None, alias="aws_secret_access_key")
    endpoint: str | None = Field(
        default=None,
        validation_alias=AliasChoices("aws_endpoint_url", "fsspec_s3_endpoint_url"),
    )
    region: str | None = Field(
        default=None,
        validation_alias=AliasChoices("aws_region", "aws_default_region"),
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


def setup_duckdb_storage(con: duckdb.DuckDBPyConnection | None = None) -> None:
    """Create the s3 secret for the configured credentials on `con` (or the default)."""
    settings = storage_settings
    if not settings.secret:
        return
    options = {
        "KEY_ID": settings.key,
        "SECRET": settings.secret,
        "REGION": settings.region,
    }
    if settings.duckdb_endpoint:
        options["ENDPOINT"] = settings.duckdb_endpoint
        options["URL_STYLE"] = "path"
        options["USE_SSL"] = str(not settings.allow_http)
    params = "".join(
        ", {} '{}'".format(k, v.replace("'", "''"))
        for k, v in options.items()
        if v is not None
    )
    (con or duckdb.default_connection()).execute(
        f"CREATE OR REPLACE SECRET secret (TYPE s3, PROVIDER config{params})"
    )


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


# `bucket` is a function of the schema, so a schema filter prunes it
PRUNE: dict[str, PruneFn] = {"bucket": prune_by_schema(get_schema_bucket)}


def pack_statement(stmt: Statement, source: str | None = None) -> SDict:
    """Pack statement to wire format for db but keep microseconds"""
    data = cast(SDict, stmt.to_dict())
    data["prop_type"] = stmt.prop_type
    data["first_seen"] = iso_datetime(data["first_seen"])
    data["last_seen"] = iso_datetime(data["last_seen"])
    data["bucket"] = get_schema_bucket(data["schema"])
    data["source"] = source
    data["origin"] = data["origin"] or DEFAULT_ORIGIN
    data["fragment"] = stmt.fragment if isinstance(stmt, LakeStatement) else ""
    return data


def statements_to_table(statements: Iterable[Statement]) -> pa.Table:
    """Pack statements into an `ARROW_SCHEMA` table columnwise, `source` left null."""
    ids: list[str | None] = []
    entity_ids: list[str | None] = []
    canonical_ids: list[str | None] = []
    datasets: list[str] = []
    buckets: list[str] = []
    origins: list[str] = []
    schemata: list[str] = []
    props: list[str] = []
    prop_types: list[str | None] = []
    values: list[str] = []
    original_values: list[str | None] = []
    langs: list[str | None] = []
    externals: list[bool] = []
    first_seens: list[datetime | None] = []
    last_seens: list[datetime | None] = []
    fragments: list[str] = []

    for stmt in statements:
        ids.append(stmt.id)
        entity_ids.append(stmt.entity_id)
        canonical_ids.append(stmt.canonical_id)
        datasets.append(stmt.dataset)
        buckets.append(get_schema_bucket(stmt.schema))
        origins.append(stmt.origin or DEFAULT_ORIGIN)
        schemata.append(stmt.schema)
        props.append(stmt.prop)
        prop_types.append(stmt.prop_type)
        values.append(stmt.value)
        original_values.append(stmt.original_value)
        langs.append(stmt.lang)
        externals.append(stmt.external)
        first_seens.append(iso_datetime(stmt.first_seen))
        last_seens.append(iso_datetime(stmt.last_seen))
        fragments.append(stmt.fragment if isinstance(stmt, LakeStatement) else "")

    return pa.table(
        {
            "id": ids,
            "entity_id": entity_ids,
            "canonical_id": canonical_ids,
            "dataset": datasets,
            "bucket": buckets,
            "origin": origins,
            "source": [None] * len(ids),
            "schema": schemata,
            "prop": props,
            "prop_type": prop_types,
            "value": values,
            "original_value": original_values,
            "lang": langs,
            "external": externals,
            "first_seen": first_seens,
            "last_seen": last_seens,
            "fragment": fragments,
        },
        schema=ARROW_SCHEMA,
    )


ViewSqlBuilder = Callable[[DeltaTable], str]
"""Builds the SELECT body of a view registered on the lake store's connection."""


def default_view_sql(dt: DeltaTable) -> str:
    """The default `statement` view: the raw delta rows."""
    # `delta_scan` can't bind its path, so it is quoted inline
    table_uri = dt.table_uri.replace("'", "''")
    return f"SELECT * FROM delta_scan('{table_uri}')"


class Row:
    """A sqlalchemy `Row` lookalike (attribute and index access) for duckdb rows."""

    def __init__(self, data: SDict) -> None:
        for key, value in data.items():
            setattr(self, key, value)

    def __iter__(self) -> Generator[Any, None, None]:
        yield from self.__dict__.values()

    def __getitem__(self, i: int) -> Any:
        return list(self.__iter__())[i]


class LakeStore(SQLStore):
    @property
    def source(self) -> SqlSource:
        """The lake statement view, with `bucket` pruning and the view filter."""
        return SqlSource(
            self.table,
            prune=PRUNE,
            base_filter=self._view_filter,
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
        # only feeds the unused sqlite engine and the resolver, queries use duckdb
        kwargs["uri"] = "sqlite:///:memory:"
        super().__init__(*args, **kwargs)
        self.table = TABLE
        self.uri = self._backend.uri

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
        """The shared duckdb connection (UTC, views registered); query via `cursor`."""
        config = {
            "autoinstall_known_extensions": "true",
            "autoload_known_extensions": "true",
            **self._duckdb_config,
        }
        con = duckdb.connect(":memory:", config=config)
        # icu ships bundled with the duckdb wheel, so this works offline
        con.execute("LOAD icu; SET GLOBAL TimeZone='UTC'")
        # before the views, which already read through `delta_scan`
        setup_duckdb_storage(con)
        dt = self.deltatable
        for name, builder in self._view_sqls.items():
            con.sql(f"CREATE OR REPLACE VIEW {name} AS {builder(dt)}")
        return con

    @contextmanager
    def cursor(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield a thread-isolated cursor on the shared duckdb connection."""
        cur = self._duckdb.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def _apply_filters(self, q: Select) -> Select:
        """Hook for subclasses to add WHERE clauses before every query (no-op)."""
        return q

    def _execute(self, q: Select, stream: bool = True) -> Generator[Any, None, None]:
        if not self.exists:
            return
        q = self._apply_filters(q)
        if self._view_filter is not None and isinstance(q, Select):
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

    def writer(
        self, origin: str | None = DEFAULT_ORIGIN, source: str | None = None
    ) -> "LakeWriter":
        writer = LakeWriter(self, origin=origin or DEFAULT_ORIGIN, source=source)
        return cast("LakeWriter", self.casting_writer(writer))

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
        key = f"{canonical_id}\t{dedupe}\t{stmt.origin}"
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
        # flush per entity, not per statement, to keep an entity in one file
        if len(self.batch) >= self.BATCH_STATEMENTS:
            self.flush()

    def _build_table(self) -> pa.Table:
        keys = sorted(self.batch)
        table = statements_to_table(self.batch[key][0] for key in keys)
        # `source` is per write, not statement content, so it's set as one column
        sources = [self.batch[key][1] for key in keys]
        return table.set_column(
            ARROW_SCHEMA.get_field_index("source"),
            "source",
            pa.array(sources, pa.string()),
        )

    def flush(self) -> None:
        if not self.batch:
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
        """Z-order and compact the storage, optionally per partition and vacuumed."""
        base_filters: FilterConjunctionType = []
        if dataset is not None:
            base_filters.append(("dataset", "=", dataset))
        if origin is not None:
            base_filters.append(("origin", "=", origin))

        with self.store._lock:
            for b in [bucket] if bucket is not None else ALL_BUCKETS:
                self.store.deltatable.optimize.z_order(
                    Z_ORDER,
                    writer_properties=writer_for_bucket(b),
                    target_size=TARGET_SIZE,
                    partition_filters=[*base_filters, ("bucket", "=", b)],
                )
            if vacuum:
                self.store.deltatable.vacuum(
                    retention_hours=vacuum_keep_hours,
                    enforce_retention_duration=False,
                    dry_run=False,
                    full=True,
                )
