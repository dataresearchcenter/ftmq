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
        bucket=[bucket]/  # things, intervals, documents
            origin=[origin]/
                [uid].parquet
    ```
"""

from pathlib import Path
from typing import Any, Generator, Iterable
from urllib.parse import urlparse

import duckdb
import numpy as np
import pandas as pd
from anystore.functools import weakref_cache as cache
from anystore.lock import Lock
from anystore.logging import get_logger
from anystore.store.fs import Store as FSStore
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
from followthemoney import EntityProxy, StatementEntity, model
from followthemoney.dataset.dataset import Dataset
from followthemoney.statement import Statement
from nomenklatura import settings as nks
from nomenklatura import store as nk
from nomenklatura.db import get_metadata
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Boolean, DateTime, column, select, table
from sqlalchemy.sql import Select

from ftmq.query import Query
from ftmq.store.base import Store
from ftmq.store.sql import SQLQueryView, SQLStore
from ftmq.types import StatementEntities
from ftmq.util import apply_dataset, ensure_entity, get_scope_dataset

log = get_logger(__name__)

Z_ORDER = ["canonical_id", "entity_id", "schema", "prop"]
TARGET_SIZE = 50 * 10_485_760  # 500 MB
PARTITION_BY = ["dataset", "bucket", "origin"]
DEFAULT_ORIGIN = "default"
BUCKET_DOCUMENT = "document"
BUCKET_INTERVAL = "interval"
BUCKET_THING = "thing"
STATISTICS_BLOOM = ColumnProperties(
    bloom_filter_properties=BloomFilterProperties(True),
    statistics_enabled="CHUNK",
    dictionary_enabled=True,
)
STATISTICS = ColumnProperties(statistics_enabled="CHUNK", dictionary_enabled=True)
WRITER = WriterProperties(
    data_page_size_limit=64 * 1024,
    dictionary_page_size_limit=512 * 1024,
    max_row_group_size=500_000,
    compression="SNAPPY",
    column_properties={
        "canonical_id": STATISTICS,
        "entity_id": STATISTICS,
        "schema": STATISTICS,
        "prop": STATISTICS_BLOOM,
        "value": STATISTICS_BLOOM,
    },
)

TABLE = table(
    nks.STATEMENT_TABLE,
    column("id"),
    column("entity_id"),
    column("canonical_id"),
    column("dataset"),
    column("bucket"),
    column("origin"),
    column("schema"),
    column("prop"),
    column("prop_type"),
    column("value"),
    column("original_value"),
    column("lang"),
    column("external", Boolean),
    column("first_seen", DateTime),
    column("last_seen", DateTime),
)


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

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
        duckdb.query(
            f"""CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER config,
            KEY_ID '{storage_settings.key}',
            SECRET '{storage_settings.secret}',
            ENDPOINT '{storage_settings.endpoint}',
            URL_STYLE 'path',
            USE_SSL '{not storage_settings.allow_http}'
            );"""
        )


@cache
def get_schema_bucket(schema_name: str) -> str:
    s = model[schema_name]
    if s.is_a("Document"):
        return BUCKET_DOCUMENT
    if s.is_a("Interval"):
        return BUCKET_INTERVAL
    return BUCKET_THING


def pack_statement(stmt: Statement) -> SDict:
    data = stmt.to_db_row()
    data["bucket"] = get_schema_bucket(data["schema"])
    return data


def pack_statements(statements: Iterable[Statement]) -> pd.DataFrame:
    df = pd.DataFrame(map(pack_statement, statements))
    df = df.drop_duplicates()  # .sort_values(Z_ORDER)
    df = df.fillna(np.nan)
    return df


def compile_query(q: Select) -> str:
    table = nks.STATEMENT_TABLE
    sql = str(q.compile(compile_kwargs={"literal_binds": True}))
    return sql.replace(f"FROM {table}", f"FROM arrow as {table}")


class Row:
    """Fake sqlalchemy row-like class"""

    def __init__(self, data: SDict) -> None:
        for key, value in data.items():
            setattr(self, key, value)

    def __iter__(self) -> Generator[Any, None, None]:
        yield from self.__dict__.values()

    def __getitem__(self, i: int) -> Any:
        return list(self.__iter__())[i]


def query_duckdb(q: Select, table: DeltaTable) -> duckdb.DuckDBPyRelation:
    rel = duckdb.arrow(table.to_pyarrow_dataset())
    query = compile_query(q)
    return rel.query("arrow", query)


def stream_duckdb(q: Select, table: DeltaTable) -> Generator[Any, None, None]:
    res = query_duckdb(q, table)
    while rows := res.fetchmany(100_000):
        for row in rows:
            yield Row(dict(zip(res.columns, row)))


def ensure_schema_buckets(q: Query) -> Select:
    if not q.schemata_names:
        return q.sql.statements
    buckets: set[str] = set()
    for schema in q.schemata_names:
        buckets.add(get_schema_bucket(schema))
    return q.sql.statements.where(TABLE.c.bucket.in_(buckets))


class LakeQueryView(SQLQueryView):
    def query(self, query: Query | None = None) -> StatementEntities:
        if query:
            query.table = self.store.table
            query = self.ensure_scoped_query(query)
            sql = ensure_schema_buckets(query)
            yield from self.store._iterate(sql)
        else:
            yield from super().query(query)


class LakeStore(SQLStore):
    def __init__(self, *args, **kwargs) -> None:
        self._backend: FSStore = FSStore(uri=kwargs.pop("uri"))
        self._partition_by = kwargs.pop("partition_by", PARTITION_BY)
        self._lock: Lock = kwargs.pop("lock", Lock(self._backend))
        self._enforce_dataset = kwargs.pop("enforce_dataset", False)
        assert isinstance(
            self._backend, FSStore
        ), f"Invalid store backend: `{self._backend.__class__}"
        kwargs["uri"] = "sqlite:///:memory:"  # fake it till you make it
        get_metadata.cache_clear()
        super().__init__(*args, **kwargs)
        self.table = TABLE
        self.uri = self._backend.uri
        setup_duckdb_storage()

    @property
    def deltatable(self) -> DeltaTable:
        return DeltaTable(self.uri, storage_options=storage_options())

    def _execute(self, q: Select, stream: bool = True) -> Generator[Any, None, None]:
        try:
            yield from stream_duckdb(q, self.deltatable)
        except TableNotFoundError:
            pass

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
    ) -> SQLQueryView:
        scope = scope or self.dataset
        return LakeQueryView(self, scope, external)

    def writer(self, origin: str | None = DEFAULT_ORIGIN) -> "LakeWriter":
        return LakeWriter(self, origin=origin or DEFAULT_ORIGIN)

    def get_origins(self) -> set[str]:
        q = select(self.table.c.origin).distinct()
        return set([r.origin for r in stream_duckdb(q, self.deltatable)])


class LakeWriter(nk.Writer):
    store: LakeStore
    BATCH_STATEMENTS = 1_000_000

    def __init__(self, store: Store, origin: str | None = DEFAULT_ORIGIN):
        super().__init__(store)
        self.batch: set[Statement] = set()
        self.origin = origin or DEFAULT_ORIGIN

    def add_statement(self, stmt: Statement) -> None:
        if stmt.entity_id is None:
            return
        stmt.origin = stmt.origin or self.origin
        canonical_id = self.store.linker.get_canonical(stmt.entity_id)
        stmt.canonical_id = canonical_id
        self.batch.add(stmt)

    def add_entity(self, entity: EntityProxy, origin: str | None = None) -> None:
        e = ensure_entity(entity, StatementEntity, self.store.dataset)
        if self.store._enforce_dataset:
            e = apply_dataset(e, self.store.dataset, replace=True)
        for stmt in e.statements:
            if origin:
                stmt.origin = origin
            self.add_statement(stmt)
        # we check here instead of in `add_statement` as this will keep entities
        # together in the same parquet files`
        if len(self.batch) >= self.BATCH_STATEMENTS:
            self.flush()

    def flush(self) -> None:
        if self.batch:
            log.info(
                f"Write {len(self.batch)} statements to deltalake ...",
                uri=self.store.uri,
            )
            with self.store._lock:
                write_deltalake(
                    str(self.store.uri),
                    pack_statements(self.batch),
                    partition_by=self.store._partition_by,
                    mode="append",
                    schema_mode="merge",
                    writer_properties=WRITER,
                    target_file_size=TARGET_SIZE,
                    storage_options=storage_options(),
                )

        self.batch = set()

    def pop(self, entity_id: str) -> list[Statement]:
        q = select(TABLE)
        q = q.where(TABLE.c.canonical_id == entity_id)
        statements: list[Statement] = []
        for row in self.store._execute(q):
            statements.append(Statement.from_db_row(row))

        self.store.deltatable.delete(f"canonical_id = '{entity_id}'")
        return statements

    def optimize(
        self, vacuum: bool | None = False, vacuum_keep_hours: int | None = 0
    ) -> None:
        """
        Optimize the storage: Z-Ordering and compacting
        """
        self.store.deltatable.optimize.z_order(
            Z_ORDER, writer_properties=WRITER, target_size=TARGET_SIZE
        )
        if vacuum:
            self.store.deltatable.vacuum(
                retention_hours=vacuum_keep_hours,
                enforce_retention_duration=False,
                dry_run=False,
                full=True,
            )
