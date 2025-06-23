"""
https://openaleph.org/docs/lib/ftm-datalake/rfc/#basic-layout

A file-like "datalake" statement store based on parquet files and
[deltalake](https://delta.io)

Backend has to be local filesystem, s3 or anything else compatible with
`deltalake`

Layout:
    ```
    ./[dataset]/
        entities/
            statements/
                _delta_log/
                    [ix].json
                origin=[origin]/
                    [uuid].parquet
    ```
"""

# from collections import defaultdict

# from functools import cache
from pathlib import Path
from typing import Any, Generator, Iterable

import duckdb

# from delta import DeltaTable, configure_spark_with_delta_pip
# from pyspark.sql.session import SparkSession
import numpy as np
import pandas as pd
from anystore.logging import get_logger
from anystore.store.fs import Store as FSStore
from anystore.types import SDict
from anystore.util import join_uri
from deltalake import write_deltalake
from nomenklatura import settings as nks
from nomenklatura import store as nk
from nomenklatura.db import get_metadata
from nomenklatura.entity import CompositeEntity
from nomenklatura.statement import Statement
from sqlalchemy.sql import Select

from ftmq.model import Catalog
from ftmq.store.base import Store
from ftmq.store.sql import SQLStore
from ftmq.types import CEGenerator, SGenerator

DEFAULT_ORIGIN = "default"

log = get_logger(__name__)


TABLE = (
    # column, type, nullable
    ("id", "STRING", False),
    ("entity_id", "STRING", False),
    ("canonical_id", "STRING", False),
    ("schema", "STRING", False),
    ("prop", "STRING", False),
    ("value", "STRING", False),
    ("original_value", "STRING", True),
    ("lang", "STRING", True),
    ("dataset", "STRING", False),
    ("origin", "STRING", False),
    ("first_seen", "TIMESTAMP", True),
    ("last_seen", "TIMESTAMP", True),
)


# @cache
# def make_spark() -> SparkSession:
#     builder = (
#         SparkSession.builder.appName("ftm-lakehouse")
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config(
#             "spark.sql.catalog.spark_catalog",
#             "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#         )
#     )
#     return configure_spark_with_delta_pip(builder).getOrCreate()


# @cache
# def make_table(uri: Uri, dataset: str) -> DeltaTable:
#     spark = make_spark()
#     table = DeltaTable.createIfNotExists(spark).location(str(uri)).tableName(dataset)
#     for c, t, n in TABLE:
#         table = table.addColumn(c, t, nullable=n)
#     table = table.partitionedBy("origin")
#     table = table.clusterBy("schema", "prop", "canonical_id")
#     table.execute()
#     return DeltaTable.forName(spark, dataset)


def multi_streams_sort(*streams: SGenerator) -> SGenerator:
    """combine sort statements from different sorted streams"""
    # statements = defaultdict(set[Statement])
    # for ix, stream in enumerate(streams):
    #     statement = next(stream)
    #     statements[ix].add(next(stream))
    #     pass
    for stream in streams:
        yield from stream


def pack_statement(stmt: Statement, origin: str) -> SDict:
    data = stmt.to_db_row()
    data["origin"] = origin
    return data


def pack_statements(
    statements: Iterable[Statement], origin: str
) -> Generator[tuple[str, pd.DataFrame], None, None]:
    df = pd.DataFrame(pack_statement(s, origin) for s in statements)
    df = df.drop_duplicates().sort_values("canonical_id")
    df = df.fillna(np.nan)
    for dataset in df["dataset"].unique():
        yield dataset, df[df["dataset"] == dataset]


def make_string_query(q: Select, uri: str) -> str:
    table = nks.STATEMENT_TABLE
    sql = str(q.compile(compile_kwargs={"literal_binds": True}))
    return sql.replace(f"FROM {table}", f"FROM delta_scan('{uri}') as {table}")


class Row:
    def __init__(self, data: SDict) -> None:
        for key, value in data.items():
            setattr(self, key, value)

    def __iter__(self) -> Generator[Any, None, None]:
        yield from self.__dict__.values()

    def __getitem__(self, i: int) -> Any:
        return list(self.__iter__())[i]


def stream_duckdb(q: Select, uri: str) -> Generator[Any, None, None]:
    query = make_string_query(q, uri)
    res = duckdb.query(query)
    while rows := res.fetchmany(10_000):
        for row in rows:
            yield Row(dict(zip(res.columns, row)))


class LakeStore(SQLStore):
    def __init__(self, *args, **kwargs) -> None:
        self._backend: FSStore = FSStore(uri=kwargs.pop("uri"))
        assert isinstance(
            self._backend, FSStore
        ), f"Invalid store backend: `{self._backend.__class__}"
        kwargs["uri"] = "sqlite:///:memory:"
        get_metadata.cache_clear()
        super().__init__(*args, **kwargs)
        self.uri = self._backend.uri

    def _execute(self, q: Select, stream: bool = True) -> Generator[Any, None, None]:
        streams: list[CEGenerator] = []
        for dataset in self.dataset.dataset_names:
            if dataset != "catalog":
                streams.append(stream_duckdb(q, join_uri(self.uri, dataset)))
        yield from multi_streams_sort(*streams)

    def get_catalog(self) -> Catalog:
        names: set[str] = set()
        for child in self._backend._fs.ls(self._backend.uri):
            names.add(Path(child).name)
        return Catalog.from_names(names)

    # def ensure_table(self, dataset: str) -> None:
    #     make_table(self.uri, dataset)

    def writer(self) -> "LakeWriter":
        return LakeWriter(self)


class LakeWriter(nk.Writer):
    store: LakeStore
    BATCH_STATEMENTS = 1_000_000

    def __init__(self, store: Store, origin: str | None = DEFAULT_ORIGIN):
        super().__init__(store)
        self.batch: set[Statement] = set()
        self.origin = origin or DEFAULT_ORIGIN

    def add_statement(self, stmt: Statement) -> None:
        self.batch.add(stmt)

    def add_entity(self, entity: CompositeEntity) -> None:
        super().add_entity(entity)
        if len(self.batch) >= self.BATCH_STATEMENTS:
            self.flush()

    def flush(self) -> None:
        if self.batch:
            log.info(
                f"Write {len(self.batch)} statements to deltalake ...",
                uri=self.store.uri,
            )
            for dataset, df in pack_statements(self.batch, self.origin):
                # self.store.ensure_table(dataset)
                ds_uri = join_uri(self.store.uri, dataset)
                log.info(
                    f"Write {len(df)} statements to dataset partition ...",
                    uri=ds_uri,
                    dataset=dataset,
                )
                write_deltalake(ds_uri, df, partition_by=["origin"], mode="append")

        self.batch = set()
