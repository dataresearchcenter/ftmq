"""The `SQLStore` on a duckdb database file, addressed as `duckdb://<path>`.

Only the bulk upsert and the index DDL are duckdb-specific. The default resolver
is an ephemeral in-memory one (see `get_resolver`). An in-memory store is per
thread (each pooled connection is its own empty database): use a file for
anything threaded.
"""

from pathlib import Path
from typing import Any

import duckdb_engine  # noqa: F401  # registers the `duckdb` sqlalchemy dialect
from followthemoney import StatementEntity
from followthemoney.dataset.dataset import Dataset
from nomenklatura.store import sql as nk
from sqlalchemy import Index, Table, event
from sqlalchemy.dialects.postgresql import insert as duckdb_insert
from sqlalchemy.engine import Dialect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateIndex
from sqlalchemy.sql.compiler import DDLCompiler

from ftmq.store.base import Writer
from ftmq.store.sql import SQLStore

SCHEME = "duckdb://"
MEMORY = ":memory:"


@compiles(CreateIndex, "duckdb")
def _create_index_if_not_exists(
    create: CreateIndex, compiler: DDLCompiler, **kw: Any
) -> str:
    # `duckdb_engine` can't reflect indexes, so `checkfirst` never finds one
    create.if_not_exists = True
    return compiler.visit_create_index(create, **kw)  # type: ignore[no-any-return,no-untyped-call]


def _not_duckdb(*args: Any, dialect: Dialect, **kw: Any) -> bool:
    return dialect.name != "duckdb"


@event.listens_for(Index, "after_parent_attach")
def _skip_partial_index(index: Index, table: Table) -> None:
    # duckdb has no partial indexes, but its dialect renders the postgres `WHERE`
    if index.dialect_options["postgresql"]["where"] is not None:
        index.ddl_if(callable_=_not_duckdb)


def parse_uri(uri: str) -> str:
    """Normalize a `duckdb://<path>` store uri into a sqlalchemy url.

    The path follows the scheme directly (relative to the cwd, or absolute with
    any number of leading slashes); an empty path or `:memory:` is in-memory.
    """
    path = str(uri)
    if path.startswith(SCHEME):
        path = path[len(SCHEME) :]
    if not path or path == MEMORY:
        return f"{SCHEME}/{MEMORY}"
    if path.startswith("/"):
        path = "/" + path.lstrip("/")
    return f"{SCHEME}/{Path(path).absolute()}"


class DuckDBWriter(nk.SQLWriter[Dataset, StatementEntity]):
    """nomenklatura's SQL writer with a duckdb bulk upsert (the postgres grammar)."""

    def _upsert_batch(self) -> None:
        if not len(self.batch):
            return
        values = [s.to_db_row() for s in self.batch]
        if self.tx is None:
            self.tx = self.conn.begin()
        istmt = duckdb_insert(self.store.table).values(values)
        stmt = istmt.on_conflict_do_update(
            index_elements=["id"],
            set_=dict(
                canonical_id=istmt.excluded.canonical_id,
                schema=istmt.excluded.schema,
                prop_type=istmt.excluded.prop_type,
                lang=istmt.excluded.lang,
                original_value=istmt.excluded.original_value,
                last_seen=istmt.excluded.last_seen,
            ),
        )
        self.conn.execute(stmt)
        self.batch = set()


class DuckDBStore(SQLStore):
    """A statement store in a duckdb database file.

    Example:
        ```python
        from ftmq.store import get_store

        store = get_store("duckdb://./followthemoney.duckdb")
        ```
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["uri"] = parse_uri(str(kwargs.get("uri") or MEMORY))
        super().__init__(*args, **kwargs)

    def writer(self, *args: Any, **kwargs: Any) -> Writer:
        # not `super().writer()`: nomenklatura's `SQLWriter` can't upsert into duckdb
        return self.casting_writer(DuckDBWriter(self))
