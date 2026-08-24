"""A [`SQLStore`][ftmq.store.sql.SQLStore] variant backed by a
[duckdb](https://duckdb.org) database file, addressed as `duckdb://<path>`.

Everything the SQL store does compiles unchanged: duckdb is reached through the
`duckdb_engine` sqlalchemy dialect, so the statement table, the query
compilation ([`ftmq.query.sql`][ftmq.query.sql]) and the store views are the
ones from [`ftmq.store.sql`][ftmq.store.sql]. Only the two spots where
nomenklatura hardcodes the sqlite / postgres dialects need a duckdb spelling:
the bulk upsert (see [`DuckDBWriter`][ftmq.store.duckdb.DuckDBWriter]) and the
resolver, which stays on its own sqlite database (see
[`get_resolver`][ftmq.store.base.get_resolver]).

Unlike the [lake store][ftmq.store.lake.LakeStore] - which also queries through
duckdb, but over parquet files - this is a single writable database file with
the same read/write semantics as the sqlite backend.

Threading: a duckdb *file* store is shared across threads (one pooled
connection each, all reaching the same database). An in-memory store
(`duckdb://`) is not: a duckdb connection is not thread-safe, so `duckdb_engine`
pools in-memory connections per thread - and each of those is its own empty
database. Use a file for anything threaded (a threaded server, the api).
"""

from pathlib import Path
from typing import Any

import duckdb_engine  # noqa: F401  # registers the `duckdb` sqlalchemy dialect
from followthemoney import StatementEntity
from followthemoney.dataset.dataset import Dataset
from nomenklatura.store import sql as nk
from sqlalchemy.dialects.postgresql import insert as duckdb_insert

from ftmq.store.base import Writer
from ftmq.store.sql import SQLStore

SCHEME = "duckdb://"
MEMORY = ":memory:"


def parse_uri(uri: str) -> str:
    """Normalize a `duckdb://<path>` uri into a sqlalchemy url.

    The store uri spells the path directly after the scheme, so `duckdb://`,
    `duckdb:///` and `duckdb:////` in front of an absolute path all address the
    same file (sqlalchemy's own url grammar would read the first slashes as
    host / root). A relative path is resolved against the current directory,
    an empty path (or `:memory:`) gives an in-memory database.

    Args:
        uri: The store uri, e.g. `duckdb://./data.duckdb`

    Returns:
        A sqlalchemy url for the `duckdb` dialect.
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
    """nomenklatura's SQL writer with a duckdb bulk upsert.

    `nk.SQLWriter._upsert_batch` knows the sqlite and postgres spellings of
    `INSERT ... ON CONFLICT DO UPDATE` and raises `NotImplementedError` for
    anything else. duckdb speaks the postgres grammar, so the postgres insert
    construct compiles against it unchanged.
    """

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
        # not `super().writer()`: that is nomenklatura's `SQLWriter`, which
        # can't upsert into duckdb. Casting is applied here instead.
        return self.casting_writer(DuckDBWriter(self))
