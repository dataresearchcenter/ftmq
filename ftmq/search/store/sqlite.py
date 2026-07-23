"""
SQlite FTS5
"""

from functools import cache
from typing import Any, Iterable

from anystore.logging import get_logger
from normality import normalize
from pydantic import ConfigDict
from sqlalchemy import Column, MetaData, Table, Text, Unicode, insert, or_, select, text
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.exc import OperationalError

from ftmq.query import Query
from ftmq.search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq.search.settings import Settings
from ftmq.search.store.base import BaseStore

settings = Settings()

log = get_logger(__name__)

KEY_LEN = 512
VALUE_LEN = 65535


@cache
def get_metadata() -> MetaData:
    return MetaData()


@cache
def make_table(name: str = settings.sql_table_name) -> Table:
    metadata = get_metadata()
    return Table(
        name,
        metadata,
        Column("id", Unicode(KEY_LEN), primary_key=True, unique=True),
        Column("datasets", Unicode(VALUE_LEN), index=True),
        Column("schema", Unicode(KEY_LEN), index=True, nullable=False),
        Column("countries", Unicode(KEY_LEN), index=True, nullable=False),
        Column("caption", Unicode(VALUE_LEN), index=True, nullable=False),
        Column("names", Unicode(VALUE_LEN), index=True, nullable=False),
    )


@cache
def make_names_table(name: str = settings.sql_table_name) -> Table:
    metadata = get_metadata()
    return Table(
        f"{name}_names",
        metadata,
        Column("id", Unicode(KEY_LEN), nullable=False),
        Column("name", Unicode(VALUE_LEN), index=True, nullable=False),
    )


@cache
def make_fts_table(name: str = settings.sql_table_name) -> Table:
    metadata = get_metadata()
    return Table(
        f"{name}_fts",
        metadata,
        Column("id", Unicode(KEY_LEN), nullable=False),
        Column("text", Text(VALUE_LEN), nullable=False),
    )


def to_array(values: list[str]) -> str:
    if not values:
        return ""
    return f"#{'#'.join(values)}#"


def from_array(value: str) -> list[str]:
    return value.strip("#").split("#")


class SQliteStore(BaseStore):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    table_name: str = settings.sql_table_name
    table: Table
    names_table: Table
    fts_table: Table
    engine: Engine

    buffer: list[tuple[str, str, str, str, str, str]] = []
    fts_buffer: list[tuple[str, str]] = []
    names_buffer: list[tuple[str, str]] = []

    def __init__(self, **data: Any) -> None:
        uri = data.get("uri", settings.uri)
        table_name = data.get("table_name", settings.sql_table_name)
        data["engine"] = create_engine(uri)
        data["table"] = make_table(table_name)
        data["names_table"] = make_names_table(table_name)
        data["fts_table"] = make_fts_table(table_name)
        super().__init__(**data)
        self.create()

    def create(self) -> None:
        metadata = get_metadata()
        metadata.create_all(
            self.engine, tables=[self.table, self.names_table], checkfirst=True
        )
        with self.engine.connect() as conn:
            try:
                conn.execute(
                    text(
                        f"CREATE VIRTUAL TABLE {self.table_name}_fts USING "
                        "fts5(id UNINDEXED, text)"
                    )
                )
            except OperationalError as e:
                if "already exists" in str(e):
                    return
                raise e

    def flush(self) -> None:
        conn = self.engine.connect()
        tx = conn.begin()
        if self.buffer:
            conn.execute(insert(self.table).values(self.buffer))
        if self.names_buffer:
            conn.execute(insert(self.names_table).values(self.names_buffer))
        if self.fts_buffer:
            conn.execute(insert(self.fts_table).values(self.fts_buffer))
        tx.commit()
        self.buffer = []
        self.fts_buffer = []
        self.names_buffer = []

    def put(self, doc: EntityDocument) -> None:
        self.buffer.append(
            (
                doc.id,
                to_array(doc.datasets),
                doc.schema_,
                to_array(doc.countries),
                doc.caption,
                to_array(doc.names),
            )
        )
        self.fts_buffer.append((doc.id, doc.text))
        for name in doc.names:
            self.names_buffer.append((doc.id, name))
        if len(self.buffer) == 10_000:
            self.flush()

    def search(
        self, q: str, query: Query | None = None
    ) -> Iterable[EntitySearchResult]:
        q = normalize(q, lowercase=False) or ""
        stmt = (
            select(text("rank"), self.table)
            .join(self.table, self.table.c.id == self.fts_table.c.id)
            .where(self.fts_table.c.text.match(q))
            .order_by(text("rank"))
            .limit(100)
        )
        if query is not None:
            if query.dataset_names:
                stmt = stmt.where(
                    or_(
                        *(
                            self.table.c.datasets.like(f"%#{d}#%")
                            for d in query.dataset_names
                        )
                    )
                )
            if query.schemata_names:
                stmt = stmt.where(self.table.c.schema.in_(query.schemata_names))
            if query.countries:
                stmt = stmt.where(
                    or_(
                        *(
                            self.table.c.countries.like(f"%#{c}#%")
                            for c in query.countries
                        )
                    )
                )
        with self.engine.connect() as conn:
            for res in conn.execute(stmt):
                data = dict(res._mapping)
                score = data.pop("rank")
                data["datasets"] = from_array(data["datasets"])
                data["names"] = from_array(data["names"])
                data["countries"] = from_array(data["countries"])
                yield EntitySearchResult(score=score * -1, **data)

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        q = normalize(q, lowercase=False) or ""
        stmt = (
            select(self.names_table)
            .where(self.names_table.c.name.ilike(f"{q}%"))
            .limit(100)
        )
        with self.engine.connect() as conn:
            for id_, name in conn.execute(stmt):
                yield AutocompleteResult(id=id_, name=name)
