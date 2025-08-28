import logging
from datetime import datetime
from typing import Generator, Iterable, TypeAlias

from banal import ensure_list
from followthemoney import EntityProxy, StatementEntity
from followthemoney.dataset.util import dataset_name_check
from normality import slugify
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    String,
    Table,
    UniqueConstraint,
    distinct,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import OperationalError

from ftmq.store.fragments.loader import BulkLoader
from ftmq.store.fragments.utils import NULL_ORIGIN
from ftmq.types import Statements
from ftmq.util import make_dataset

log = logging.getLogger(__name__)
UNDEFINED = (OperationalError,)
try:
    from psycopg.errors import UndefinedTable

    UNDEFINED = (UndefinedTable, *UNDEFINED)
except ImportError:
    try:
        from psycopg2.errors import UndefinedTable

        UNDEFINED = (UndefinedTable, *UNDEFINED)
    except ImportError:
        pass


EntityFragments: TypeAlias = Generator[EntityProxy, None, None]


class Fragments(object):
    def __init__(self, store, name, origin=NULL_ORIGIN):
        self.store = store
        self.name = dataset_name_check(name)
        self.origin = origin
        self._table = None

    @property
    def table(self):
        if self._table is not None:
            return self._table
        table_name = slugify("%s %s" % (self.store.PREFIX, self.name), sep="_")
        if not table_name:
            raise RuntimeError(f"Invalid table name: `{table_name}`")
        json_type = JSONB if self.store.is_postgres else JSON
        self._table = Table(
            table_name,
            self.store.meta,
            Column("id", String, nullable=False),
            Column("origin", String, nullable=False),
            Column("fragment", String, nullable=False),
            Column("timestamp", DateTime, default=datetime.utcnow),
            Column("entity", json_type),
            UniqueConstraint("id", "origin", "fragment"),
            extend_existing=True,
        )
        self._table.create(bind=self.store.engine, checkfirst=True)
        return self._table

    def reset(self):
        self._table = None

    def drop(self):
        log.debug("Dropping ftm-store: %s", self.table)
        try:
            self.table.drop(self.store.engine)
            self.reset()
        except UNDEFINED:
            self.reset()
            raise

    def delete(self, entity_id=None, fragment=None, origin=None):
        table = self.table
        stmt = table.delete()
        if entity_id is not None:
            stmt = stmt.where(table.c.id == entity_id)
        if fragment is not None:
            stmt = stmt.where(table.c.fragment == fragment)
        if origin is not None:
            stmt = stmt.where(table.c.origin == origin)
        try:
            with self.store.engine.connect() as conn:
                conn.execute(stmt)
                conn.commit()
        except UNDEFINED:
            self.reset()
            raise

    def put(self, entity, fragment=None, origin=None):
        bulk = self.bulk()
        bulk.put(entity, fragment=fragment, origin=origin)
        return bulk.flush()

    def bulk(self, size=1000):
        return BulkLoader(self, size)

    def fragments(self, entity_ids=None, fragment=None):
        stmt = self.table.select()
        entity_ids = ensure_list(entity_ids)
        if len(entity_ids) == 1:
            stmt = stmt.where(self.table.c.id == entity_ids[0])
        if len(entity_ids) > 1:
            stmt = stmt.where(self.table.c.id.in_(entity_ids))
        if fragment is not None:
            stmt = stmt.where(self.table.c.fragment == fragment)
        stmt = stmt.order_by(self.table.c.id)
        # stmt = stmt.order_by(self.table.c.origin)
        # stmt = stmt.order_by(self.table.c.fragment)
        conn = self.store.engine.connect()
        try:
            conn = conn.execution_options(stream_results=True)
            for ent in conn.execute(stmt):
                data = {"id": ent.id, "datasets": [self.name], **ent.entity}
                if ent.origin != NULL_ORIGIN:
                    data["origin"] = ent.origin
                yield data
        except Exception:
            self.reset()
            raise
        finally:
            conn.close()

    def partials(self, entity_id=None, skip_errors=False) -> EntityFragments:
        for fragment in self.fragments(entity_ids=entity_id):
            try:
                yield EntityProxy.from_dict(fragment, cleaned=True)
            except Exception:
                if skip_errors:
                    log.exception("Invalid data [%s]: %s", self.name, fragment["id"])
                    continue
                raise

    def iterate(self, entity_id=None, skip_errors=False) -> EntityFragments:
        entity = None
        invalid = None
        fragments = 1
        for partial in self.partials(entity_id=entity_id, skip_errors=skip_errors):
            if partial.id == invalid:
                continue
            if entity is not None:
                if entity.id == partial.id:
                    fragments += 1
                    if fragments % 10000 == 0:
                        log.debug(
                            "[%s:%s] aggregated %d fragments...",
                            entity.schema.name,
                            entity.id,
                            fragments,
                        )
                    try:
                        entity.merge(partial)
                    except Exception:
                        if skip_errors:
                            log.exception(
                                "Invalid merge [%s]: %s", self.name, entity.id
                            )
                            invalid = entity.id
                            entity = None
                            fragments = 1
                            continue
                        raise
                    continue
                yield entity
            entity = partial
            fragments = 1
        if entity is not None:
            yield entity

    def statements(
        self,
        entity_ids: Iterable[str] | None = None,
        origin: str | None = None,
        since: datetime | None = None,
    ) -> Statements:
        """Iterate unsorted statements with its fragment origins"""
        stmt = self.table.select()
        entity_ids = ensure_list(entity_ids)
        if len(entity_ids) == 1:
            stmt = stmt.where(self.table.c.id == entity_ids[0])
        if len(entity_ids) > 1:
            stmt = stmt.where(self.table.c.id.in_(entity_ids))
        if origin is not None:
            stmt = stmt.where(self.table.c.origin == origin)
        if since is not None:
            stmt = stmt.where(self.table.c.timestamp > since)
        conn = self.store.engine.connect()
        default_dataset = make_dataset(self.name)
        try:
            conn = conn.execution_options(stream_results=True)
            for fragment in conn.execute(stmt):
                data = {"id": fragment.id, "datasets": [self.name], **fragment.entity}
                entity = StatementEntity.from_dict(
                    data, default_dataset=default_dataset
                )
                for statement in entity.statements:
                    statement.last_seen = fragment.timestamp.isoformat()
                    statement.origin = (
                        fragment.origin if fragment.origin != NULL_ORIGIN else None
                    )
                    yield statement
        except Exception:
            self.reset()
            raise
        finally:
            conn.close()

    def get(self, entity_id) -> EntityProxy | None:
        for entity in self.iterate(entity_id=entity_id):
            return entity

    def __iter__(self) -> EntityFragments:
        return self.iterate()

    def __len__(self):
        q = select(func.count(distinct(self.table.c.id)))
        with self.store.engine.connect() as conn:
            res = conn.execute(q)

        return res.scalar()

    def __repr__(self):
        return "<Dataset(%r, %r)>" % (self.store, self.name)
