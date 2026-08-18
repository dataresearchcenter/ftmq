import logging
import random
import time
from datetime import datetime

from normality import stringify
from sqlalchemy.dialects.postgresql import insert as postgresql_upsert
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
from sqlalchemy.exc import (
    DatabaseError,
    DisconnectionError,
    OperationalError,
    ResourceClosedError,
    TimeoutError,
)
from sqlalchemy.sql.expression import insert, update

# We have to cast null fragment values to some text to make the
# UniqueConstraint work
DEFAULT_FRAGMENT = "default"
# dialects with a native upsert, keyed by `dialect.name`. sqlite has
# `ON CONFLICT DO UPDATE` since 3.24, with the same interface as postgres
UPSERTS = {"postgresql": postgresql_upsert, "sqlite": sqlite_upsert}
EXCEPTIONS = (
    DatabaseError,
    DisconnectionError,
    OperationalError,
    ResourceClosedError,
    TimeoutError,
)
try:
    from psycopg import DatabaseError, OperationalError

    EXCEPTIONS = (DatabaseError, OperationalError, *EXCEPTIONS)
except ImportError:
    try:
        from psycopg2 import DatabaseError, OperationalError

        EXCEPTIONS = (DatabaseError, OperationalError, *EXCEPTIONS)
    except ImportError:
        pass

log = logging.getLogger(__name__)


class BulkLoader(object):
    def __init__(self, dataset, size):
        self.dataset = dataset
        self.store = dataset.store
        self.size = size
        self.buffer = {}
        self.upsert = UPSERTS.get(self.store.engine.dialect.name)

    def put(self, entity, fragment=None, origin=None):
        origin = origin or self.dataset.origin
        fragment = stringify(fragment) or DEFAULT_FRAGMENT
        if hasattr(entity, "to_dict"):
            entity = entity.to_dict()
        else:
            entity = dict(entity)
        id_ = entity.pop("id")
        if id_:
            self.buffer[(id_, origin, fragment)] = entity
            if len(self.buffer) >= self.size:
                self.flush()
        else:
            log.warning("Entity has no ID!")

    def _store_values(self, conn, values):
        """Insert-or-update one row at a time, for dialects without upsert.

        A conflict on (id, origin, fragment) must not abort the whole batch:
        a single already known row would otherwise swallow the new rows next
        to it in the same buffer.
        """
        table = self.dataset.table
        for value in values:
            stmt = update(table)
            stmt = stmt.values(entity=value["entity"], timestamp=value["timestamp"])
            stmt = stmt.where(table.c.id == value["id"])
            stmt = stmt.where(table.c.origin == value["origin"])
            stmt = stmt.where(table.c.fragment == value["fragment"])
            if not conn.execute(stmt).rowcount:
                conn.execute(insert(table).values(value))

    def _upsert_values(self, conn, values):
        """Use the dialect's upsert mechanism (ON CONFLICT DO UPDATE)."""
        istmt = self.upsert(self.dataset.table).values(values)
        stmt = istmt.on_conflict_do_update(
            index_elements=["id", "origin", "fragment"],
            set_=dict(
                entity=istmt.excluded.entity,
                timestamp=istmt.excluded.timestamp,
            ),
        )
        conn.execute(stmt)

    def flush(self):
        if not len(self.buffer):
            return
        values = []
        now = datetime.utcnow()
        for (id_, origin, fragment), entity in sorted(self.buffer.items()):
            values.append(
                {
                    "id": id_,
                    "origin": origin,
                    "fragment": fragment,
                    "timestamp": now,
                    "entity": entity,
                }
            )

        for attempt in range(10):
            conn = self.store.engine.connect()
            tx = conn.begin()
            try:
                if self.upsert is not None:
                    self._upsert_values(conn, values)
                else:
                    self._store_values(conn, values)
                tx.commit()
                conn.close()
                self.buffer = {}
                return
            except EXCEPTIONS:
                tx.rollback()
                conn.close()
                self.dataset.reset()
                self.store.engine.dispose()
                log.exception("Database error storing entities")
                time.sleep(attempt * random.random())
