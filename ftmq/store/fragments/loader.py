import logging
import random
import time
from datetime import datetime

from normality import stringify
from sqlalchemy.dialects.postgresql import insert as upsert
from sqlalchemy.exc import (
    DatabaseError,
    DisconnectionError,
    IntegrityError,
    OperationalError,
    ResourceClosedError,
    TimeoutError,
)
from sqlalchemy.sql.expression import insert, update

# We have to cast null fragment values to some text to make the
# UniqueConstraint work
DEFAULT_FRAGMENT = "default"
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
        table = self.dataset.table
        try:
            conn.execute(insert(table).values(values))
        except IntegrityError:
            changing = (
                "entity",
                "timestamp",
            )
            for value in values:
                stmt = update(table)
                changed = {c: value.get(c, {}) for c in changing}
                stmt = stmt.values(changed)
                stmt = stmt.where(table.c.id == value["id"])
                stmt = stmt.where(table.c.origin == value["origin"])
                stmt = stmt.where(table.c.fragment == value["fragment"])
                stmt = stmt.where(table.c.timestamp < value["timestamp"])
                conn.execute(stmt)

    def _upsert_values(self, conn, values):
        """Use postgres' upsert mechanism (ON CONFLICT TO UPDATE)."""
        istmt = upsert(self.dataset.table).values(values)
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
                if self.store.is_postgres:
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
