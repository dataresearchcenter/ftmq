from sqlalchemy import MetaData, create_engine
from sqlalchemy import inspect as sqlalchemy_inspect

from ftmq.store.fragments.dataset import Fragments
from ftmq.store.fragments.utils import NULL_ORIGIN


class Store(object):
    """A database containing multiple tables that represent
    FtM-store datasets."""

    PREFIX = "ftm"

    def _adjust_psycopg3_uri(self, database_uri: str) -> str:
        """Adjust PostgreSQL URI to use psycopg3 dialect if psycopg is available."""
        if database_uri.startswith(("postgresql://", "postgres://")):
            try:
                import psycopg  # noqa: F401

                # Use psycopg3 dialect for better performance and compatibility
                if database_uri.startswith("postgresql://"):
                    return database_uri.replace(
                        "postgresql://", "postgresql+psycopg://", 1
                    )
                elif database_uri.startswith("postgres://"):
                    return database_uri.replace(
                        "postgres://", "postgresql+psycopg://", 1
                    )
            except ImportError:
                # Fall back to psycopg2 if psycopg3 is not available
                pass
        return database_uri

    def __init__(
        self,
        database_uri: str,
        **config,
    ):
        self.database_uri = self._adjust_psycopg3_uri(database_uri)

        # Configure connection pooling for psycopg3
        if self.database_uri.startswith("postgresql+psycopg://"):
            config.setdefault("pool_size", 10)
            config.setdefault("max_overflow", 20)
            config.setdefault("pool_timeout", 30)
            config.setdefault("pool_recycle", 3600)
            config.setdefault("pool_pre_ping", True)
        else:
            config.setdefault("pool_size", 5)

        self.engine = create_engine(self.database_uri, future=True, **config)
        self.is_postgres = self.engine.dialect.name == "postgresql"
        self.meta = MetaData()

    def get(self, name, origin=NULL_ORIGIN):
        return Fragments(self, name, origin=origin)

    def all(self, origin=NULL_ORIGIN):
        prefix = f"{self.PREFIX}_"
        inspect = sqlalchemy_inspect(self.engine)
        for table in inspect.get_table_names():
            if table.startswith(prefix):
                name = table[len(prefix) :]
                yield Fragments(self, name, origin=origin)

    def close(self):
        self.engine.dispose()

    def __len__(self):
        return len(list(self.all()))

    def __repr__(self):
        return "<Store(%r)>" % self.engine
