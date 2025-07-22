from sqlalchemy import MetaData, create_engine
from sqlalchemy import inspect as sqlalchemy_inspect

from ftmq.store.fragments.dataset import Fragments
from ftmq.store.fragments.utils import NULL_ORIGIN


class Store(object):
    """A database containing multiple tables that represent
    FtM-store datasets."""

    PREFIX = "ftm"

    def __init__(
        self,
        database_uri: str,
        **config,
    ):
        self.database_uri = database_uri
        # config.setdefault('pool_size', 1)
        self.engine = create_engine(database_uri, future=True, **config)
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
