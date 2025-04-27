from functools import cache

from anystore import smart_stream
from nomenklatura.db import get_engine
from nomenklatura.entity import CompositeEntity
from nomenklatura.resolver import Edge, Resolver
from sqlalchemy.engine import Engine

from ftmq.logging import get_logger

log = get_logger(__name__)


@cache
def get_resolver(
    uri: str | None = None, engine: Engine | None = None
) -> Resolver[CompositeEntity]:
    resolver = Resolver.make_default(engine=engine or get_engine())
    if not uri:
        return resolver
    resolver.begin()
    for ix, edge in enumerate(smart_stream(uri)):
        edge = Edge.from_line(edge)
        resolver._register(edge)
        if ix and ix % 10_000 == 0:
            log.info("Loading edge %d ..." % ix)
    resolver.commit()
    return resolver
