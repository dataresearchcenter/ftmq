from typing import Generator

from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver
from sqlalchemy import desc, or_, select


def get_similar(
    entity_id: str, resolver: Resolver, limit: int | None = None
) -> Generator[tuple[str, float], None, None]:
    """
    Get similar entity ids for given id with NO_JUDGEMENT from resolver
    """

    t = resolver._table
    stmt = select(t.c.target, t.c.source, t.c.score)
    stmt = stmt.where(or_(t.c.source == entity_id, t.c.target == entity_id))
    stmt = stmt.where(t.c.judgement == Judgement.NO_JUDGEMENT.value)
    stmt = stmt.order_by(desc(t.c.score))
    if limit:
        stmt = stmt.limit(limit)
    resolver.begin()
    cursor = resolver._get_connection().execute(stmt)
    while batch := cursor.fetchmany(10000):
        for target, source, score in batch:
            if target != entity_id:
                yield target, score
            if source != entity_id:
                yield source, score
