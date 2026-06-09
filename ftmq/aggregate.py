"""
Overwrite `ftm aggregate` with the possibility to merge via common parent
schemata.
"""

from typing import Iterable

from anystore.io import logged_items
from followthemoney import model
from followthemoney.exc import InvalidData
from followthemoney.proxy import EntityProxy
from followthemoney.schema import Schema
from followthemoney.statement.entity import StatementEntity

from ftmq.enums import Schemata
from ftmq.types import Entity, StatementEntities
from ftmq.util import make_entity

SCHEMATA: dict[Schemata, int] = {s: len(model[s].extends) for s in Schemata}


def extends(s: Schema) -> set[Schema]:
    schemata: set[Schema] = set()
    for schema in s.extends:
        schemata.add(schema)
        for e_schema in schema.extends:
            schemata.add(e_schema)
            schemata.update(extends(e_schema))
    return schemata


def common_ancestor(s1: Schema, s2: Schema) -> Schema:
    ancestors = extends(s1) & extends(s2)
    for schema in sorted(ancestors, key=lambda x: SCHEMATA[x.name], reverse=True):
        return schema
    raise InvalidData(f"No common ancestors: {s1}, {s2}")


# `merge()` is the *downgrading* merge built on top of FollowTheMoney's native
# entity merge. Downstream code (e.g. investigraph) may monkeypatch
# `EntityProxy.merge` / `StatementEntity.merge` to delegate back into this
# function so that every merge downgrades on schema conflict -- calling the
# bound `.merge` here would then recurse infinitely. Capture the native
# implementations at import time (before any such override is installed) and
# always merge through them.
_NATIVE_MERGE = {
    StatementEntity: StatementEntity.merge,
    EntityProxy: EntityProxy.merge,
}


def _native_merge(p1: Entity, p2: Entity) -> Entity:
    for klass in type(p1).__mro__:
        fn = _NATIVE_MERGE.get(klass)
        if fn is not None:
            return fn(p1, p2)
    return p1.merge(p2)  # unknown proxy type: fall back to the bound method


def merge(p1: Entity, p2: Entity, downgrade: bool | None = False) -> Entity:
    try:
        p1 = _native_merge(p1, p2)
        p1.schema = model.common_schema(p1.schema, p2.schema)
        return p1
    except InvalidData as e:
        if downgrade:
            # try common schemata, this will probably "downgrade" entities
            # as in, losing some schema specific properties
            schema = common_ancestor(p1.schema, p2.schema)
            p1_data = p1.to_full_dict()
            p1_data["schema"] = schema.name
            p2_data = p2.to_full_dict()
            p2_data["schema"] = schema.name
            p1 = make_entity(p1_data, p1.__class__)
            p2 = make_entity(p2_data, p1.__class__)
            return _native_merge(p1, p2)

        raise e


def aggregate(
    proxies: Iterable[Entity], downgrade: bool | None = False
) -> StatementEntities:
    buffer: dict[str, Entity] = {}
    for proxy in logged_items(proxies, "Aggregate", item_name="Proxy"):
        if proxy.id in buffer:
            buffer[proxy.id] = merge(buffer[proxy.id], proxy, downgrade)
        else:
            buffer[proxy.id] = proxy
    yield from buffer.values()
