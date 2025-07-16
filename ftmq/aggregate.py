"""
Overwrite `ftm aggregate` with the possibility to merge via common parent
schemata.
"""

from typing import Iterable

from anystore.io import logged_items
from followthemoney import model
from followthemoney.exc import InvalidData
from followthemoney.schema import Schema

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


def merge(p1: Entity, p2: Entity, downgrade: bool | None = False) -> Entity:
    try:
        p1 = p1.merge(p2)
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
            return p1.merge(p2)

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
