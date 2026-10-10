"""
Merge entities by id, optionally downgrading conflicting schemata to a common parent.

The `*_unsafe` variants merge trusted, id-sorted dict streams without building FtM
objects, and with that without value validation.
"""

import logging
from collections import defaultdict
from typing import Any, Iterable, Iterator, NotRequired, Self, TypedDict, cast

from anystore.io import logged_items
from banal import ensure_list
from followthemoney import Statement, model
from followthemoney.exc import InvalidData
from followthemoney.proxy import EntityProxy
from followthemoney.schema import Schema
from followthemoney.statement import StatementDict
from followthemoney.statement.entity import StatementEntity
from followthemoney.statement.util import BASE_ID

from ftmq.types import Entity, StatementEntities
from ftmq.util import datetime_iso, make_dataset, make_entity

log = logging.getLogger(__name__)


def merge_schema(*schemata: str | Schema) -> Schema:
    """Lenient `model.common_schema` over any number of schemata.

    Schemata another one extends are absorbed, as `model.common_schema` does; if
    more than one remains, they merge to their most specific common ancestor. The
    result doesn't depend on the order: unlike a pairwise fold, a later schema
    can't specialize a merge that already fell back to an ancestor.

    Example:
        ```python
        merge_schema("LegalEntity", "Person")  # Person
        merge_schema("Person", "Company", "LegalEntity")  # LegalEntity
        merge_schema("Airplane", "Vessel")  # Vehicle
        ```

    Args:
        *schemata: Schemata (or their names) to merge

    Returns:
        The merged schema

    Raises:
        InvalidData: For an unknown schema, or schemata without a common ancestor
    """
    resolved: set[Schema] = set()
    for name in schemata:
        schema = model.get(name)
        if schema is None:
            raise InvalidData(f"Invalid schema, can't merge: {name}")
        resolved.add(schema)
    if not resolved:
        raise InvalidData("No schema to merge")
    specific = [
        schema
        for schema in resolved
        if not any(other is not schema and other.is_a(schema) for other in resolved)
    ]
    if len(specific) == 1:
        return specific[0]
    common = set.intersection(*(schema.schemata for schema in specific))
    if not common:
        names = ", ".join(sorted(schema.name for schema in resolved))
        raise InvalidData(f"No common ancestors: {names}")
    return max(common, key=lambda schema: (len(schema.schemata), schema.name))


# captured at import: downstream code (investigraph) may monkeypatch `.merge` to
# delegate to `merge()` below, which would then recurse via the bound method
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


def _with_schema(proxy: Entity, schema: Schema, entity_type: type[Entity]) -> Entity:
    data = proxy.to_full_dict()
    data["schema"] = schema.name
    return make_entity(data, entity_type)


def merge(p1: Entity, p2: Entity, downgrade: bool | None = False) -> Entity:
    try:
        p1 = _native_merge(p1, p2)
        p1.schema = model.common_schema(p1.schema, p2.schema)
        return p1
    except InvalidData as e:
        if downgrade:
            # merge as the common ancestor, losing schema specific properties
            schema = merge_schema(p1.schema, p2.schema)
            entity_type = p1.__class__
            p1 = _with_schema(p1, schema, entity_type)
            p2 = _with_schema(p2, schema, entity_type)
            return _native_merge(p1, p2)

        raise e


def aggregate(
    proxies: Iterable[Entity], downgrade: bool | None = False
) -> StatementEntities:
    buffer: dict[str, Entity] = {}
    schemata: defaultdict[str | None, set[str]] = defaultdict(set)  # for `downgrade`
    for proxy in logged_items(proxies, "Aggregate", item_name="Proxy"):
        if downgrade:
            schemata[proxy.id].add(proxy.schema.name)
        if proxy.id in buffer:
            entity = merge(buffer[proxy.id], proxy, downgrade)
            if downgrade:
                # a pairwise merge would specialize an earlier downgrade again
                schema = merge_schema(*schemata[proxy.id])
                if entity.schema != schema:
                    entity = _with_schema(entity, schema, entity.__class__)
            buffer[proxy.id] = entity
        else:
            buffer[proxy.id] = proxy
    yield from buffer.values()


class EntityDict(TypedDict):
    """An entity as the `*_unsafe` aggregations yield it, in the shape of
    `to_dict()`: from statements as `StatementEntity`, with `caption`,
    `datasets`, `referents`, the timestamps and the statements' `origin` /
    `role`; from fragments as `EntityProxy`, with every other fragment key merged
    into a list."""

    id: str
    schema: str
    properties: dict[str, list[str]]
    caption: NotRequired[str]
    datasets: NotRequired[list[str]]
    referents: NotRequired[list[str]]
    origin: NotRequired[list[str]]
    role: NotRequired[list[str]]
    first_seen: NotRequired[str]
    last_seen: NotRequired[str]
    last_change: NotRequired[str]


# fragment keys `EntityProxy.to_dict` doesn't take from the merged context
_FRAGMENT_KEYS = frozenset(("id", "schema", "properties", "caption"))
# `EntityDict` keys `EntityPayload.from_dict` reads as lists
_LIST_KEYS = ("datasets", "referents", "origin", "role")


class EntityPayload:
    """The statements, the fragments or the dict of one entity.

    Statements are kept as they come and folded once, on first access; fragments
    are folded as they come, as `EntityProxy.merge` does. `statements` stays empty
    for a payload of fragments or of a dict (`from_dict`).
    """

    __slots__ = (
        "id",
        "dataset",
        "statements",
        "_first",
        "_schemata",
        "_properties",
        "_context",
        "_size",
        "_first_seen",
        "_last_seen",
        "_last_change",
        "_min_first_seen",
        "_max_first_seen",
        "_folded",
        "_dict",
    )

    def __init__(self, id: str, dataset: str | None = None) -> None:
        self.id = id
        self.dataset = dataset
        self.statements: list[StatementDict] = []
        self._first: dict[str, Any] | None = None  # a fragment not yet folded
        self._schemata: set[str] = set()
        self._properties: dict[str, dict[str, None]] = {}  # ordered sets
        self._context: dict[str, dict[Any, None]] = {}
        self._size = 0  # over all values, as `EntityProxy._size`
        self._first_seen: str | None = None
        self._last_seen: str | None = None
        self._last_change: str | None = None
        self._min_first_seen: str | None = None
        self._max_first_seen: str | None = None
        self._folded = False
        self._dict: EntityDict | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any], dataset: str | None = None) -> Self:
        """The payload of an entity dict, e.g. a line of an `entities.ftm.json`
        (`smart_stream_json`): `to_dict()` returns it, with its `datasets`,
        `referents`, `origin` and `role` as lists."""
        payload = cls(data["id"], dataset)
        entity = dict(data)
        entity["properties"] = data.get("properties") or {}
        for key in _LIST_KEYS:
            if key in entity:
                entity[key] = [v for v in ensure_list(entity[key]) if v is not None]
        # the only `first_seen` values a dict carries
        seen = [v for v in (data.get("first_seen"), data.get("last_change")) if v]
        payload._min_first_seen = min(seen, default=None)
        payload._max_first_seen = max(seen, default=None)
        payload._folded = True
        payload._dict = cast(EntityDict, entity)
        return payload

    def add_statement(self, statement: StatementDict) -> None:
        self.statements.append(statement)

    def add_fragment(self, fragment: dict[str, Any]) -> None:
        if not self._schemata:
            if self._first is None:
                self._first = fragment
                return
            first, self._first = self._first, None
            self._schemata.add(first["schema"])
            # as the `EntityProxy` constructor: counted, but not capped
            for prop, values in (first.get("properties") or {}).items():
                unique = self._properties[prop] = dict.fromkeys(values)
                self._size += sum(map(len, unique))
            self._merge_context(first)

        name = fragment["schema"]
        self._schemata.add(name)
        # as `EntityProxy.unsafe_add`: reject a value of a capped type (text,
        # html) that would push the entity past the cap; every other value counts
        # towards it, duplicates included. The type is the fragment's own: the
        # merged schema may be a common ancestor lacking the property.
        schema = model.schemata.get(name)
        schema_props = schema.properties if schema is not None else {}
        size = self._size
        properties = self._properties
        for prop, values in (fragment.get("properties") or {}).items():
            seen = properties.get(prop)
            if seen is None:
                seen = properties[prop] = {}
            schema_prop = schema_props.get(prop)
            cap = schema_prop.type.total_size if schema_prop is not None else None
            if cap is None:
                seen.update(dict.fromkeys(values))
                size += sum(map(len, values))
                continue
            for value in values:
                value_size = len(value)
                if size + value_size > cap:
                    continue
                size += value_size
                seen[value] = None
        self._size = size
        self._merge_context(fragment)

    def _merge_context(self, fragment: dict[str, Any]) -> None:
        context = self._context
        for key, value in fragment.items():
            if key not in _FRAGMENT_KEYS:
                seen = context.setdefault(key, {})
                for v in ensure_list(value):
                    if v is not None:
                        seen[v] = None

    def _fold_statements(self) -> None:
        if self._folded or not self.statements:
            return
        self._folded = True
        # locals are faster than attribute lookups in the loop
        schemata = self._schemata
        properties = self._properties
        datasets = self._context.setdefault("datasets", {})
        referents = self._context.setdefault("referents", {})
        origins: dict[str, None] = {}
        roles: dict[str, None] = {}
        first_seen: str | None = None
        last_seen: str | None = None
        last_change: str | None = None
        min_first_seen: str | None = None
        max_first_seen: str | None = None
        entity = self.id

        # rows may carry columns beyond `StatementDict` (`role`)
        for s in cast(list[dict[str, Any]], self.statements):
            schemata.add(s["schema"])
            datasets[s["dataset"]] = None

            origin = s.get("origin")
            if origin:
                origins[origin] = None

            role = s.get("role")
            if role:
                roles[role] = None

            entity_id = s.get("entity_id")
            if entity_id and entity_id != entity:
                referents[entity_id] = None

            seen = datetime_iso(s.get("first_seen"))
            if seen is not None:
                if min_first_seen is None or seen < min_first_seen:
                    min_first_seen = seen
                if max_first_seen is None or seen > max_first_seen:
                    max_first_seen = seen

            prop = s["prop"]
            if prop == BASE_ID:
                if seen is not None and (last_change is None or seen > last_change):
                    last_change = seen
                continue

            values = properties.get(prop)
            if values is None:
                values = properties[prop] = {}
            values[s["value"]] = None
            # non-id statements only, as `StatementEntity.to_context_dict`
            if seen is not None and (first_seen is None or seen < first_seen):
                first_seen = seen
            seen = datetime_iso(s.get("last_seen"))
            if seen is not None and (last_seen is None or seen > last_seen):
                last_seen = seen

        if origins:
            self._context["origin"] = origins
        if roles:
            self._context["role"] = roles
        self._first_seen = first_seen
        self._last_seen = last_seen
        self._last_change = last_change
        self._min_first_seen = min_first_seen
        self._max_first_seen = max_first_seen

    @property
    def origins(self) -> set[str]:
        """Every origin asserting something about this entity."""
        return set(self.to_dict().get("origin", ()))

    @property
    def min_first_seen(self) -> str | None:
        """Earliest `first_seen` across all statements, `id` rows included
        (unlike `first_seen` in `to_dict()`); of a dict, over its `first_seen`
        and `last_change`."""
        self._fold_statements()
        return self._min_first_seen

    @property
    def max_first_seen(self) -> str | None:
        """Latest `first_seen` across all statements, `id` rows included; of a
        dict, over its `first_seen` and `last_change`."""
        self._fold_statements()
        return self._max_first_seen

    def to_dict(self) -> EntityDict:
        """The entity dict, built once.

        Raises:
            InvalidData: If the schemata don't merge (see `merge_schema`)
        """
        if self._dict is None:
            self._dict = self._to_dict()
        return self._dict

    def to_entity(self) -> StatementEntity:
        """The entity as a `StatementEntity`, with FtM validation."""
        dataset = make_dataset(self.dataset)
        if self.statements:
            statements = [Statement.from_dict(s) for s in self.statements]
            return StatementEntity.from_statements(dataset, statements)
        return make_entity(dict(self.to_dict()), StatementEntity, dataset)

    def _to_dict(self) -> EntityDict:
        first = self._first
        if first is not None:  # a single fragment: its properties as is
            self._merge_context(first)
            single: dict[str, Any] = {k: list(v) for k, v in self._context.items()}
            single["id"] = self.id
            single["schema"] = first["schema"]
            single["properties"] = first.get("properties") or {}
            return cast(EntityDict, single)

        if not self.statements:
            schema = merge_schema(*self._schemata)
            data: dict[str, Any] = {k: list(v) for k, v in self._context.items()}
            data["id"] = self.id
            data["schema"] = schema.name
            data["properties"] = {k: list(v) for k, v in self._properties.items()}
            return cast(EntityDict, data)

        self._fold_statements()
        schema = merge_schema(*self._schemata)
        properties = self._properties
        caption = None
        for prop_name in schema.caption:
            values = properties.get(prop_name)
            if values:
                caption = min(values)
                break
        # sorted: the statements of an entity come in no fixed order
        data = {k: sorted(v) for k, v in self._context.items()}
        data["id"] = self.id
        data["caption"] = caption or schema.label
        data["schema"] = schema.name
        data["properties"] = {k: sorted(v) for k, v in sorted(properties.items())}
        if self._first_seen is not None:
            data["first_seen"] = self._first_seen
        if self._last_seen is not None:
            data["last_seen"] = self._last_seen
        if self._last_change is not None:
            data["last_change"] = self._last_change
        return cast(EntityDict, data)


def _to_dict(payload: EntityPayload, skip_errors: bool) -> EntityDict | None:
    try:
        return payload.to_dict()
    except InvalidData:
        if not skip_errors:
            raise
        log.exception("Invalid merge: %s", payload.id)
        return None


def aggregate_statement_payloads(
    data: Iterable[StatementDict], dataset: str | None = None
) -> Iterator[EntityPayload]:
    """Group trusted statement dicts into one `EntityPayload` per entity, for
    consumers that need more than the entity dict (the statements,
    `min_first_seen` / `max_first_seen`, `to_entity()`).

    Args:
        data: Statement dicts, sorted by `canonical_id` (falling back to
            `entity_id`), as a store reads them
        dataset: Dataset name for `EntityPayload.to_entity`

    Returns:
        A generator of `EntityPayload` instances, one per canonical id
    """
    current: EntityPayload | None = None
    for statement in data:
        entity_id = statement.get("canonical_id") or statement["entity_id"]
        if current is None or entity_id != current.id:
            if current is not None:
                yield current
            current = EntityPayload(entity_id, dataset)
        current.add_statement(statement)
    if current is not None:
        yield current


def aggregate_statements_unsafe(
    data: Iterable[StatementDict], skip_errors: bool = False
) -> Iterator[EntityDict]:
    """Aggregate trusted statement dicts (e.g. db rows) into entity dicts.

    A fast path around `StatementEntity.from_statements(...).to_dict()`, without
    FtM object construction and with that without value validation. Values and
    lists are sorted, the caption is the first value of the first caption
    property. Conflicting schemata merge leniently (`merge_schema`).

    Example:
        ```python
        import duckdb
        from ftmq.aggregate import aggregate_statements_unsafe

        rel = duckdb.sql("SELECT * FROM 'statements.parquet' ORDER BY canonical_id")
        rows = (dict(zip(rel.columns, row)) for row in rel.fetchall())
        for data in aggregate_statements_unsafe(rows):
            print(data["id"], data["caption"])
        ```

    Args:
        data: Statement dicts, sorted by `canonical_id` (falling back to
            `entity_id`), as a store reads them
        skip_errors: Log and skip an entity whose schemata don't merge instead of
            raising `InvalidData`

    Returns:
        A generator of entity dicts, one per canonical id
    """
    for payload in aggregate_statement_payloads(data):
        if (result := _to_dict(payload, skip_errors)) is not None:
            yield result


def aggregate_fragments_unsafe(
    data: Iterable[dict[str, Any]], skip_errors: bool = False
) -> Iterator[EntityDict]:
    """Merge trusted entity fragment dicts by id into entity dicts.

    A fast path around `EntityProxy.merge`: the result is what
    `Fragments.iterate()` yields, as `to_dict()`, without FtM object construction
    and with that without value cleaning. Values unite in order of appearance,
    every other key becomes the list of its unique values (`caption` is
    dropped); a single fragment keeps its properties as is. Text and html values
    that would push a merged entity past the `PROP_VALUE_MAX` size are rejected,
    as `EntityProxy.unsafe_add` does. Conflicting schemata merge leniently
    (`merge_schema`).

    Example:
        ```python
        from ftmq.aggregate import aggregate_fragments_unsafe
        from ftmq.store.fragments import get_fragments

        fragments = get_fragments("my_dataset")
        for data in aggregate_fragments_unsafe(fragments.fragments()):
            print(data["id"], data["schema"])
        ```

    Args:
        data: Entity fragment dicts, sorted by `id`
        skip_errors: Log and skip an entity whose schemata don't merge instead of
            raising `InvalidData`

    Returns:
        A generator of entity dicts, one per id
    """
    current: EntityPayload | None = None
    for fragment in data:
        entity_id = fragment["id"]
        if current is None or entity_id != current.id:
            if current is not None and (result := _to_dict(current, skip_errors)):
                yield result
            current = EntityPayload(entity_id)
        current.add_fragment(fragment)
    if current is not None and (result := _to_dict(current, skip_errors)):
        yield result
