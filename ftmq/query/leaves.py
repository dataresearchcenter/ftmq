"""
Leaf conditions for the ftmq query language, split by the statement-table
column they target:

- meta leaves (`M`): `dataset`, `schema` (exact), `schemata` (is-a), `origin`,
  `id` / `entity_id` / `canonical_id`.
- the property leaf (`P`): a specific FtM property (the `prop` column).
- the group leaf (`G`): a followthemoney property-type group (the `prop_type`
  column, keyed by `registry.groups`: `names`, `dates`, `countries`, `entities`,
  ...).

Leaves reuse `Lookup` / `BaseFilter` from `ftmq.query.filters` for comparator
matching and casting, and add the per-family entity access plus correct
`null` (present/absent) semantics.
"""

from __future__ import annotations

from typing import Any, Iterator, TypedDict

from banal import ensure_list
from followthemoney import model
from followthemoney.property import Property
from followthemoney.proxy import EntityProxy
from followthemoney.schema import Schema
from followthemoney.types import registry

from ftmq.query.exceptions import QueryError
from ftmq.query.filters import BaseFilter
from ftmq.util import parse_comparator

META_FIELDS = (
    "dataset",
    "schema",
    "schemata",
    "origin",
    "id",
    "entity_id",
    "canonical_id",
)


class LeafDict(TypedDict):
    """Serialized form of a single [`Leaf`][ftmq.query.leaves.Leaf] condition."""

    t: str  # family tag: "M" (meta) | "P" (property) | "G" (group)
    f: str  # field / property / group name
    op: str  # comparator, e.g. "eq", "in", "gte", "null"
    v: "str | bool | list[str]"  # cast value (list for `in` / `not_in`)


def parse_lookup(key: str) -> tuple[str, str]:
    """Split a `field__comparator` lookup key into its parts.

    Args:
        key: A lookup key such as `name`, `date__gte` or `schema__in`.

    Returns:
        A `(field, comparator)` tuple; the comparator defaults to `eq`.

    Raises:
        QueryError: If the comparator suffix is not a valid comparator.
    """
    try:
        field, comparator = parse_comparator(key)
    except KeyError:
        raise QueryError(f"Invalid comparator in lookup: `{key}`")
    return field, str(comparator)


class Leaf(BaseFilter):
    """A single condition. Subclasses set `family` and implement `values()`
    (the entity values to test) or override `apply()`."""

    family: str = ""
    key: str = ""

    def values(self, entity: EntityProxy) -> Iterator[str]:
        """Yield the entity values this leaf tests against.

        Args:
            entity: The entity to read values from.

        Yields:
            The relevant string values (property values, schema name, ...).
        """
        raise NotImplementedError

    def apply(self, entity: EntityProxy) -> bool:
        """Test whether the entity matches this condition.

        Args:
            entity: The entity to test.

        Returns:
            `True` if any of the entity's values satisfy the comparator (or,
            for the `null` comparator, the presence / absence check).
        """
        if str(self.comparator) == "null":
            present = any(True for _ in self.values(entity))
            # value was cast to a bool by `BaseFilter.get_casted_value`
            return (not present) if self.value else present
        return any(self.lookup.apply(v) for v in self.values(entity))

    def field_dict(self) -> LeafDict:
        """Serialize this leaf to a family-tagged mapping.

        Returns:
            The `{t, f, op, v}` [`LeafDict`][ftmq.query.leaves.LeafDict] used by
            the query-tree serialization.
        """
        value = self.value
        if isinstance(value, (set, frozenset)):
            value = sorted(value)
        return LeafDict(t=self.family, f=self.key, op=str(self.comparator), v=value)


class DatasetLeaf(Leaf):
    """Matches an entity's `datasets` membership."""

    family, key = "M", "dataset"

    def values(self, entity: EntityProxy) -> Iterator[str]:
        # `.datasets` is added by the StatementEntity / ValueEntity subclasses
        yield from getattr(entity, "datasets", [])


class SchemaLeaf(Leaf):
    """Exact schema match."""

    family, key = "M", "schema"

    def __init__(self, value: Any, comparator: str | None = None) -> None:
        super().__init__(value, comparator)
        # validate real schema names for equality-style comparators (a
        # `startswith`/`ilike` prefix is not expected to be a full schema)
        if str(self.comparator) in ("eq", "in", "not", "not_in"):
            for name in ensure_list(value):
                if model.get(name) is None:
                    raise QueryError(f"Invalid schema: `{name}`")

    def values(self, entity: EntityProxy) -> Iterator[str]:
        yield entity.schema.name


class SchemataLeaf(Leaf):
    """`is-a` match: the entity's schema (or one of its ancestors) is the
    queried schema, i.e. `model[X] in entity.schema.schemata`."""

    family, key = "M", "schemata"

    def __init__(self, value: Any, comparator: str | None = None) -> None:
        super().__init__(value, comparator)
        self.schemata: set[Schema] = set()
        for item in ensure_list(value):
            schema = item if isinstance(item, Schema) else model.get(item)
            if schema is None:
                raise QueryError(f"Invalid schema: `{item}`")
            self.schemata.add(schema)
        if not self.schemata:
            raise QueryError(f"Invalid schemata: `{value}`")
        if str(self.comparator) not in ("eq", "in", "not", "not_in"):
            raise QueryError(f"Invalid comparator for `schemata`: `{self.comparator}`")

    def apply(self, entity: EntityProxy) -> bool:
        hit = bool(self.schemata & entity.schema.schemata)
        if str(self.comparator) in ("not", "not_in"):
            return not hit
        return hit


class OriginLeaf(Leaf):
    """Matches an entity's `origin` (read from its context)."""

    family, key = "M", "origin"

    def values(self, entity: EntityProxy) -> Iterator[str]:
        context = getattr(entity, "context", None) or {}
        value: Any = context.get("origin")
        origins: list[str] = ensure_list(value)
        yield from origins


class IdLeaf(Leaf):
    """Matches an entity's id."""

    family, key = "M", "id"

    def values(self, entity: EntityProxy) -> Iterator[str]:
        if entity.id is not None:
            yield entity.id


class EntityIdLeaf(IdLeaf):
    """Matches the `entity_id` column (the pre-resolution id)."""

    key = "entity_id"


class CanonicalIdLeaf(IdLeaf):
    """Matches the `canonical_id` column (the resolved id)."""

    key = "canonical_id"


class PropertyLeaf(Leaf):
    """Matches a specific FtM property value (the `prop` column)."""

    family = "P"

    def __init__(self, prop: str | Property, value: Any, comparator: str | None = None):
        super().__init__(value, comparator)
        self.key = self._validate(prop)

    @staticmethod
    def _validate(prop: str | Property) -> str:
        if isinstance(prop, Property):
            return prop.name
        if isinstance(prop, str):
            for p in model.properties:
                if p.name == prop or p.qname == prop:
                    return prop
        raise QueryError(f"Invalid prop: `{prop}`")

    def values(self, entity: EntityProxy) -> Iterator[str]:
        yield from entity.get(self.key, quiet=True)


class GroupLeaf(Leaf):
    """A property-type group (the `prop_type` column). `entities` is the
    reverse-lookup group."""

    family = "G"

    def __init__(self, group: str, value: Any, comparator: str | None = None):
        if group not in registry.groups:
            raise QueryError(f"Invalid property group: `{group}`")
        super().__init__(value, comparator)
        self.key = group
        self.prop_type = registry.groups[group]

    def values(self, entity: EntityProxy) -> Iterator[str]:
        yield from entity.get_type_values(self.prop_type)


_META_LEAVES: dict[str, type[Leaf]] = {
    "dataset": DatasetLeaf,
    "schema": SchemaLeaf,
    "schemata": SchemataLeaf,
    "origin": OriginLeaf,
    "id": IdLeaf,
    "entity_id": EntityIdLeaf,
    "canonical_id": CanonicalIdLeaf,
}


def make_meta_leaf(key: str, value: Any) -> Leaf:
    """Build a meta leaf (the `M` family) from a lookup.

    Args:
        key: A meta lookup key, e.g. `dataset__in`, `schema` or `id__startswith`.
        value: The lookup value.

    Returns:
        The resolved meta leaf.

    Raises:
        QueryError: If the field is not a known meta field.
    """
    field, comparator = parse_lookup(key)
    cls = _META_LEAVES.get(field)
    if cls is None:
        raise QueryError(f"Unknown meta field: `{field}`")
    return cls(value, comparator)


def make_property_leaf(key: str, value: Any) -> Leaf:
    """Build a property leaf (the `P` family) from a lookup.

    Args:
        key: A property lookup key, e.g. `name` or `amountEur__gte`.
        value: The lookup value.

    Returns:
        The resolved property leaf.

    Raises:
        QueryError: If the property is not a valid FtM property.
    """
    prop, comparator = parse_lookup(key)
    return PropertyLeaf(prop, value, comparator)


def make_group_leaf(key: str, value: Any) -> Leaf:
    """Build a property-type group leaf (the `G` family) from a lookup.

    Args:
        key: A group lookup key, e.g. `countries`, `dates__gte` or `entities`.
        value: The lookup value.

    Returns:
        The resolved group leaf.

    Raises:
        QueryError: If the group is not a valid `registry.groups` name.
    """
    group, comparator = parse_lookup(key)
    return GroupLeaf(group, value, comparator)


LEAF_FACTORIES = {
    "M": make_meta_leaf,
    "P": make_property_leaf,
    "G": make_group_leaf,
}


def leaf_from_dict(data: LeafDict) -> Leaf:
    """Reconstruct a leaf from its serialized [`LeafDict`][ftmq.query.leaves.LeafDict].

    Args:
        data: The `{t, f, op, v}` mapping produced by
            [`Leaf.field_dict`][ftmq.query.leaves.Leaf.field_dict].

    Returns:
        The reconstructed leaf.
    """
    field, op, value = data["f"], data["op"], data["v"]
    key = field if op == "eq" else f"{field}__{op}"
    return LEAF_FACTORIES[data["t"]](key, value)
