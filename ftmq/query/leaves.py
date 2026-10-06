"""
Leaf conditions, one family per statement-table column they target:

- `M` (meta): `dataset`, `schema` (exact), `schemata` (is-a), `id` / `entity_id` /
  `canonical_id`.
- `P`: a followthemoney property (the `prop` column).
- `G`: a property-type group (the `prop_type` column, keyed by `registry.groups`).
- `C`: a context / storage column such as `origin`, `fragment` or `first_seen`.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Callable, Iterable, Iterator, TypedDict

from banal import as_bool, ensure_list, hash_data, is_listish
from followthemoney import model
from followthemoney.property import Property
from followthemoney.proxy import EntityProxy
from followthemoney.schema import Schema

from ftmq.query.exceptions import QueryError
from ftmq.query.refs import (
    CanonicalIdRef,
    ContextRef,
    DatasetRef,
    EntityIdRef,
    GroupRef,
    IdRef,
    PropRef,
    Ref,
    SchemaRef,
)


class LeafDict(TypedDict):
    """Serialized form of a single [`Leaf`][ftmq.query.leaves.Leaf] condition."""

    t: str  # family tag: "M" | "P" | "G" | "C"
    f: str  # field / property / group name
    op: str  # comparator, e.g. "eq", "in", "gte", "null"
    v: "str | bool | list[str]"  # cast value (list for `in` / `not_in`)


# in-memory test per comparator, `(entity value, leaf value)`; `null` is in `apply`
MATCHERS: dict[str, Callable[[Any, Any], Any]] = {
    "eq": lambda v, x: v == x,
    "not": lambda v, x: v != x,
    "in": lambda v, x: v in x,
    "not_in": lambda v, x: v not in x,
    "gt": lambda v, x: v > x,
    "gte": lambda v, x: v >= x,
    "lt": lambda v, x: v < x,
    "lte": lambda v, x: v <= x,
    "like": lambda v, x: x in v,
    "ilike": lambda v, x: x.lower() in v.lower(),
    "notlike": lambda v, x: x not in v,
    "notilike": lambda v, x: x.lower() not in v.lower(),
    "startswith": lambda v, x: v.startswith(x),
    "endswith": lambda v, x: v.endswith(x),
}
COMPARATORS: frozenset[str] = frozenset(MATCHERS) | {"null"}


# bounds on one field co-refer, see `group_conjunction`
ORDERED_COMPARATORS: frozenset[str] = frozenset({"gt", "gte", "lt", "lte"})


def group_conjunction(leaves: "Iterable[Leaf]") -> "list[list[Leaf]]":
    """Group the leaves of one AND node into the sets that co-refer.

    Conditions that could hold of one statement row must hold of the same row. A
    field's leaves co-refer when they are all bounds (`gt` / `gte` / `lt` / `lte`),
    so `P(date__gte=a) & P(date__lt=b)` is one date inside the window. Any other
    repeated field keeps a per-leaf reading: `M(dataset="d1") & M(dataset="d2")`
    is "in both datasets". Used by both the SQL and the in-memory evaluator.

    Args:
        leaves: The leaf children of one AND node.

    Returns:
        One group per co-referring set, in input order: a multi-leaf group holds
            bounds on one field, every other leaf is its own group.
    """
    leaves = list(leaves)
    by_field: dict[tuple[str, str], list[Leaf]] = defaultdict(list)
    for leaf in leaves:
        by_field[(leaf.family, leaf.key)].append(leaf)
    groups: list[list[Leaf]] = []
    emitted: set[tuple[str, str]] = set()
    for leaf in leaves:
        field = (leaf.family, leaf.key)
        group = by_field[field]
        if len(group) == 1:
            groups.append(group)
        elif all(f.comparator in ORDERED_COMPARATORS for f in group):
            if field not in emitted:
                emitted.add(field)
                groups.append(group)
        else:
            groups.append([leaf])
    return groups


def is_row_scoped(leaf: "Leaf") -> bool:
    """Whether a leaf tests a column describing one statement row.

    True for the `C` columns and `dataset`, so AND-ed conditions on distinct ones
    co-refer. Entity-wide fields (`schema`, `schemata`, the ids) and absence tests
    (`__null=True`) are not row-scoped.
    """
    if leaf.comparator == "null" and leaf.value:
        return False
    return isinstance(leaf, (ContextLeaf, DatasetLeaf))


def row_scoped_groups(groups: "list[list[Leaf]]") -> "list[list[Leaf]]":
    """Select the groups that co-refer across fields (columns of one statement row).

    A field [`group_conjunction`][ftmq.query.leaves.group_conjunction] split
    (repeated equality) is excluded, so `M(dataset="d1") & M(dataset="d2")` stays
    two conditions next to a `C(origin=...)`.

    Args:
        groups: The output of `group_conjunction` for one AND node.

    Returns:
        The subset of those groups, as the same list objects.
    """
    split = Counter((g[0].family, g[0].key) for g in groups)
    return [
        g
        for g in groups
        if split[(g[0].family, g[0].key)] == 1 and all(is_row_scoped(f) for f in g)
    ]


def parse_lookup(key: str) -> tuple[str, str]:
    """Split a `field__comparator` lookup key into its parts.

    Args:
        key: A lookup key such as `name`, `date__gte` or `schema__in`.

    Returns:
        A `(field, comparator)` tuple; the comparator defaults to `eq`.

    Raises:
        QueryError: If the comparator suffix is not a valid comparator.
    """
    field, _, comparator = key.partition("__")
    comparator = comparator or "eq"
    if comparator not in COMPARATORS:
        raise QueryError(f"Invalid comparator in lookup: `{key}`")
    return field, comparator


class Leaf:
    """A single condition: a comparator plus a cast value.

    Subclasses set `family` / `key` and implement `values()` or override `apply()`.
    The comparator is not validated here but by
    [`parse_lookup`][ftmq.query.leaves.parse_lookup].
    """

    family: str = ""
    key: str = ""

    def __init__(self, value: Any, comparator: str | None = None) -> None:
        self.comparator: str = comparator or "eq"
        self.value: Any = self.get_casted_value(value)

    def __hash__(self) -> int:
        # the family is part of the identity: `topics` is a property and a group
        return hash(hash_data(self.field_dict()))

    def __eq__(self, other: Any) -> bool:
        return hash(self) == hash(other)

    def get_casted_value(self, value: Any) -> Any:
        if self.comparator in ("in", "not_in"):
            return set(self.stringify(v) for v in ensure_list(value))
        if self.comparator == "null":
            return as_bool(value)
        if is_listish(value):
            raise QueryError(f"Invalid value for `{self.comparator}`: {value}")
        return self.stringify(value) if value is not None else None

    def stringify(self, value: Any) -> str:
        if hasattr(value, "name"):
            return str(value.name)
        return str(value)

    def values(self, entity: EntityProxy) -> Iterator[str]:
        """Yield the entity values this leaf tests.

        Args:
            entity: The entity to read values from.

        Yields:
            The string values (property values, schema name, ...).
        """
        raise NotImplementedError

    def match(self, value: Any) -> bool:
        """Apply the comparator to one entity value (the in-memory match)."""
        matcher = MATCHERS.get(self.comparator)
        if matcher is None:
            raise QueryError(f"Comparator not implemented: `{self.comparator}`")
        return bool(matcher(value, self.value))

    def match_row(self, statement: Any) -> bool:
        """Test against one statement row; only a row-scoped leaf ever matches."""
        return False

    def apply(self, entity: EntityProxy) -> bool:
        """Test whether the entity matches this condition.

        Args:
            entity: The entity to test.

        Returns:
            Whether any entity value satisfies the comparator (for `null`: the
                presence / absence check).
        """
        if self.comparator == "null":
            present = any(True for _ in self.values(entity))
            # `self.value` is a bool here, cast by `get_casted_value`
            return (not present) if self.value else present
        return any(self.match(v) for v in self.values(entity))

    @property
    def wire(self) -> str:
        """The field's spelling on a string surface, shared with aggregations."""
        return self.key

    def field_dict(self) -> LeafDict:
        """Serialize this leaf to a family-tagged mapping.

        Returns:
            The `{t, f, op, v}` [`LeafDict`][ftmq.query.leaves.LeafDict].
        """
        value = self.value
        if isinstance(value, (set, frozenset)):
            value = sorted(value)
        return LeafDict(t=self.family, f=self.key, op=self.comparator, v=value)


class RefLeaf(Leaf):
    """A leaf reading its field through a [`Ref`][ftmq.query.refs.Ref].

    The ref validates the field name and reads the values; the leaf adds the
    comparator.
    """

    ref: Ref

    def values(self, entity: EntityProxy) -> Iterator[str]:
        yield from self.ref.values(entity)

    def match_row(self, statement: Any) -> bool:
        value = self.ref.row_value(statement)
        if value is None:
            # a `null=False` leaf asks for the column to be set, which it isn't
            return False
        return True if self.comparator == "null" else self.match(value)

    @property
    def wire(self) -> str:
        return self.ref.wire


class DatasetLeaf(RefLeaf):
    """Matches an entity's `datasets` membership."""

    family, key = "M", "dataset"
    ref = DatasetRef()


class SchemaLeaf(RefLeaf):
    """Exact schema match."""

    family, key = "M", "schema"
    ref = SchemaRef()

    def __init__(self, value: Any, comparator: str | None = None) -> None:
        super().__init__(value, comparator)
        # a `startswith` / `ilike` value is not expected to be a full schema name
        if self.comparator in ("eq", "in", "not", "not_in"):
            for name in ensure_list(value):
                if model.get(name) is None:
                    raise QueryError(f"Invalid schema: `{name}`")


class SchemataLeaf(Leaf):
    """Is-a match: `model[X] in entity.schema.schemata`."""

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
        if self.comparator not in ("eq", "in", "not", "not_in"):
            raise QueryError(f"Invalid comparator for `schemata`: `{self.comparator}`")

    @property
    def names(self) -> set[str]:
        """The concrete schemata matched, including non-abstract descendants."""
        names: set[str] = set()
        for schema in self.schemata:
            names.add(schema.name)
            names.update(d.name for d in schema.descendants if not d.abstract)
        return names

    def apply(self, entity: EntityProxy) -> bool:
        hit = bool(self.schemata & entity.schema.schemata)
        if self.comparator in ("not", "not_in"):
            return not hit
        return hit


class IdLeaf(RefLeaf):
    """Matches an entity's id."""

    family, key = "M", "id"
    ref = IdRef()


class EntityIdLeaf(IdLeaf):
    """Matches the `entity_id` column (the pre-resolution id)."""

    key = "entity_id"
    ref = EntityIdRef()


class CanonicalIdLeaf(IdLeaf):
    """Matches the `canonical_id` column (the resolved id)."""

    key = "canonical_id"
    ref = CanonicalIdRef()


class PropertyLeaf(RefLeaf):
    """Matches a specific FtM property value (the `prop` column)."""

    family = "P"

    def __init__(self, prop: str | Property, value: Any, comparator: str | None = None):
        super().__init__(value, comparator)
        self.ref = PropRef(prop)
        self.key = self.ref.key


class GroupLeaf(RefLeaf):
    """A property-type group (the `prop_type` column); `entities` is the reverse
    lookup."""

    family = "G"

    def __init__(self, group: str, value: Any, comparator: str | None = None):
        self.ref = GroupRef(group)
        super().__init__(value, comparator)
        self.key = self.ref.key
        self.prop_type = self.ref.prop_type


class ContextLeaf(RefLeaf):
    """A context / storage column (`origin`, `fragment`, `first_seen`, ...).

    In memory it reads the column off the entity's statements, else
    `entity.context`; in SQL the same-named statement-table column. An entity
    without the key does not match.
    """

    family = "C"

    def __init__(self, key: str, value: Any, comparator: str | None = None):
        super().__init__(value, comparator)
        self.ref = ContextRef(key)
        self.key = key


_META_LEAVES: dict[str, type[Leaf]] = {
    "dataset": DatasetLeaf,
    "schema": SchemaLeaf,
    "schemata": SchemataLeaf,
    "id": IdLeaf,
    "entity_id": EntityIdLeaf,
    "canonical_id": CanonicalIdLeaf,
}


def make_leaf(family: str, key: str, value: Any) -> Leaf:
    """Build a leaf of a field family from a `field__comparator` lookup.

    Args:
        family: `M` (meta), `P` (property), `G` (property-type group) or `C`
            (context column; any identifier, checked at SQL compile time).
        key: A lookup key, e.g. `name`, `amountEur__gte` or `schema__in`.
        value: The lookup value.

    Returns:
        The leaf.

    Raises:
        QueryError: For an unknown family, meta field, property or group.
    """
    field, comparator = parse_lookup(key)
    if family == "M":
        cls = _META_LEAVES.get(field)
        if cls is None:
            raise QueryError(f"Unknown meta field: `{field}`")
        return cls(value, comparator)
    if family == "P":
        return PropertyLeaf(field, value, comparator)
    if family == "G":
        return GroupLeaf(field, value, comparator)
    if family == "C":
        return ContextLeaf(field, value, comparator)
    raise QueryError(f"Unknown field family: `{family}`")


def leaf_from_dict(data: LeafDict) -> Leaf:
    """Rebuild a leaf from its [`LeafDict`][ftmq.query.leaves.LeafDict].

    Args:
        data: The `{t, f, op, v}` mapping produced by
            [`Leaf.field_dict`][ftmq.query.leaves.Leaf.field_dict].

    Returns:
        The reconstructed leaf.
    """
    field, op, value = data["f"], data["op"], data["v"]
    key = field if op == "eq" else f"{field}__{op}"
    return make_leaf(data["t"], key, value)
