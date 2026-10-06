"""
Field references: a leaf without a value.

A [`Ref`][ftmq.query.refs.Ref] names where to read (a property, a property-type
group, a meta or context column), reads its values off an entity and has one wire
spelling ([`Ref.wire`][ftmq.query.refs.Ref.wire] /
[`ref_from_wire`][ftmq.query.refs.ref_from_wire]):

```python
Query().where(P(amountEur__gte=1000))      # a leaf: ref + comparator + value
Query().aggregate(A(sum=P("amountEur")))   # an aggregation: just the ref
```
"""

from __future__ import annotations

from functools import total_ordering
from typing import Any, ClassVar, Iterator

from banal import ensure_list
from followthemoney import model
from followthemoney.property import Property
from followthemoney.proxy import EntityProxy
from followthemoney.types import PropertyType, registry

from ftmq.query.exceptions import QueryError

PROPERTIES_PREFIX = "properties."
GROUP_PREFIX = "group."
CONTEXT_PREFIX = "context."

PROP_NAMES: frozenset[str] = frozenset(p.name for p in model.properties)
NUMERIC_PROPS: frozenset[str] = frozenset(
    p.name for p in model.properties if p.type == registry.number
)


@total_ordering
class Ref:
    """A reference to one field of one family.

    Built via the `M` / `P` / `G` / `C` constructors called with a field name, or
    `Year()`; subclasses set `family` / `key` and implement `values()`.
    """

    family: ClassVar[str] = ""
    key: str = ""

    def values(self, entity: EntityProxy) -> Iterator[str]:
        """Yield this field's values for an entity."""
        raise NotImplementedError

    def row_value(self, statement: Any) -> str | None:
        """This field's value on one statement row, `None` unless row-scoped."""
        return None

    def selects(self, prop: Property) -> bool:
        """Whether a [`select`][ftmq.Query.select] on this ref keeps the property."""
        return False

    @property
    def is_numeric(self) -> bool:
        """Whether the values are read as numbers, not strings."""
        return False

    @property
    def wire(self) -> str:
        """The spelling on every string surface (params, rql, dict keys, CLI)."""
        return self.key

    def __str__(self) -> str:
        return self.wire

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.wire}>"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Ref) and (self.family, self.key) == (
            other.family,
            other.key,
        )

    def __hash__(self) -> int:
        return hash((self.family, self.key))

    def __lt__(self, other: "Ref") -> bool:
        return self.wire < other.wire


class MetaRef(Ref):
    """A meta column, carried by every statement of an entity."""

    family = "M"


class IdRef(MetaRef):
    """The entity id, not the referent ids in the value of a `prop = "id"` row."""

    key = "id"

    def values(self, entity: EntityProxy) -> Iterator[str]:
        if entity.id is not None:
            yield entity.id


class EntityIdRef(IdRef):
    """The `entity_id` column (the pre-resolution id)."""

    key = "entity_id"


class CanonicalIdRef(IdRef):
    """The `canonical_id` column (the resolved id)."""

    key = "canonical_id"


class DatasetRef(MetaRef):
    """The dataset an entity was observed in."""

    key = "dataset"

    def values(self, entity: EntityProxy) -> Iterator[str]:
        # `.datasets` is added by the StatementEntity / ValueEntity subclasses
        yield from getattr(entity, "datasets", [])

    def row_value(self, statement: Any) -> str | None:
        value = getattr(statement, "dataset", None)
        return None if value is None else str(value)


class SchemaRef(MetaRef):
    """The entity schema."""

    key = "schema"

    def values(self, entity: EntityProxy) -> Iterator[str]:
        yield entity.schema.name


class PropRef(Ref):
    """One followthemoney property (the `prop` column)."""

    family = "P"

    def __init__(self, prop: str | Property) -> None:
        if isinstance(prop, Property):
            prop = prop.name
        if prop not in PROP_NAMES:
            raise QueryError(f"Invalid prop: `{prop}`")
        self.key = prop

    def values(self, entity: EntityProxy) -> Iterator[str]:
        yield from entity.get(self.key, quiet=True)

    def selects(self, prop: Property) -> bool:
        return prop.name == self.key

    @property
    def is_numeric(self) -> bool:
        return self.key in NUMERIC_PROPS

    @property
    def wire(self) -> str:
        # prefixed: `topics` is both a property and a group
        return f"{PROPERTIES_PREFIX}{self.key}"


class GroupRef(Ref):
    """A followthemoney property-type group (the `prop_type` column):
    `names`, `dates`, `countries`, `entities`, ..."""

    family = "G"

    def __init__(self, group: str) -> None:
        if group not in registry.groups:
            raise QueryError(f"Invalid property group: `{group}`")
        self.key = group
        self.prop_type: PropertyType = registry.groups[group]

    def values(self, entity: EntityProxy) -> Iterator[str]:
        yield from entity.get_type_values(self.prop_type)

    def selects(self, prop: Property) -> bool:
        return bool(prop.type == self.prop_type)

    @property
    def wire(self) -> str:
        return f"{GROUP_PREFIX}{self.key}"


class ContextRef(Ref):
    """A context / storage column: `origin`, plus backend-specific columns
    such as `fragment`, `first_seen` or `bucket`."""

    family = "C"

    def __init__(self, key: str) -> None:
        self.key = key

    def values(self, entity: EntityProxy) -> Iterator[str]:
        # a statement entity never populates `context`: read each row instead
        statements = getattr(entity, "statements", None)
        if statements is not None:
            seen: set[str] = set()
            for statement in statements:
                value = self.row_value(statement)
                if value is not None and value not in seen:
                    seen.add(value)
                    yield value
            return
        context: dict[str, Any] = getattr(entity, "context", None) or {}
        values = context.get(self.key)
        if values is None:
            # provenance fields (`first_seen`, ...) live on attributes, not context
            attribute = getattr(entity, self.key, None)
            if not callable(attribute):
                values = attribute
        for value in ensure_list(values):
            yield str(value)

    def row_value(self, statement: Any) -> str | None:
        value = getattr(statement, self.key, None)
        return None if value is None else str(value)

    @property
    def wire(self) -> str:
        # open-ended keys always carry the prefix
        return f"{CONTEXT_PREFIX}{self.key}"


class YearRef(Ref):
    """The year of any date-typed value: derived from the `dates` group."""

    family = "Y"
    key = "year"
    prop_type: PropertyType = registry.date

    def values(self, entity: EntityProxy) -> Iterator[str]:
        for value in entity.get_type_values(self.prop_type):
            yield value[:4]


def Year() -> YearRef:
    """The year dimension: `A(count=M("id"), by=Year())`."""
    return YearRef()


META_REFS: dict[str, type[MetaRef]] = {
    "id": IdRef,
    "entity_id": EntityIdRef,
    "canonical_id": CanonicalIdRef,
    "dataset": DatasetRef,
    "schema": SchemaRef,
}


def make_meta_ref(key: str) -> MetaRef:
    """Build a meta ref (the `M` family) by field name."""
    cls = META_REFS.get(key)
    if cls is None:
        raise QueryError(
            f"Unknown meta field: `{key}` - one of ({', '.join(META_REFS)})"
        )
    return cls()


def ref_from_wire(value: str) -> Ref:
    """Resolve a wire spelling into a ref, for every string surface.

    `properties.<name>`, `group.<name>` and `context.<name>` name their family; a
    meta field (`id`, `entity_id`, `canonical_id`, `dataset`, `schema`) and `year`
    are bare.

    Args:
        value: E.g. `properties.amountEur`, `group.countries`, `id` or `year`.

    Returns:
        The resolved ref.

    Raises:
        QueryError: If the spelling matches no field.
    """
    if value.startswith(PROPERTIES_PREFIX):
        return PropRef(value[len(PROPERTIES_PREFIX) :])
    if value.startswith(GROUP_PREFIX):
        return GroupRef(value[len(GROUP_PREFIX) :])
    if value.startswith(CONTEXT_PREFIX):
        return ContextRef(value[len(CONTEXT_PREFIX) :])
    if value in META_REFS:
        return make_meta_ref(value)
    if value == YearRef.key:
        return YearRef()
    raise QueryError(
        f"Unknown field: `{value}` - expected `{PROPERTIES_PREFIX}<name>`, "
        f"`{GROUP_PREFIX}<name>`, `{CONTEXT_PREFIX}<name>`, "
        f"a meta field ({', '.join(META_REFS)}) or `{YearRef.key}`"
    )
