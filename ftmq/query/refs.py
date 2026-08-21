"""
Field references: a leaf without a value.

A [`Ref`][ftmq.query.refs.Ref] names *where* to read - a followthemoney
property, a property-type group, a meta column, a context column - without
saying what to match. It is what a filter leaf carries besides its comparator
and value, and it is what an aggregation
([`A`][ftmq.query.aggregations.A]) projects over:

```python
Query().where(P(amountEur__gte=1000))      # a leaf: ref + comparator + value
Query().aggregate(A(sum=P("amountEur")))   # an aggregation: just the ref
```

Refs are built by the same family constructors as filter leaves, called with a
positional field name instead of `field=value` lookups: `M("dataset")`,
`P("amountEur")`, `G("countries")`, `C("origin")`, plus `Year()` for the
date-derived year dimension.

Every ref knows how to read its values off an entity (used by the in-memory
evaluator, and by the filter leaves, which delegate here) and how it is spelled
on the wire ([`Ref.wire`][ftmq.query.refs.Ref.wire] /
[`ref_from_wire`][ftmq.query.refs.ref_from_wire]). The SQL side maps refs to
columns in `ftmq.query.sql`, keeping sqlalchemy out of the query IR.
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

# valid followthemoney property names, and the subset whose values are numbers
# (the ones an aggregation or sort reads through the number parser instead of
# as strings). The single source for "is this prop numeric" - the SQL adapter
# and the in-memory sort read this same set.
PROP_NAMES: frozenset[str] = frozenset(p.name for p in model.properties)
NUMERIC_PROPS: frozenset[str] = frozenset(
    p.name for p in model.properties if p.type == registry.number
)


@total_ordering
class Ref:
    """A reference to one field of one family.

    Subclasses set `family` / `key` and implement `values()`; they are built
    via the `M` / `P` / `G` / `C` constructors rather than directly.
    """

    family: ClassVar[str] = ""
    key: str = ""

    def values(self, entity: EntityProxy) -> Iterator[str]:
        """Yield this field's values for an entity."""
        raise NotImplementedError

    @property
    def is_numeric(self) -> bool:
        """Whether the values are numbers (read through followthemoney's
        number parser instead of as strings)."""
        return False

    @property
    def wire(self) -> str:
        """How this ref is spelled on a string surface (params, rql, dict keys,
        CLI flags): the same spelling the filter grammar uses."""
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
    """The entity id. Aggregating it addresses *entities*, not the referent
    ids in the `value` of a `prop = "id"` statement."""

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

    @property
    def is_numeric(self) -> bool:
        return self.key in NUMERIC_PROPS

    @property
    def wire(self) -> str:
        # the same `properties.` prefix the filter grammar uses, so a name that
        # is both a property and a group (`topics`) stays addressable as either
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

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, GroupRef):
            return NotImplemented
        return self.key == other.key and self.prop_type == other.prop_type

    def __hash__(self) -> int:
        return hash((self.family, self.key, self.prop_type))

    def values(self, entity: EntityProxy) -> Iterator[str]:
        yield from entity.get_type_values(self.prop_type)

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
        context: dict[str, Any] = getattr(entity, "context", None) or {}
        for value in ensure_list(context.get(self.key)):
            yield str(value)

    @property
    def wire(self) -> str:
        # context keys are open-ended (backends add their own columns), so they
        # always carry the prefix rather than competing with the other families
        return f"{CONTEXT_PREFIX}{self.key}"


class YearRef(Ref):
    """The year of any date-typed value - a dimension derived from the `dates`
    group, not a column of its own."""

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
    """Resolve a wire spelling back into a ref - the single place a string
    becomes a field reference, used by every string surface (URL params, RQL,
    `to_dict` keys, CLI flags).

    The family is encoded in the spelling: `properties.<name>` for a property,
    `group.<name>` for a property-type group, `context.<name>` for a context
    column; a meta field (`id`, `entity_id`, `canonical_id`, `dataset`,
    `schema`) and `year` are bare.

    Args:
        value: A wire key such as `properties.amountEur`, `group.countries`,
            `id`, `year` or `context.origin`.

    Returns:
        The resolved ref.

    Raises:
        QueryError: If the spelling matches no field of any family.
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
