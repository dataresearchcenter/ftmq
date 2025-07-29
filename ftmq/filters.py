from typing import Any, Iterable, TypeVar, Union

from banal import as_bool, ensure_list, is_listish
from followthemoney import model
from followthemoney.property import Property
from followthemoney.schema import Schema
from followthemoney.types import registry

from ftmq.enums import Comparators
from ftmq.types import Entity, Value


class Lookup:
    IN = Comparators["in"]
    EQUALS = Comparators["eq"]
    NULL = Comparators["null"]

    def __init__(self, comparator: Comparators, value: Value | None = None):
        self.comparator = self.get_comparator(comparator)
        self.value = value

    def __str__(self) -> str:
        return str(self.comparator)

    def __eq__(self, other: Any) -> bool:
        return str(self) == str(other)

    def get_comparator(self, comparator: str) -> Comparators:
        try:
            return Comparators[comparator]
        except KeyError:
            raise ValueError(f"Invalid oparator: `{comparator}`")

    def apply(self, value: str | None) -> bool:
        if self.comparator == "eq":
            return value == self.value
        if self.comparator == "not":
            return value != self.value
        if self.comparator == "in":
            return value in self.value
        if self.comparator == "not_in":
            return value not in self.value
        if self.comparator == "startswith":
            return value.startswith(self.value)
        if self.comparator == "endswith":
            return value.endswith(self.value)
        if self.comparator == "null":
            return not value == self.value
        if self.comparator == "gt":
            return value > self.value
        if self.comparator == "gte":
            return value >= self.value
        if self.comparator == "lt":
            return value < self.value
        if self.comparator == "lte":
            return value <= self.value
        if self.comparator == "like":
            return self.value in value
        if self.comparator == "ilike":
            return self.value.lower() in value.lower()
        return False


class BaseFilter:
    def __init__(
        self,
        value: Value,
        comparator: Comparators | None = None,
    ):
        try:
            self.comparator = Comparators[comparator or "eq"]
        except KeyError:
            raise ValueError(f"Invalid comparator `{comparator}`")
        self.value: Value = self.get_casted_value(value)
        self.lookup: Lookup = Lookup(self.comparator, self.value)

    def __hash__(self) -> int:
        return hash((self.key, str(self.lookup), str(self.value)))

    def __eq__(self, other: Any) -> bool:
        return hash(self) == hash(other)

    def __lt__(self, other: Any) -> bool:
        # allow ordering (helpful for testing)
        return hash(self) < hash(other)

    def __gt__(self, other: Any) -> bool:
        # allow ordering (helpful for testing)
        return hash(self) > hash(other)

    def to_dict(self) -> dict[str, Any]:
        if self.comparator == Lookup.EQUALS:
            key = self.key
        else:
            key = f"{self.key}__{self.lookup}"
        return {key: self.value}

    def apply(self, entity: Entity) -> bool:
        return self.lookup.apply(self.value)

    def get_casted_value(self, value: Any) -> Value:
        if self.comparator == Lookup.IN:
            return set([self.stringify(v) for v in ensure_list(value)])
        if self.comparator == Lookup.NULL:
            return as_bool(value)
        if is_listish(value):
            raise ValueError(f"Invalid value for `{self.comparator}`: {value}")
        return self.stringify(value) if value is not None else None

    def stringify(self, value: Any) -> str:
        if hasattr(value, "name"):
            return value.name
        return str(value)


class DatasetFilter(BaseFilter):
    key = "dataset"

    def apply(self, entity: Entity) -> bool:
        if self.comparator == Lookup.EQUALS:
            return self.value in entity.datasets
        for value in entity.datasets:
            if self.lookup.apply(value):
                return True
        return False


class OriginFilter(BaseFilter):
    key = "origin"

    def apply(self, entity: Entity) -> bool:
        if not hasattr(entity, "context"):
            return False
        origins = ensure_list(entity.context.get("origin"))
        if self.comparator == Lookup.EQUALS:
            return self.value in origins
        for value in origins:
            if self.lookup.apply(value):
                return True
        return False


class SchemaFilter(BaseFilter):
    key = "schema"

    def __init__(
        self,
        value: Value | Schema | Iterable[Schema],
        comparator: Comparators | None = None,
        schema_include_descendants: bool = False,
        schema_include_matchable: bool = False,
    ):
        super().__init__(value, comparator)
        self.schemata: set[Schema] = set()
        for schema in ensure_list(value):
            schema = model.get(schema)
            if schema is not None:
                self.schemata.add(schema)
                if schema_include_descendants:
                    self.schemata.update(schema.descendants)
                if schema_include_matchable:
                    self.schemata.update(schema.matchable_schemata)
        if not self.schemata and self.comparator in ("eq", "in", "not", "not_in"):
            raise ValueError(f"Invalid schema: `{self.value}`")
        if len(self.schemata) > 1:
            if self.comparator not in ("eq", "in", "not", "not_in"):
                raise ValueError(
                    f"Invalid schema lookup: `{self.comparator}` with multiple schemata"
                )
            self.value = {s.name for s in self.schemata}
            if self.comparator == Comparators["eq"]:
                self.comparator = Comparators["in"]
            elif self.comparator == Comparators["not"]:
                self.comparator = Comparators["not_in"]

    def apply(self, entity: Entity) -> bool:
        if len(self.schemata) > 1:
            return entity.schema in self.schemata
        return self.lookup.apply(entity.schema.name)


class PropertyFilter(BaseFilter):
    def __init__(self, prop: Property, value: Value, comparator: str | None = None):
        super().__init__(value, comparator)
        self.key = self.validate(prop)

    def apply(self, entity: Entity) -> bool:
        for value in entity.get(self.key, quiet=True):
            if self.lookup.apply(value):
                return True
        return False

    def validate(self, prop: str | Property) -> str:
        if isinstance(prop, Property):
            return prop.name
        if isinstance(prop, str):
            for p in model.properties:
                if p.name == prop or p.qname == prop:
                    return prop
        raise ValueError(f"Invalid prop: `{prop}`")


class ReverseFilter(BaseFilter):
    """
    Filter for entities that point to a given entity (via id)
    """

    key = "reverse"

    def apply(self, entity: Entity) -> bool:
        for prop, value in entity.itervalues():
            if prop.type == registry.entity:
                if self.lookup.apply(value):
                    return True
        return False


class IdFilter(BaseFilter):
    key = "id"

    def apply(self, entity: Entity) -> bool:
        return self.lookup.apply(entity.id)


class EntityIdFilter(IdFilter):
    key = "entity_id"


class CanonicalIdFilter(IdFilter):
    key = "canonical_id"


Filter = Union[
    DatasetFilter,
    SchemaFilter,
    PropertyFilter,
    ReverseFilter,
    EntityIdFilter,
    CanonicalIdFilter,
]
F = TypeVar("F", bound=Filter)

FILTERS = {
    "dataset": DatasetFilter,
    "schema": SchemaFilter,
    "property": PropertyFilter,
    "reverse": ReverseFilter,
    "entity_id": EntityIdFilter,
    "canonical_id": CanonicalIdFilter,
    "origin": OriginFilter,
}
