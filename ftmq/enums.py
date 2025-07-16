from enum import Enum, EnumMeta
from typing import Any, Iterable

from followthemoney import model


class EMeta(EnumMeta):
    def __contains__(self, member: Any) -> bool:
        try:
            self(member)
            return True
        except ValueError:
            return False


def StrEnum(name: str, values: Iterable[Any]) -> Enum:
    # mimic py3.11 enum.StrEnum
    # and fix default enum implementation:
    # https://gist.github.com/simonwoerpel/bdb9959de75e550349961677549624fb
    class _StrEnum(str, Enum, metaclass=EMeta):
        def __str__(self):
            return self.value

        @property
        def value(self) -> Any:
            return super().value

        @property
        def name(self) -> Any:
            return super().name

        @value.setter
        def value(self, value: str):
            self.value = value

        @name.setter
        def name(self, name: str):
            self.name = name

    return _StrEnum(name, {str(v): str(v) for v in values})


Schemata = StrEnum("Schemata", [k for k in model.schemata.keys()])
Things = StrEnum("Things", [k for k, s in model.schemata.items() if s.is_a("Thing")])
Intervals = StrEnum(
    "Intervals", [k for k, s in model.schemata.items() if s.is_a("Interval")]
)
Properties = StrEnum("Properties", {p.name for p in model.properties})
PropertyTypes = StrEnum("PropertyTypes", {p.type for p in model.properties})
PropertyTypesMap = Enum("PropertyTypesMap", {p.name: p.type for p in model.properties})
Comparators = StrEnum(
    "Comparators",
    [
        "eq",
        "not",
        "in",
        "not_in",
        "null",
        "gt",
        "gte",
        "lt",
        "lte",
        "like",
        "ilike",
        "notlike",
        "notilike",
        "between",
        "startswith",
        "endswith",
    ],
)
Aggregations = StrEnum("Aggregations", ("min", "max", "sum", "avg", "count"))
Fields = StrEnum("Fields", ["id", "dataset", "schema", "year"])

# aleph
Frequencies = StrEnum(
    "Frequencies",
    ("unknown", "never", "hourly", "daily", "weekly", "monthly", "annually"),
)
Categories = StrEnum(
    "Categories",
    (
        "news",
        "leak",
        "land",
        "gazette",
        "court",
        "company",
        "sanctions",
        "procurement",
        "finance",
        "grey",
        "library",
        "license",
        "regulatory",
        "poi",
        "customs",
        "census",
        "transport",
        "casefile",
        "other",
        "casefile",
    ),
)
