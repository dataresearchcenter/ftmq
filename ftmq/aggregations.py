import statistics
from collections import defaultdict
from typing import Any, Generator, Iterable, TypeAlias

from anystore.util import clean_dict
from banal import ensure_list
from followthemoney.types import registry
from pydantic import BaseModel

from ftmq.enums import Aggregations, Fields, Properties
from ftmq.types import Entities, Entity
from ftmq.util import prop_is_numeric

Value: TypeAlias = int | float | str
Values: TypeAlias = list[Value]


class Aggregation(BaseModel):
    prop: Properties | Fields
    func: Aggregations
    values: Values = []
    value: Value | None = None
    group_props: list[Properties | Fields] | None = []
    grouper: dict[Properties, dict[str, Values]] = defaultdict(
        lambda: defaultdict(list)
    )
    groups: dict[Properties, dict[str, Value]] = defaultdict(dict)

    def __hash__(self) -> int:
        return hash((self.prop, self.func, *sorted(ensure_list(self.group_props))))

    def __eq__(self, other: Any) -> bool:
        return hash(self) == hash(other)

    def get_value(self, values: Values) -> Value | None:
        if self.func == "min":
            return min(values)
        if self.func == "max":
            return max(values)
        if self.func == "sum":
            return sum(values)
        if self.func == "avg":
            return statistics.mean(values)
        if self.func == "count":
            return len(set(values))

    def get_proxy_values(
        self, proxy: Entity, prop: Properties | Fields | None = None
    ) -> Generator[str, None, None]:
        prop = prop or self.prop
        if prop == Fields.id:
            yield proxy.id
        elif prop == Fields.dataset:
            yield from proxy.datasets
        elif prop == Fields.schema:
            yield proxy.schema.name
        elif prop == Fields.year:
            for value in proxy.get_type_values(registry.date):
                yield value[:4]
        else:
            yield from proxy.get(prop, quiet=True)

    def collect(self, proxy: Entity) -> Entity:
        is_numeric = prop_is_numeric(proxy.schema, self.prop)
        for value in self.get_proxy_values(proxy):
            if is_numeric:
                value = registry.number.to_number(value)
            if value is not None:
                self.values.append(value)
                for prop in self.group_props:
                    for g in self.get_proxy_values(proxy, prop):
                        self.grouper[prop][g].append(value)
        return proxy

    def apply(self, proxies: Entities) -> Entities:
        for proxy in proxies:
            yield self.collect(proxy)
        self.__exit__()

    def __enter__(self) -> "Aggregation":
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self.value = self.get_value(self.values)
        for prop in self.group_props:
            for g, values in self.grouper[prop].items():
                self.groups[prop][g] = self.get_value(values)

    def dict(self, *args, **kwargs) -> dict[str, Any]:
        self.__exit__()
        return super().dict(*args, **kwargs)


AggregatorResult: TypeAlias = dict[
    Aggregations | dict[str, Aggregations], dict[Properties, Value]
]


class Aggregator(BaseModel):
    aggregations: list[Aggregation] = []
    result: AggregatorResult = defaultdict(dict)

    def __enter__(self) -> "Aggregator":
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self.result["groups"] = defaultdict(lambda: defaultdict(dict))
        for agg in self.aggregations:
            self.result[str(agg.func)][str(agg.prop)] = agg.value
            for group in agg.group_props:
                self.result["groups"][str(group)][str(agg.func)][str(agg.prop)] = (
                    agg.groups[group]
                )
        self.result = clean_dict(self.result)

    def apply(self, proxies: Entities) -> Entities:
        for agg in self.aggregations:
            proxies = agg.apply(proxies)
        yield from proxies
        self.__exit__()

    @classmethod
    def from_dict(
        cls, data: dict[Aggregations | str, Iterable[Properties]]
    ) -> "Aggregator":
        groups = ensure_list(data.pop("groups", None))
        return cls(
            aggregations=[
                Aggregation(prop=p, func=agg, group_props=groups)
                for agg, props in data.items()
                for p in ensure_list(props)
            ],
        )

    def to_dict(self) -> dict[str, set[str]]:
        data = defaultdict(set)
        data["groups"] = defaultdict(lambda: defaultdict(set))
        for agg in self.aggregations:
            data[str(agg.func)].add(str(agg.prop))
            for group in agg.group_props:
                data["groups"][str(group)][str(agg.func)].add(str(agg.prop))
        return clean_dict(data)
