"""
Aggregations for the ftmq query language.

An aggregation is a *projection* over the matched entities (a SELECT-list /
GROUP BY concern), not a filter predicate: the `A` node does not compose with
the `& | ~` boolean tree the `M`/`P`/`G`/`C` filter nodes build. It is declared
with [`Query.aggregate`][ftmq.Query.aggregate], parallel to `where()` and
`order_by()`.

`A(sum="amountEur", by="beneficiary")` builds one immutable [`Agg`][ftmq.query.aggregations.Agg]
spec per `func=prop` pair. [`Aggregator`][ftmq.query.aggregations.Aggregator] is
the in-memory accumulator that runs those specs over a stream of entities; the
SQL backend reads the same specs (see [`ftmq.query.sql`][ftmq.query.sql]).
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, TypeAlias, cast

from anystore.util import clean_dict
from banal import ensure_list
from followthemoney import model
from followthemoney.types import registry

from ftmq.query.exceptions import QueryError
from ftmq.types import Entity
from ftmq.util import prop_is_numeric

Value: TypeAlias = int | float | str
Values: TypeAlias = list[Value]

AggregatorResult: TypeAlias = dict[str, Any]

# the aggregation functions this module implements (see `reduce_values`);
# mirrors `ftmq.enums.Aggregations`
FUNCTIONS: frozenset[str] = frozenset({"min", "max", "sum", "avg", "count"})
# non-property fields aggregatable via `Agg.values`; mirrors `ftmq.enums.Fields`
FIELDS: frozenset[str] = frozenset({"id", "dataset", "schema", "year"})
_PROP_NAMES: frozenset[str] = frozenset(p.name for p in model.properties)


def _validate_field(name: str) -> str:
    """A property/field name valid as an aggregation target or group."""
    if name in _PROP_NAMES or name in FIELDS:
        return name
    raise QueryError(f"Invalid aggregation field: `{name}`")


@dataclass(frozen=True)
class Agg:
    """An immutable aggregation spec: a function over a property, optionally
    grouped. Built via the [`A`][ftmq.query.aggregations.A] node or
    [`Query.aggregate`][ftmq.Query.aggregate]."""

    func: str
    prop: str
    groups: tuple[str, ...] = ()

    def values(self, proxy: Entity, prop: str | None = None) -> Iterator[str]:
        """Yield the entity values for this spec's property (or a group prop)."""
        prop = prop or self.prop
        if prop == "id":
            if proxy.id is not None:
                yield proxy.id
        elif prop == "dataset":
            yield from proxy.datasets
        elif prop == "schema":
            yield proxy.schema.name
        elif prop == "year":
            for value in proxy.get_type_values(registry.date):
                yield value[:4]
        else:
            yield from proxy.get(prop, quiet=True)


def make_agg(func: str, prop: str, groups: Iterable[str] = ()) -> Agg:
    """Validate and build a single [`Agg`][ftmq.query.aggregations.Agg] spec."""
    if func not in FUNCTIONS:
        raise QueryError(f"Invalid aggregation function: `{func}`")
    return Agg(
        func=func,
        prop=_validate_field(prop),
        groups=tuple(_validate_field(g) for g in groups),
    )


def reduce_values(func: str, values: Values) -> Value | None:
    """Reduce collected values with an aggregation function (`None` if empty)."""
    if not values:
        return None
    if func == "min":
        return min(values)
    if func == "max":
        return max(values)
    if func == "sum":
        return sum(cast("list[float]", values))
    if func == "avg":
        return statistics.mean(cast("list[float]", values))
    if func == "count":
        return len(set(values))
    return None


class A:
    """An aggregation projection node: `A(sum="amountEur", by="beneficiary")`.

    Each keyword is an aggregation function (`min`, `max`, `sum`, `avg`,
    `count`) whose value is the property (or properties) to aggregate; `by=`
    groups by one or more properties / fields. Unlike the `M`/`P`/`G`/`C`
    filter nodes, `A` is not a boolean leaf - it does not compose with
    `& | ~`; pass it to [`Query.aggregate`][ftmq.Query.aggregate].

    Examples:
        ```python
        A(sum="amountEur", by="beneficiary")
        A(count="id")
        A(sum=["amountEur", "amount"])
        ```
    """

    def __init__(
        self,
        *,
        by: str | Iterable[str] | None = None,
        **funcs: str | Iterable[str],
    ) -> None:
        groups: tuple[str, ...] = tuple(str(g) for g in ensure_list(by))
        aggs: list[Agg] = []
        for func, props in funcs.items():
            for prop in ensure_list(props):
                aggs.append(make_agg(func, prop, groups))
        if not aggs:
            raise QueryError("Empty aggregation: pass at least one `func=prop`")
        self.aggs: tuple[Agg, ...] = tuple(aggs)


class Aggregator:
    """In-memory accumulator: runs a set of [`Agg`][ftmq.query.aggregations.Agg]
    specs over an entity stream.

    A fresh instance per run holds all mutable state, so applying the same
    query twice never double-counts (the specs themselves are immutable).
    """

    def __init__(self, aggs: Iterable[Agg]) -> None:
        self.aggs: list[Agg] = list(aggs)
        self._values: dict[Agg, Values] = defaultdict(list)
        self._grouped: dict[Agg, dict[str, dict[str, Values]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

    def collect(self, proxy: Entity) -> None:
        """Accumulate one entity's values into every spec."""
        for agg in self.aggs:
            is_numeric = prop_is_numeric(proxy.schema, agg.prop)
            for raw in agg.values(proxy):
                value: Any = registry.number.to_number(raw) if is_numeric else raw
                if value is None:
                    continue
                self._values[agg].append(value)
                for group in agg.groups:
                    for g in agg.values(proxy, group):
                        self._grouped[agg][group][g].append(value)

    def apply(self, proxies: Iterable[Entity]) -> Iterator[Entity]:
        """Collect every entity while passing the stream through unchanged."""
        for proxy in proxies:
            self.collect(proxy)
            yield proxy

    @property
    def result(self) -> AggregatorResult:
        """The reduced result: `{func: {prop: value}, "groups": {group: {func:
        {prop: {group_value: value}}}}}` (empties removed)."""
        res: Any = defaultdict(dict)
        groups: Any = defaultdict(lambda: defaultdict(dict))
        for agg in self.aggs:
            res[agg.func][agg.prop] = reduce_values(agg.func, self._values[agg])
            for group in agg.groups:
                groups[group][agg.func][agg.prop] = {
                    g: reduce_values(agg.func, values)
                    for g, values in self._grouped[agg][group].items()
                }
        res["groups"] = groups
        return clean_dict(res)


def aggregations_to_dict(aggs: Iterable[Agg]) -> dict[str, Any]:
    """Serialize aggregation specs to the query `to_dict` shape:
    `{func: {props}, "groups": {group: {func: {props}}}}`."""
    data: dict[str, Any] = defaultdict(set)
    data["groups"] = defaultdict(lambda: defaultdict(set))
    for agg in aggs:
        data[agg.func].add(agg.prop)
        for group in agg.groups:
            data["groups"][group][agg.func].add(agg.prop)
    return clean_dict(data)


def aggregations_from_dict(data: dict[str, Any]) -> set[Agg]:
    """Rebuild aggregation specs from the output of
    [`aggregations_to_dict`][ftmq.query.aggregations.aggregations_to_dict],
    restoring each spec's groups from the nested `groups` mapping."""
    data = dict(data)
    nested = data.pop("groups", None) or {}
    groups_by_agg: dict[tuple[str, str], set[str]] = defaultdict(set)
    for group, funcs in nested.items():
        for func, props in funcs.items():
            for prop in ensure_list(props):
                groups_by_agg[(func, prop)].add(group)
    aggs: set[Agg] = set()
    for func, props in data.items():
        for prop in ensure_list(props):
            groups = tuple(sorted(groups_by_agg.get((func, prop), ())))
            aggs.add(make_agg(func, prop, groups))
    return aggs
