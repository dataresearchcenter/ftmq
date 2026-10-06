"""
Aggregations: a projection over the matched entities, not a filter.

[`A`][ftmq.query.aggregations.A] builds one immutable
[`Agg`][ftmq.query.aggregations.Agg] spec per `func=<ref>` pair and is passed to
[`Query.aggregate`][ftmq.Query.aggregate].
[`Aggregator`][ftmq.query.aggregations.Aggregator] runs the specs in memory; the
SQL backend compiles the same specs.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping, TypeAlias, cast

from anystore.util import clean_dict
from banal import ensure_list
from followthemoney.types import registry

from ftmq.query.exceptions import QueryError
from ftmq.query.refs import Ref, ref_from_wire
from ftmq.types import Entity

Value: TypeAlias = int | float | str
Values: TypeAlias = list[Value]

AggregatorResult: TypeAlias = dict[str, Any]

FUNCTIONS: frozenset[str] = frozenset({"min", "max", "sum", "avg", "count"})
# buckets per facet (Aleph's default)
DEFAULT_FACET_SIZE = 20


@dataclass(frozen=True)
class Agg:
    """An immutable aggregation spec: a function over a ref, optionally grouped.

    Built via [`A`][ftmq.query.aggregations.A].
    """

    func: str
    ref: Ref
    groups: tuple[Ref, ...] = ()

    @property
    def key(self) -> str:
        """The wire spelling of the aggregated field."""
        return self.ref.wire


def make_agg(func: str, ref: Ref, groups: Iterable[Ref] = ()) -> Agg:
    """Validate and build an `Agg` spec; groups are sorted, so their order is moot."""
    if func not in FUNCTIONS:
        raise QueryError(
            f"Invalid aggregation function: `{func}` - one of "
            f"({', '.join(sorted(FUNCTIONS))})"
        )
    return Agg(
        func=func, ref=_ensure_ref(ref), groups=tuple(sorted(map(_ensure_ref, groups)))
    )


@dataclass(frozen=True)
class FacetOrder:
    """Ranks facet buckets by a grouped metric instead of by entity count.

    Wire spelling: `<func>:<field>[:asc]`.
    """

    func: str
    ref: Ref
    ascending: bool = False

    @property
    def wire(self) -> str:
        """E.g. `sum:properties.amountEur` or `count:id:asc`."""
        wire = f"{self.func}:{self.ref.wire}"
        return f"{wire}:asc" if self.ascending else wire

    @classmethod
    def from_wire(cls, value: str) -> FacetOrder:
        """Parse the wire spelling (`:desc` is accepted too).

        Args:
            value: E.g. `sum:properties.amountEur:asc`.

        Returns:
            The facet order.
        """
        func, _, rest = value.partition(":")
        field, _, direction = rest.partition(":")
        if direction not in ("", "asc", "desc"):
            raise QueryError(
                f"Invalid facet sort: `{value}` - expected `<func>:<field>[:asc]`"
            )
        return make_facet_order(func, ref_from_wire(field), direction == "asc")

    def orders(self, agg: Agg) -> bool:
        """Whether this ranks by `agg` (ignoring its grouping).

        Args:
            agg: An aggregation spec.

        Returns:
            `True` if `agg` has this function and field.
        """
        return agg.func == self.func and agg.ref == self.ref


def make_facet_order(func: str, ref: Ref, ascending: bool = False) -> FacetOrder:
    """Validate and build a `FacetOrder`.

    Args:
        func: The aggregation function.
        ref: The aggregated field.
        ascending: Rank the smallest values first.

    Returns:
        The facet order.
    """
    agg = make_agg(func, ref)
    return FacetOrder(func=agg.func, ref=agg.ref, ascending=ascending)


def groupers(aggs: Iterable[Agg]) -> set[Ref]:
    """The fields the given specs group by (the facets of a query)."""
    return {g for agg in aggs for g in agg.groups}


def _ensure_ref(ref: Ref) -> Ref:
    """Reject a bare field name: only a `Ref` says which family it means."""
    if isinstance(ref, Ref):
        return ref
    raise QueryError(
        f"Invalid aggregation field: `{ref}` - expected a field reference such "
        'as `P("amountEur")`, `G("countries")`, `M("dataset")` or `Year()`'
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
    """An aggregation projection node: `A(sum=P("amountEur"), by=P("beneficiary"))`.

    Each keyword is a function (`min`, `max`, `sum`, `avg`, `count`) over one or
    more refs; `by=` groups by one or more refs. It does not compose with
    `& | ~`; pass it to [`Query.aggregate`][ftmq.Query.aggregate].

    Examples:
        ```python
        A(sum=P("amountEur"), by=P("beneficiary"))
        A(count=M("id"), by=[G("countries"), Year()])
        A(sum=[P("amountEur"), P("amount")])
        ```
    """

    def __init__(
        self,
        *,
        by: Ref | Iterable[Ref] | None = None,
        **funcs: Ref | Iterable[Ref],
    ) -> None:
        groups: tuple[Ref, ...] = tuple(cast("list[Ref]", ensure_list(by)))
        aggs: list[Agg] = []
        for func, refs in funcs.items():
            for ref in cast("list[Ref]", ensure_list(refs)):
                aggs.append(make_agg(func, ref, groups))
        if not aggs:
            raise QueryError("Empty aggregation: pass at least one `func=<ref>`")
        self.aggs: tuple[Agg, ...] = tuple(aggs)


class Aggregator:
    """In-memory accumulator running [`Agg`][ftmq.query.aggregations.Agg] specs.

    Holds all mutable state, so use a fresh instance per run.
    """

    def __init__(
        self,
        aggs: Iterable[Agg],
        sizes: Mapping[Ref, int] | None = None,
        order: FacetOrder | None = None,
    ) -> None:
        self.aggs: list[Agg] = list(aggs)
        self.sizes: dict[Ref, int] = dict(sizes or {})
        self.order = order
        self._groupers = groupers(self.aggs)
        self._values: dict[Agg, Values] = defaultdict(list)
        self._grouped: dict[Agg, dict[Ref, dict[str, Values]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        # entity ids per group value, to rank buckets by count
        self._entities: dict[Ref, dict[str, set[str | None]]] = defaultdict(
            lambda: defaultdict(set)
        )

    def collect(self, proxy: Entity) -> None:
        """Accumulate one entity's values into every spec."""
        for group in self._groupers:
            for g in group.values(proxy):
                self._entities[group][g].add(proxy.id)
        for agg in self.aggs:
            for raw in agg.ref.values(proxy):
                value: Any = (
                    registry.number.to_number(raw) if agg.ref.is_numeric else raw
                )
                if value is None:
                    continue
                self._values[agg].append(value)
                for group in agg.groups:
                    for g in group.values(proxy):
                        self._grouped[agg][group][g].append(value)

    def apply(self, proxies: Iterable[Entity]) -> Iterator[Entity]:
        """Collect every entity while passing the stream through unchanged."""
        for proxy in proxies:
            self.collect(proxy)
            yield proxy

    def _top(self, group: Ref) -> list[str]:
        """Top buckets of `group` by entity count or facet sort, ties by value."""
        order = self.order
        ranking = next(
            (a for a in self.aggs if order and order.orders(a) and group in a.groups),
            None,
        )
        if order is None or ranking is None:
            entities = self._entities[group]
            ranked = sorted(entities, key=lambda g: (-len(entities[g]), g))
        else:
            values = {
                g: reduce_values(ranking.func, v)
                for g, v in self._grouped[ranking][group].items()
            }
            ranked = sorted(g for g, v in values.items() if v is not None)
            ranked.sort(key=lambda g: cast(Any, values[g]), reverse=not order.ascending)
        return ranked[: self.sizes.get(group, DEFAULT_FACET_SIZE)]

    @property
    def result(self) -> AggregatorResult:
        """The reduced result, keyed by the wire spelling of each field.

        `{func: {field: value}, "groups": {group: {func: {field: {bucket: value}}}}}`,
        empties removed, each group capped to its top buckets.
        """
        res: Any = defaultdict(dict)
        groups: Any = defaultdict(lambda: defaultdict(dict))
        top = {group: self._top(group) for group in self._groupers}
        for agg in self.aggs:
            res[agg.func][agg.key] = reduce_values(agg.func, self._values[agg])
            for group in agg.groups:
                grouped = self._grouped[agg][group]
                groups[group.wire][agg.func][agg.key] = {
                    g: reduce_values(agg.func, grouped[g])
                    for g in top[group]
                    if g in grouped
                }
        res["groups"] = groups
        return clean_dict(res)


def aggregations_to_dict(aggs: Iterable[Agg]) -> list[dict[str, Any]]:
    """Serialize specs to sorted `{"func", "field", "by"?}` dicts (wire spellings)."""
    specs: list[dict[str, Any]] = []
    for agg in sorted(aggs, key=lambda a: (a.func, a.key, a.groups)):
        spec: dict[str, Any] = {"func": agg.func, "field": agg.key}
        if agg.groups:
            spec["by"] = [g.wire for g in agg.groups]
        specs.append(spec)
    return specs


def aggregations_from_dict(data: Iterable[dict[str, Any]]) -> set[Agg]:
    """Rebuild specs from the output of `aggregations_to_dict`."""
    return {
        make_agg(
            spec["func"],
            ref_from_wire(spec["field"]),
            [ref_from_wire(g) for g in spec.get("by", [])],
        )
        for spec in data
    }
