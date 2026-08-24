"""
Aggregations for the ftmq query language.

An aggregation is a *projection* over the matched entities (a SELECT-list /
GROUP BY concern), not a filter predicate: the `A` node does not compose with
the `& | ~` boolean tree the `M`/`P`/`G`/`C` filter nodes build. It is declared
with [`Query.aggregate`][ftmq.Query.aggregate], parallel to `where()` and
`order_by()`.

`A(sum=P("amountEur"), by=P("beneficiary"))` builds one immutable
[`Agg`][ftmq.query.aggregations.Agg] spec per `func=<ref>` pair, where the
field is a [`Ref`][ftmq.query.refs.Ref] built by the same `M` / `P` / `G` / `C`
markers as a filter leaf. [`Aggregator`][ftmq.query.aggregations.Aggregator] is
the in-memory accumulator that runs those specs over a stream of entities; the
SQL backend reads the same specs (see `ftmq.query.sql`).
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, TypeAlias, cast

from anystore.util import clean_dict
from banal import ensure_list
from followthemoney.types import registry

from ftmq.query.exceptions import QueryError
from ftmq.query.refs import Ref, ref_from_wire
from ftmq.types import Entity

Value: TypeAlias = int | float | str
Values: TypeAlias = list[Value]

AggregatorResult: TypeAlias = dict[str, Any]

# the aggregation functions this module implements (see `reduce_values`)
FUNCTIONS: frozenset[str] = frozenset({"min", "max", "sum", "avg", "count"})


@dataclass(frozen=True)
class Agg:
    """An immutable aggregation spec: a function over a field reference,
    optionally grouped by others. Built via the
    [`A`][ftmq.query.aggregations.A] node or
    [`Query.aggregate`][ftmq.Query.aggregate]."""

    func: str
    ref: Ref
    groups: tuple[Ref, ...] = ()

    @property
    def key(self) -> str:
        """The wire spelling of the aggregated field."""
        return self.ref.wire


def make_agg(func: str, ref: Ref, groups: Iterable[Ref] = ()) -> Agg:
    """Validate and build a single [`Agg`][ftmq.query.aggregations.Agg] spec.

    Groups are sorted (by wire spelling), so two specs over the same fields
    compare and serialize identically regardless of input order.
    """
    if func not in FUNCTIONS:
        raise QueryError(
            f"Invalid aggregation function: `{func}` - one of "
            f"({', '.join(sorted(FUNCTIONS))})"
        )
    return Agg(
        func=func, ref=_ensure_ref(ref), groups=tuple(sorted(map(_ensure_ref, groups)))
    )


def _ensure_ref(ref: Ref) -> Ref:
    """Aggregations address fields by reference, not by name: the family a
    bare string belongs to is exactly what the `M` / `P` / `G` / `C` markers
    carry (and what a name alone cannot - `topics` is both a property and a
    property-type group)."""
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

    Each keyword is an aggregation function (`min`, `max`, `sum`, `avg`,
    `count`) whose value is the field reference (or references) to aggregate;
    `by=` groups by one or more references. Fields are addressed with the same
    `M` / `P` / `G` / `C` markers the filter families use, called with a bare
    field name (plus `Year()`), so an aggregation says which family it means
    instead of leaving it to be guessed from the name. Unlike the filter nodes,
    `A` is not a boolean leaf - it does not compose with `& | ~`; pass it to
    [`Query.aggregate`][ftmq.Query.aggregate].

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
    """In-memory accumulator: runs a set of [`Agg`][ftmq.query.aggregations.Agg]
    specs over an entity stream.

    A fresh instance per run holds all mutable state, so applying the same
    query twice never double-counts (the specs themselves are immutable).
    """

    def __init__(self, aggs: Iterable[Agg]) -> None:
        self.aggs: list[Agg] = list(aggs)
        self._values: dict[Agg, Values] = defaultdict(list)
        self._grouped: dict[Agg, dict[Ref, dict[str, Values]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

    def collect(self, proxy: Entity) -> None:
        """Accumulate one entity's values into every spec."""
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

    @property
    def result(self) -> AggregatorResult:
        """The reduced result, keyed by the wire spelling of each field:
        `{func: {field: value}, "groups": {group: {func: {field: {group_value:
        value}}}}}` (empties removed)."""
        res: Any = defaultdict(dict)
        groups: Any = defaultdict(lambda: defaultdict(dict))
        for agg in self.aggs:
            res[agg.func][agg.key] = reduce_values(agg.func, self._values[agg])
            for group in agg.groups:
                groups[group.wire][agg.func][agg.key] = {
                    g: reduce_values(agg.func, values)
                    for g, values in self._grouped[agg][group].items()
                }
        res["groups"] = groups
        return clean_dict(res)


def aggregations_to_dict(aggs: Iterable[Agg]) -> list[dict[str, Any]]:
    """Serialize aggregation specs to the query `to_dict` shape: one
    `{"func": ..., "field": ..., "by": [...]}` mapping per spec (fields spelled
    as on the wire, `by` omitted when ungrouped), deterministically ordered."""
    specs: list[dict[str, Any]] = []
    for agg in sorted(aggs, key=lambda a: (a.func, a.key, a.groups)):
        spec: dict[str, Any] = {"func": agg.func, "field": agg.key}
        if agg.groups:
            spec["by"] = [g.wire for g in agg.groups]
        specs.append(spec)
    return specs


def aggregations_from_dict(data: Iterable[dict[str, Any]]) -> set[Agg]:
    """Rebuild aggregation specs from the output of
    [`aggregations_to_dict`][ftmq.query.aggregations.aggregations_to_dict]."""
    return {
        make_agg(
            spec["func"],
            ref_from_wire(spec["field"]),
            [ref_from_wire(g) for g in spec.get("by", [])],
        )
        for spec in data
    }
