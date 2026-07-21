"""
[RQL](https://github.com/pjwerneck/pyrql) (Resource Query Language) bridge.

RQL is a URL-friendly query language of nestable named operators - e.g.
`and(eq(schema,Person),or(eq(properties.name,jane),eq(countries,de)))` - which
maps directly onto the ftmq `Expr` tree (`and`/`or`/`not` + comparison leaves).
Unlike the flat Aleph param grammar, RQL expresses arbitrary nesting, so this is
the way to carry a full `M & (P | G)` tree through a single string. It also
carries aggregations: RQL's native `sum` / `min` / `max` / `mean` / `count` and
`aggregate(...)` operators map onto ftmq `A` nodes, side by side with the filter
under a top-level `and`.

Field names follow the Aleph convention (`schema`, `dataset`, `properties.<name>`,
a `registry.groups` name, or `origin`); a bare name that matches none of those is
treated as an FtM property.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

import pyrql  # type: ignore[import-untyped]

from ftmq.query.aggregations import Agg, make_agg
from ftmq.query.aleph import _FAMILIES, _aleph_field, _resolve_field
from ftmq.query.exceptions import QueryError
from ftmq.query.leaves import Leaf
from ftmq.query.nodes import AND, OR, Expr, combine

# RQL comparison operator -> ftmq comparator
RQL_COMPARATORS = {
    "eq": "eq",
    "ne": "not",
    "lt": "lt",
    "le": "lte",
    "gt": "gt",
    "ge": "gte",
    "in": "in",
    "out": "not_in",
    "like": "like",
    "ilike": "ilike",
    "contains": "like",
}

# ftmq comparator -> RQL operator (the expressible subset; `null`, `startswith`,
# `endswith`, `notlike`, `notilike`, `between` have no RQL equivalent)
TO_RQL_OPERATORS = {
    "eq": "eq",
    "not": "ne",
    "lt": "lt",
    "lte": "le",
    "gt": "gt",
    "gte": "ge",
    "in": "in",
    "not_in": "out",
    "like": "like",
    "ilike": "ilike",
}

# RQL aggregate operator -> ftmq function (RQL calls the average `mean`)
RQL_FUNCTIONS = {
    "sum": "sum",
    "min": "min",
    "max": "max",
    "mean": "avg",
    "count": "count",
}
# ftmq function -> RQL operator
TO_RQL_FUNCTIONS = {v: k for k, v in RQL_FUNCTIONS.items()}
# operator names that introduce an aggregation rather than a filter
AGG_OPERATORS = set(RQL_FUNCTIONS) | {"aggregate"}


def _resolve_rql_field(field: str) -> tuple[str, str]:
    try:
        return _resolve_field(field)
    except QueryError:
        # a bare name that is not meta / group / context / `properties.` is
        # treated as a property; validity is checked when the leaf is built
        return "P", field


def _rql_leaf(op: str, args: list[Any]) -> Expr:
    comparator = RQL_COMPARATORS.get(op)
    if comparator is None:
        raise QueryError(f"Unsupported RQL operator: `{op}`")
    field, value = args[0], args[1]
    family, key = _resolve_rql_field(field)
    if comparator in ("in", "not_in"):
        value = list(value)
    elif comparator in ("like", "ilike") and isinstance(value, str):
        # RQL uses `*` as the wildcard; ftmq `like`/`ilike` is substring-based
        value = value.replace("*", "")
    lookup = key if comparator == "eq" else f"{key}__{comparator}"
    return _FAMILIES[family](**{lookup: value})


def rql_to_expr(data: dict[str, Any]) -> Expr:
    """Convert a parsed RQL AST (`{"name": ..., "args": [...]}`) to an `Expr`."""
    op, args = data["name"], data["args"]
    if op == "and":
        result = combine(*(rql_to_expr(a) for a in args), connector=AND)
    elif op == "or":
        result = combine(*(rql_to_expr(a) for a in args), connector=OR)
    elif op == "not":
        return ~rql_to_expr(args[0])
    else:
        return _rql_leaf(op, args)
    if result is None:
        raise QueryError(f"Empty RQL group: `{op}`")
    return result


def _metric_aggs(node: dict[str, Any], groups: tuple[str, ...]) -> list[Agg]:
    """One RQL metric call (`sum(prop, ...)`) -> `Agg` specs."""
    func = RQL_FUNCTIONS.get(node["name"])
    if func is None:
        raise QueryError(f"Unsupported RQL aggregate operator: `{node['name']}`")
    return [make_agg(func, str(prop), groups) for prop in node["args"]]


def _node_aggs(node: dict[str, Any]) -> list[Agg]:
    """One RQL aggregate node -> `Agg` specs.

    `aggregate(g1, ..., f1(p), ...)` groups the trailing metric calls by the
    leading property names; a bare metric call (`sum(p)`) is ungrouped.
    """
    if node["name"] == "aggregate":
        groups = tuple(a for a in node["args"] if not isinstance(a, dict))
        aggs: list[Agg] = []
        for arg in node["args"]:
            if isinstance(arg, dict):
                aggs.extend(_metric_aggs(arg, groups))
        return aggs
    return _metric_aggs(node, ())


def parse_rql(value: str) -> tuple[Expr | None, set[Agg]]:
    """Parse an RQL query string into a filter `Expr` and aggregation specs.

    Filter operators (`and` / `or` / `not` + comparisons) build the tree; the
    aggregate operators (`sum` / `min` / `max` / `mean` / `count` / `aggregate`)
    build the aggregations. At the top level they sit side by side under `and`.

    Raises:
        QueryError: If the RQL uses an unsupported operator or field.
    """
    data = pyrql.parse(value)
    if not data:
        return None, set()
    aggs: set[Agg] = set()
    if data["name"] in AGG_OPERATORS:
        aggs.update(_node_aggs(data))
        return None, aggs
    if data["name"] == "and":
        filters: list[dict[str, Any]] = []
        for child in data["args"]:
            if isinstance(child, dict) and child.get("name") in AGG_OPERATORS:
                aggs.update(_node_aggs(child))
            else:
                filters.append(child)
        expr = combine(*(rql_to_expr(f) for f in filters), connector=AND)
        return expr, aggs
    return rql_to_expr(data), aggs


def _leaf_to_rql(leaf: Leaf) -> dict[str, Any]:
    op = TO_RQL_OPERATORS.get(str(leaf.comparator))
    if op is None:
        raise QueryError(f"Comparator `{leaf.comparator}` is not expressible as RQL")
    value = leaf.value
    if op in ("in", "out"):
        value = tuple(sorted(str(v) for v in value))
    return {"name": op, "args": [_aleph_field(leaf), value]}


def expr_to_rql(expr: Expr) -> dict[str, Any]:
    """Convert an `Expr` tree to an RQL AST (`{"name": ..., "args": [...]}`)."""
    group = "or" if expr.connector == OR else "and"
    parts: list[dict[str, Any]] = []
    for child in expr.children:
        if isinstance(child, Expr):
            child_ast = expr_to_rql(child)
            # flatten a non-negated same-connector subgroup into this one
            if not child.negated and child_ast.get("name") == group:
                parts.extend(child_ast["args"])
            else:
                parts.append(child_ast)
        else:
            parts.append(_leaf_to_rql(child))
    if not parts:
        raise QueryError("Cannot serialize an empty query to RQL")
    # a single-child group is just that child
    body = parts[0] if len(parts) == 1 else {"name": group, "args": parts}
    if expr.negated:
        return {"name": "not", "args": [body]}
    return body


def _aggs_to_rql(aggs: Iterable[Agg]) -> list[dict[str, Any]]:
    """Aggregation specs -> RQL metric / `aggregate` nodes.

    Ungrouped metrics become bare `sum(prop)` calls; metrics that share a `by`
    are batched into one `aggregate(groups..., funcs...)` node.
    """
    ungrouped: list[dict[str, Any]] = []
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for agg in sorted(aggs, key=lambda a: (a.groups, a.func, a.prop)):
        node: dict[str, Any] = {"name": TO_RQL_FUNCTIONS[agg.func], "args": [agg.prop]}
        if agg.groups:
            grouped[agg.groups].append(node)
        else:
            ungrouped.append(node)
    nodes: list[dict[str, Any]] = list(ungrouped)
    for groups, metrics in grouped.items():
        nodes.append({"name": "aggregate", "args": [*groups, *metrics]})
    return nodes


def to_rql(expr: Expr | None, aggs: Iterable[Agg] = ()) -> str:
    """Serialize a filter tree and aggregation specs to an RQL query string.

    Filters and aggregations sit side by side under a top-level `and`.

    Raises:
        QueryError: If a filter leaf uses a comparator with no RQL equivalent
            (`null`, `startswith`, `endswith`, ...).
    """
    nodes: list[dict[str, Any]] = []
    if expr is not None and expr:
        filter_ast = expr_to_rql(expr)
        # flatten a top-level `and` filter so aggregations join as siblings
        if not expr.negated and filter_ast.get("name") == "and":
            nodes.extend(filter_ast["args"])
        else:
            nodes.append(filter_ast)
    nodes.extend(_aggs_to_rql(aggs))
    if not nodes:
        return ""
    if len(nodes) == 1:
        return str(pyrql.unparse(nodes[0]))
    return str(pyrql.unparse({"name": "and", "args": nodes}))
