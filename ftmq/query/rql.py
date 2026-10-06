"""
[RQL](https://github.com/pjwerneck/pyrql) (Resource Query Language) bridge.

RQL nests named operators, e.g.
`and(eq(schema,Person),or(eq(properties.name,jane),eq(group.countries,de)))`, so
unlike the flat Aleph params it carries any `& | ~` tree. Aggregations map to
RQL's `sum` / `min` / `max` / `mean` / `count` / `aggregate(...)` and the
projection to `select(...)`, side by side with the filter under a top-level
`and`.

Fields use the shared wire spelling (`properties.<name>`, `group.<name>`,
`context.<name>`, bare meta fields and `year`); any other bare name is read as a
property.
"""

from __future__ import annotations

import warnings
from collections import defaultdict
from typing import Any, Iterable

with warnings.catch_warnings():
    # pyrql builds its grammar on import with pyparsing's deprecated camelCase api
    warnings.simplefilter("ignore", DeprecationWarning)
    import pyrql  # type: ignore[import-untyped]

# pyrql warns on every parse too; catch_warnings per call is not thread-safe
warnings.filterwarnings(
    "ignore", category=DeprecationWarning, module=r"pyrql\.|pyparsing\."
)

from ftmq.query.aggregations import Agg, make_agg  # noqa: E402
from ftmq.query.aleph import _FAMILIES, _resolve_field  # noqa: E402
from ftmq.query.exceptions import QueryError  # noqa: E402
from ftmq.query.leaves import Leaf  # noqa: E402
from ftmq.query.nodes import AND, OR, Expr, combine  # noqa: E402
from ftmq.query.refs import Ref, ref_from_wire  # noqa: E402

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

# the expressible subset (`null`, `startswith`, ... have no RQL operator)
TO_RQL_OPERATORS = {v: k for k, v in RQL_COMPARATORS.items() if k != "contains"}

# RQL aggregate operator -> ftmq function (RQL calls the average `mean`)
RQL_FUNCTIONS = {
    "sum": "sum",
    "min": "min",
    "max": "max",
    "mean": "avg",
    "count": "count",
}
TO_RQL_FUNCTIONS = {v: k for k, v in RQL_FUNCTIONS.items()}
AGG_OPERATORS = set(RQL_FUNCTIONS) | {"aggregate"}

SELECT_OPERATOR = "select"


def _resolve_rql_field(field: str) -> tuple[str, str]:
    try:
        return _resolve_field(field)
    except QueryError:
        # any other bare name is a property, validated when the leaf is built
        return "P", field


def _rql_leaf(op: str, args: list[Any]) -> Expr:
    comparator = RQL_COMPARATORS.get(op)
    if comparator is None:
        raise QueryError(f"Unsupported RQL operator: `{op}`")
    field, value = args[0], args[1]
    family, key = _resolve_rql_field(field)
    if comparator in ("like", "ilike") and isinstance(value, str):
        # RQL uses `*` as the wildcard; ftmq `like`/`ilike` is substring-based
        value = value.replace("*", "")
    lookup = key if comparator == "eq" else f"{key}__{comparator}"
    return _FAMILIES[family](**{lookup: value})


def rql_to_expr(data: dict[str, Any]) -> Expr:
    """Convert a parsed RQL AST (`{"name": ..., "args": [...]}`) to an `Expr`."""
    if not isinstance(data, dict):
        raise QueryError(f"Invalid RQL expression: `{data}`")
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


def _metric_aggs(node: dict[str, Any], groups: tuple[Ref, ...]) -> list[Agg]:
    """One RQL metric call (`sum(prop, ...)`) -> `Agg` specs."""
    func = RQL_FUNCTIONS.get(node["name"])
    if func is None:
        raise QueryError(f"Unsupported RQL aggregate operator: `{node['name']}`")
    return [make_agg(func, ref_from_wire(str(field)), groups) for field in node["args"]]


def _node_aggs(node: dict[str, Any]) -> list[Agg]:
    """One `sum(p)` or `aggregate(group, ..., sum(p), ...)` node -> `Agg` specs."""
    if node["name"] == "aggregate":
        groups = tuple(
            ref_from_wire(str(a)) for a in node["args"] if not isinstance(a, dict)
        )
        aggs: list[Agg] = []
        for arg in node["args"]:
            if isinstance(arg, dict):
                aggs.extend(_metric_aggs(arg, groups))
        return aggs
    return _metric_aggs(node, ())


def _node_selection(node: dict[str, Any]) -> tuple[Ref, ...]:
    return tuple(ref_from_wire(str(arg)) for arg in node["args"])


def parse_rql(value: str) -> tuple[Expr | None, set[Agg], tuple[Ref, ...]]:
    """Parse an RQL string into a filter tree, aggregation specs and a projection.

    Raises:
        QueryError: If the RQL uses an unsupported operator or field.
    """
    data = pyrql.parse(value)
    if not data:
        return None, set(), ()
    aggs: set[Agg] = set()
    selection: tuple[Ref, ...] = ()
    filters: list[Any] = []
    for node in data["args"] if data["name"] == "and" else [data]:
        name = node.get("name") if isinstance(node, dict) else None
        if name == SELECT_OPERATOR:
            selection = _node_selection(node)
        elif name in AGG_OPERATORS:
            aggs.update(_node_aggs(node))
        else:
            filters.append(node)
    expr = combine(*(rql_to_expr(f) for f in filters), connector=AND)
    return expr, aggs, selection


def _leaf_to_rql(leaf: Leaf) -> dict[str, Any]:
    op = TO_RQL_OPERATORS.get(leaf.comparator)
    if op is None:
        raise QueryError(f"Comparator `{leaf.comparator}` is not expressible as RQL")
    value = leaf.value
    if op in ("in", "out"):
        value = tuple(sorted(str(v) for v in value))
    return {"name": op, "args": [leaf.wire, value]}


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
    """Agg specs -> bare `sum(p)` nodes, plus one `aggregate(...)` per shared `by`."""
    ungrouped: list[dict[str, Any]] = []
    grouped: dict[tuple[Ref, ...], list[dict[str, Any]]] = defaultdict(list)
    for agg in sorted(aggs, key=lambda a: (a.groups, a.func, a.key)):
        node: dict[str, Any] = {"name": TO_RQL_FUNCTIONS[agg.func], "args": [agg.key]}
        if agg.groups:
            grouped[agg.groups].append(node)
        else:
            ungrouped.append(node)
    nodes: list[dict[str, Any]] = list(ungrouped)
    for groups, metrics in grouped.items():
        nodes.append({"name": "aggregate", "args": [g.wire for g in groups] + metrics})
    return nodes


def to_rql(
    expr: Expr | None,
    aggs: Iterable[Agg] = (),
    selection: Iterable[Ref] = (),
) -> str:
    """Serialize a filter tree, aggregation specs and a projection to an RQL string.

    Raises:
        QueryError: If a filter leaf uses a comparator with no RQL equivalent
            (`null`, `startswith`, `endswith`, ...).
    """
    nodes: list[dict[str, Any]] = []
    if expr:
        filter_ast = expr_to_rql(expr)
        # flatten a top-level `and` filter so aggregations join as siblings
        if filter_ast.get("name") == "and":
            nodes.extend(filter_ast["args"])
        else:
            nodes.append(filter_ast)
    nodes.extend(_aggs_to_rql(aggs))
    fields = [ref.wire for ref in selection]
    if fields:
        nodes.append({"name": SELECT_OPERATOR, "args": fields})
    if not nodes:
        return ""
    if len(nodes) == 1:
        return str(pyrql.unparse(nodes[0]))
    return str(pyrql.unparse({"name": "and", "args": nodes}))
