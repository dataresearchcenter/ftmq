"""
[RQL](https://github.com/pjwerneck/pyrql) (Resource Query Language) bridge.

RQL is a URL-friendly query language of nestable named operators - e.g.
`and(eq(schema,Person),or(eq(properties.name,jane),eq(countries,de)))` - which
maps directly onto the ftmq `Expr` tree (`and`/`or`/`not` + comparison leaves).
Unlike the flat Aleph param grammar, RQL expresses arbitrary nesting, so this is
the way to carry a full `M & (P | G)` tree through a single string.

Field names follow the Aleph convention (`schema`, `dataset`, `properties.<name>`,
a `registry.groups` name, or `origin`); a bare name that matches none of those is
treated as an FtM property.
"""

from __future__ import annotations

from typing import Any

import pyrql  # type: ignore[import-untyped]

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


def parse_rql(value: str) -> Expr | None:
    """Parse an RQL query string into an `Expr` (or `None` if empty).

    Raises:
        QueryError: If the RQL uses an unsupported operator or field.
    """
    data = pyrql.parse(value)
    return rql_to_expr(data) if data else None


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


def to_rql(expr: Expr | None) -> str:
    """Serialize an `Expr` tree to an RQL query string.

    Raises:
        QueryError: If a leaf uses a comparator with no RQL equivalent (`null`,
            `startswith`, `endswith`, ...).
    """
    if expr is None or not expr:
        return ""
    return str(pyrql.unparse(expr_to_rql(expr)))
