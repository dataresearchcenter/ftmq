"""
The Aleph / OpenAleph URL-param grammar: a bidirectional bridge between a
`Query` filter tree and the `filter:` / `exclude:` / `empty:` param convention
used by `openaleph_search.SearchQueryParser`.

Only the *filter* half lives here (the mapping between the boolean tree and the
param keys). `sort` / `limit` / `offset` are query-level concerns handled by
`Query.to_params` / `Query.from_params`.

The param model is flat (AND across keys, OR within a key, `exclude:` and
`empty:` for negation / absence), so:

- `params_to_expr` is total and always yields a flat AND-of-leaves.
- `expr_to_params` is defined on that flat subset and raises `QueryError` for a
  cross-field OR or a negated group.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any
from urllib.parse import quote, unquote

from followthemoney.types import registry

from ftmq.query.exceptions import QueryError
from ftmq.query.leaves import Leaf, PropertyLeaf
from ftmq.query.nodes import OR, Expr, G, M, P, combine

# Aleph meta filter keys -> ftmq meta field (some upstream keys are aliased)
ALEPH_META = {
    "id": "id",
    "_id": "id",
    "entity_id": "entity_id",
    "canonical_id": "canonical_id",
    "dataset": "dataset",
    "datasets": "dataset",
    "collection_id": "dataset",
    "collections": "dataset",
    "schema": "schema",
    "schemata": "schemata",
    "origin": "origin",
}
RANGE_OPS = ("gte", "gt", "lte", "lt")
_FAMILIES = {"M": M, "P": P, "G": G}


def normalize_multidict(args: Any) -> dict[str, list[str]]:
    """Coerce params into a plain `dict[str, list[str]]`.

    Args:
        args: A werkzeug `MultiDict`, a plain dict, or a dict of lists.

    Returns:
        A mapping of each key to its list of string values.
    """
    items: dict[str, list[str]] = defaultdict(list)
    if hasattr(args, "lists"):  # werkzeug MultiDict
        for key, values in args.lists():
            items[key].extend(str(v) for v in values)
    elif hasattr(args, "items"):
        for key, value in args.items():
            if isinstance(value, (list, tuple, set)):
                items[key].extend(str(v) for v in value)
            else:
                items[key].append(str(value))
    return dict(items)


def _aleph_field(leaf: Leaf) -> str:
    if isinstance(leaf, PropertyLeaf):
        return f"properties.{leaf.key}"
    # group leaves use the group name; meta leaves use their own key
    return leaf.key


def _collect_terms(expr: Expr) -> list[tuple[Leaf, bool]]:
    """Flatten an Aleph-expressible flat-AND tree into `(leaf, inverted)`
    pairs. A negated sub-tree is only allowed when it wraps a single leaf
    (`~P(x=1)` -> exclude); anything else raises."""
    if not expr:
        return []
    if expr.negated:
        leaves = list(expr.iter_leaves())
        if len(leaves) == 1:
            return [(leaves[0], True)]
        raise QueryError("Negated group is not expressible as Aleph params")
    if expr.connector == OR:
        raise QueryError("OR queries are not expressible as Aleph params")
    terms: list[tuple[Leaf, bool]] = []
    for child in expr.children:
        if isinstance(child, Expr):
            terms.extend(_collect_terms(child))
        else:
            terms.append((child, False))
    return terms


def _leaf_to_param(leaf: Leaf, inverted: bool) -> tuple[str, str, list[str]]:
    op = str(leaf.comparator)
    field = _aleph_field(leaf)
    value = leaf.value
    if inverted:
        if op == "eq":
            return "exclude:", field, [str(value)]
        if op == "in":
            return "exclude:", field, sorted(str(v) for v in value)
        raise QueryError(f"Cannot invert comparator `{op}` for Aleph params")
    if op == "eq":
        return "filter:", field, [str(value)]
    if op == "in":
        return "filter:", field, sorted(str(v) for v in value)
    if op in RANGE_OPS:
        return "filter:", f"{op}:{field}", [str(value)]
    if op == "not":
        return "exclude:", field, [str(value)]
    if op == "not_in":
        return "exclude:", field, sorted(str(v) for v in value)
    if op == "null":
        if leaf.value:
            return "empty:", field, [""]
        raise QueryError("null=False is not expressible as Aleph params")
    raise QueryError(f"Comparator `{op}` is not expressible as Aleph params")


def _resolve_field(rest: str) -> tuple[str, str]:
    """Map an Aleph filter field to a (family, ftmq-key) pair."""
    if rest in ALEPH_META:
        return "M", ALEPH_META[rest]
    if rest.startswith("properties."):
        return "P", rest[len("properties.") :]
    if rest in registry.groups:
        return "G", rest
    raise QueryError(f"Unknown Aleph filter field: `{rest}`")


def _param_to_node(prefix: str, rest: str, values: list[str]) -> Expr:
    op = None
    for candidate in RANGE_OPS:
        if rest.startswith(f"{candidate}:"):
            op = candidate
            rest = rest[len(candidate) + 1 :]
            break
    family, field = _resolve_field(rest)
    value: Any
    if prefix == "empty:":
        key, value = f"{field}__null", True
    elif prefix == "exclude:":
        if len(values) > 1:
            key, value = f"{field}__not_in", list(values)
        else:
            key, value = f"{field}__not", values[0]
    elif op is not None:
        key, value = f"{field}__{op}", values[0]
    elif len(values) > 1:
        key, value = f"{field}__in", list(values)
    else:
        key, value = field, values[0]
    return _FAMILIES[family](**{key: value})


def expr_to_params(expr: Expr | None) -> dict[str, list[str]]:
    """Project a filter tree to Aleph `filter:` / `exclude:` / `empty:` params.

    Args:
        expr: The filter tree (or `None`).

    Returns:
        The Aleph param mapping.

    Raises:
        QueryError: If the tree is not Aleph-expressible (a cross-field `OR`, or
            a negated multi-leaf group).
    """
    params: dict[str, list[str]] = defaultdict(list)
    if expr:
        for leaf, inverted in _collect_terms(expr):
            prefix, key, values = _leaf_to_param(leaf, inverted)
            params[f"{prefix}{key}"].extend(values)
    return dict(params)


def params_to_expr(items: dict[str, list[str]]) -> Expr | None:
    """Build a filter tree from Aleph params (non-filter keys are ignored).

    Args:
        items: A normalized param mapping (see
            [`normalize_multidict`][ftmq.query.aleph.normalize_multidict]).

    Returns:
        The flat AND-of-leaves filter tree, or `None` if there are no filters.
    """
    nodes: list[Expr] = []
    for key, values in items.items():
        for prefix in ("filter:", "exclude:", "empty:"):
            if key.startswith(prefix):
                nodes.append(_param_to_node(prefix, key[len(prefix) :], values))
                break
    return combine(*nodes) if nodes else None


def params_to_string(params: dict[str, list[str]]) -> str:
    """Render an Aleph param mapping as a URL query string.

    Keys are sorted for deterministic output; value order within a key is
    preserved (multi-field `sort` priority must not be reordered).

    Args:
        params: The param mapping.

    Returns:
        A `key=value&...` string with url-encoded values, sorted by key.
    """
    parts = []
    for key in sorted(params):
        for value in params[key]:
            parts.append(f"{key}={quote(str(value))}")
    return "&".join(parts)


def string_to_params(value: str) -> dict[str, list[str]]:
    """Parse an Aleph URL query string into a param mapping.

    Args:
        value: A `key=value&...` query string.

    Returns:
        A mapping of each key to its list of url-decoded values.
    """
    items: dict[str, list[str]] = defaultdict(list)
    for part in value.split("&"):
        if not part:
            continue
        key, _, val = part.partition("=")
        items[key].append(unquote(val))
    return dict(items)
