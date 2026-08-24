"""
The boolean expression tree and the `M` / `P` / `G` / `C` family constructors.

`Expr` nodes compose with `&`, `|`, `~` into arbitrary boolean trees; the
`M` (meta), `P` (property), `G` (group) and `C` (context) constructors turn
`field__op=value` kwargs into leaves of one family (and, called with a bare
field name, build `Ref`s).
"""

from __future__ import annotations

from typing import Any, Iterator, overload

from banal import hash_data
from followthemoney.proxy import EntityProxy

from ftmq.query.exceptions import QueryError
from ftmq.query.leaves import (
    Leaf,
    group_conjunction,
    leaf_from_dict,
    make_context_leaf,
    make_group_leaf,
    make_meta_leaf,
    make_property_leaf,
    row_scoped_groups,
)
from ftmq.query.refs import ContextRef, GroupRef, PropRef, Ref, make_meta_ref

AND = "AND"
OR = "OR"


def _normalize(
    children: "tuple[Expr | Leaf, ...]", connector: str
) -> "list[Expr | Leaf]":
    """Bring a node's children into canonical form: splice non-negated
    sub-groups of the same connector into this one, and drop children that
    already appear (both `Expr` and `Leaf` hash over their canonical
    serialization, so that is a structural identity).

    Both are boolean identities (associativity, and `a & a == a`), so a node
    built as `Query(P(name="x"), P(name="x"))` or by re-applying a filter in a
    chained `.where()` holds the condition once. Doing it here rather than in
    each serializer is what makes every surface - dict, rql, params, sql, and
    the in-memory evaluator - see the same deduplicated tree.
    """
    result: list[Expr | Leaf] = []
    seen: set[Expr | Leaf] = set()
    for child in children:
        items: list[Expr | Leaf]
        if (
            isinstance(child, Expr)
            and not child.negated
            and child.connector == connector
        ):
            # normalized already, so one level of splicing is enough
            items = child.children
        else:
            items = [child]
        for item in items:
            if item not in seen:
                seen.add(item)
                result.append(item)
    return result


class Expr:
    """A boolean node: a connector (`AND`/`OR`), an optional negation, and a
    list of children (nested `Expr` nodes and/or `Leaf` conditions).

    Children are canonicalized on construction (see
    [`_normalize`][ftmq.query.nodes._normalize]), so a node never holds a
    duplicate child or a nested group it could absorb.
    """

    def __init__(
        self,
        *children: "Expr | Leaf",
        connector: str = AND,
        negated: bool = False,
    ) -> None:
        self.connector = connector
        self.negated = negated
        self.children: list[Expr | Leaf] = _normalize(children, connector)

    def __bool__(self) -> bool:
        return bool(self.children) or self.negated

    def _copy(self) -> "Expr":
        clone = Expr(connector=self.connector, negated=self.negated)
        clone.children = list(self.children)  # already normalized
        return clone

    def _combine(self, other: "Expr", connector: str) -> "Expr":
        if not self:
            return other._copy()
        if not other:
            return self._copy()
        return Expr(self._copy(), other._copy(), connector=connector)

    def __and__(self, other: Any) -> "Expr":
        if not isinstance(other, Expr):
            return NotImplemented
        return self._combine(other, AND)

    def __or__(self, other: Any) -> "Expr":
        if not isinstance(other, Expr):
            return NotImplemented
        return self._combine(other, OR)

    def __invert__(self) -> "Expr":
        clone = self._copy()
        clone.negated = not self.negated
        return clone

    def apply(self, entity: EntityProxy) -> bool:
        """Evaluate the boolean expression against an entity.

        Args:
            entity: The entity to test.

        Returns:
            `True` if the entity matches this (possibly nested, possibly
            negated) tree of conditions.
        """
        if not self.children:
            result = True
        elif self.connector == OR:
            result = any(c.apply(entity) for c in self.children)
        else:
            result = self._apply_and(entity)
        return (not result) if self.negated else result

    def _apply_and(self, entity: EntityProxy) -> bool:
        """Evaluate a conjunction, with co-referring conditions sharing a value.

        `group_conjunction` marks a field's leaves as co-referring when they
        are all bounds, so `P(date__gte=a) & P(date__lt=b)` asks for *one* date
        inside the window - testing each bound separately would match an entity
        holding one date below the window and another above it. Everything else
        keeps its own per-leaf test.
        """
        exprs: list[Expr] = []
        leaves: list[Leaf] = []
        for child in self.children:
            (exprs if isinstance(child, Expr) else leaves).append(child)  # type: ignore[arg-type]
        if not all(c.apply(entity) for c in exprs):
            return False
        groups = group_conjunction(leaves)
        matched, joined = self._apply_row_scope(entity, groups)
        if not matched:
            return False
        for group in groups:
            if id(group) in joined:
                continue
            if len(group) == 1:
                if not group[0].apply(entity):
                    return False
            # the leaves of a multi-leaf group are bounds on one field, so any
            # of them reads the same values; they have to agree on one value
            elif not any(
                all(leaf.match(value) for leaf in group)
                for value in group[0].values(entity)
            ):
                return False
        return True

    @staticmethod
    def _apply_row_scope(
        entity: EntityProxy, groups: list[list[Leaf]]
    ) -> tuple[bool, set[int]]:
        """Test the conditions addressing distinct columns of one statement row
        against the entity's statements: one row has to satisfy all of them.

        Returns whether they matched, and the ids of the groups this settled so
        the caller skips them. It settles nothing unless the entity carries its
        statements and there is more than one such column to correlate - an
        entity read off a json stream has only the aggregated `context` dict,
        where the correlation between two columns is already lost, so there
        each condition is tested on its own as before.
        """
        row_groups = row_scoped_groups(groups)
        statements = getattr(entity, "statements", None)
        if len(row_groups) < 2 or statements is None:
            return True, set()
        row_leaves = [leaf for group in row_groups for leaf in group]
        matched = any(
            all(leaf.match_row(statement) for leaf in row_leaves)
            for statement in statements
        )
        return matched, {id(group) for group in row_groups}

    def iter_leaves(self, cls: type | None = None) -> Iterator[Leaf]:
        """Walk the tree and yield its leaf conditions.

        Args:
            cls: Optionally restrict to leaves of this class.

        Yields:
            Each matching leaf, depth-first.
        """
        for child in self.children:
            if isinstance(child, Expr):
                yield from child.iter_leaves(cls)
            elif cls is None or isinstance(child, cls):
                yield child

    def to_dict(self) -> dict[str, Any]:
        """Serialize the tree to a nested, canonically-ordered dict.

        The children are already flattened and deduplicated (see
        [`_normalize`][ftmq.query.nodes._normalize]); sorting them here makes
        structurally-equivalent trees (e.g. built by different `where()`
        orderings) serialize identically and hash equal.

        Returns:
            A `{"and" | "or": [...], "not": bool}` mapping, round-trippable via
            [`from_dict`][ftmq.query.nodes.Expr.from_dict].
        """
        key = self.connector.lower()
        children: list[Any] = []
        for child in self.children:
            if isinstance(child, Expr):
                children.append(child.to_dict())
            else:
                children.append({"leaf": child.field_dict()})
        children.sort(key=hash_data)
        data: dict[str, Any] = {key: children}
        if self.negated:
            data["not"] = True
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Expr":
        """Rebuild a tree from its [`to_dict`][ftmq.query.nodes.Expr.to_dict] form.

        Args:
            data: The nested mapping to deserialize.

        Returns:
            The reconstructed expression.
        """
        connector = OR if "or" in data else AND
        children: list[Expr | Leaf] = []
        for child in data.get(connector.lower(), []):
            if "leaf" in child:
                children.append(leaf_from_dict(child["leaf"]))
            else:
                children.append(cls.from_dict(child))
        return cls(*children, connector=connector, negated=bool(data.get("not")))

    def __hash__(self) -> int:
        # a within-process hash over a normalized serialization; like any
        # Python object it is not stable across processes (banal's hash_data
        # normalizes key/element order so equal trees hash equal in-process)
        return hash(hash_data(self.to_dict()))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Expr) and hash(self) == hash(other)

    def __repr__(self) -> str:
        return f"<Expr {self.to_dict()}>"


class _FamilyExpr(Expr):
    """Base for the `M`/`P`/`G`/`C` constructors.

    Called with `field__op=value` kwargs it builds leaves of one family,
    AND-combined. Called with a single positional field name it builds a
    [`Ref`][ftmq.query.refs.Ref] instead - the same field, no condition - which
    is what an aggregation projects over:

    ```python
    P(amountEur__gte=1000)          # a condition (an `Expr`)
    P("amountEur")                  # a field reference (a `Ref`)
    ```
    """

    @staticmethod
    def _make(key: str, value: Any) -> Leaf:
        raise NotImplementedError

    @staticmethod
    def _ref(field: str) -> Ref:
        raise NotImplementedError

    # a field name yields a `Ref`, not an instance of this class - which is
    # the point, and something mypy has no way to spell for `__new__`
    @overload
    def __new__(cls, field: str, /) -> Ref:  # type: ignore[misc]
        pass

    @overload
    def __new__(cls, **lookups: Any) -> "_FamilyExpr":
        pass

    def __new__(cls, *args: Any, **lookups: Any) -> Any:
        if args:
            if len(args) > 1 or lookups:
                raise QueryError(
                    f"`{cls.__name__}` takes either one field name (a reference) "
                    "or `field=value` lookups (a condition), not both"
                )
            # returning a foreign type from `__new__` skips `__init__`
            return cls._ref(args[0])
        return super().__new__(cls)

    def __init__(self, **lookups: Any) -> None:
        # built through `super().__init__`, not appended to, so the children go
        # through the same canonicalization as any other node
        super().__init__(*(self._make(k, v) for k, v in lookups.items()), connector=AND)


class M(_FamilyExpr):
    """Meta fields: `dataset`, `schema`, `schemata`, `id`, ... - `M(schema="Person")`
    as a condition, `M("dataset")` as a reference."""

    @staticmethod
    def _make(key: str, value: Any) -> Leaf:
        return make_meta_leaf(key, value)

    @staticmethod
    def _ref(field: str) -> Ref:
        return make_meta_ref(field)


class P(_FamilyExpr):
    """A specific FtM property: `P(name="Jane", amountEur__gte=1000)` as a
    condition, `P("amountEur")` as a reference."""

    @staticmethod
    def _make(key: str, value: Any) -> Leaf:
        return make_property_leaf(key, value)

    @staticmethod
    def _ref(field: str) -> Ref:
        return PropRef(field)


class G(_FamilyExpr):
    """A property-type group: `G(countries="de")` as a condition,
    `G("countries")` as a reference."""

    @staticmethod
    def _make(key: str, value: Any) -> Leaf:
        return make_group_leaf(key, value)

    @staticmethod
    def _ref(field: str) -> Ref:
        return GroupRef(field)


class C(_FamilyExpr):
    """A context / storage column: `C(origin="crawl")` as a condition,
    `C("origin")` as a reference."""

    @staticmethod
    def _make(key: str, value: Any) -> Leaf:
        return make_context_leaf(key, value)

    @staticmethod
    def _ref(field: str) -> Ref:
        return ContextRef(field)


def combine(*nodes: Expr, connector: str = AND) -> Expr | None:
    """Combine a series of nodes with a single connector, skipping empties.

    Args:
        *nodes: The `M` / `P` / `G` / `Expr` nodes to combine.
        connector: `AND` (default) or `OR`.

    Returns:
        The combined expression, or `None` if no non-empty node was passed.
    """
    result: Expr | None = None
    for node in nodes:
        if not node:
            continue
        if result is None:
            result = node
        elif connector == OR:
            result = result | node
        else:
            result = result & node
    return result
