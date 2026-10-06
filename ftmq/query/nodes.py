"""The boolean expression tree and the `M` / `P` / `G` / `C` family constructors."""

from __future__ import annotations

from typing import Any, Callable, Iterator, overload

from banal import hash_data
from followthemoney.proxy import EntityProxy

from ftmq.query.exceptions import QueryError
from ftmq.query.leaves import (
    Leaf,
    group_conjunction,
    leaf_from_dict,
    make_leaf,
    row_scoped_groups,
)
from ftmq.query.refs import ContextRef, GroupRef, PropRef, Ref, make_meta_ref

AND = "AND"
OR = "OR"


def _normalize(
    children: "tuple[Expr | Leaf, ...]", connector: str
) -> "list[Expr | Leaf]":
    """Splice non-negated same-connector sub-groups and drop duplicate children."""
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
    """A boolean node: an `AND` / `OR` connector, an optional negation and children.

    Children are `Expr` nodes or [`Leaf`][ftmq.query.leaves.Leaf] conditions,
    canonicalized on construction: non-negated sub-groups of the same connector
    are spliced in, duplicates dropped.
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

    def _combine(self, other: "Expr", connector: str) -> "Expr":
        # nodes are immutable, so they can be shared
        if not self:
            return other
        if not other:
            return self
        return Expr(self, other, connector=connector)

    def __and__(self, other: Any) -> "Expr":
        if not isinstance(other, Expr):
            return NotImplemented
        return self._combine(other, AND)

    def __or__(self, other: Any) -> "Expr":
        if not isinstance(other, Expr):
            return NotImplemented
        return self._combine(other, OR)

    def __invert__(self) -> "Expr":
        return Expr(*self.children, connector=self.connector, negated=not self.negated)

    def apply(self, entity: EntityProxy) -> bool:
        """Evaluate the tree against an entity.

        Args:
            entity: The entity to test.

        Returns:
            Whether the entity matches.
        """
        if not self.children:
            result = True
        elif self.connector == OR:
            result = any(c.apply(entity) for c in self.children)
        else:
            result = self._apply_and(entity)
        return (not result) if self.negated else result

    def _apply_and(self, entity: EntityProxy) -> bool:
        """Evaluate a conjunction; co-referring leaves must hold of one value or row."""
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
            # bounds on one field: all of them must hold of the same value
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
        """Match row-scoped groups against one statement: `(matched, settled ids)`."""
        row_groups = row_scoped_groups(groups)
        statements = getattr(entity, "statements", None)
        # nothing to correlate (or no rows): the caller tests each group alone
        if len(row_groups) < 2 or statements is None:
            return True, set()
        row_leaves = [leaf for group in row_groups for leaf in group]
        matched = any(
            all(leaf.match_row(statement) for leaf in row_leaves)
            for statement in statements
        )
        return matched, {id(group) for group in row_groups}

    def iter_leaves(self, cls: type | None = None) -> Iterator[Leaf]:
        """Yield the tree's leaf conditions, depth-first.

        Args:
            cls: Optionally restrict to leaves of this class.

        Yields:
            Each matching leaf.
        """
        for child in self.children:
            if isinstance(child, Expr):
                yield from child.iter_leaves(cls)
            elif cls is None or isinstance(child, cls):
                yield child

    def to_dict(self) -> dict[str, Any]:
        """Serialize the tree to a nested dict, children in canonical order.

        Structurally equal trees serialize, and hash, identically.

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
        # over the canonical serialization; within-process only
        return hash(hash_data(self.to_dict()))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Expr) and hash(self) == hash(other)

    def __repr__(self) -> str:
        return f"<Expr {self.to_dict()}>"


class _FamilyExpr(Expr):
    """Base of `M` / `P` / `G` / `C`: lookups build a condition, a name a `Ref`."""

    family: str = ""

    # `__new__` returning a non-instance (a `Ref`) is not expressible to mypy
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
            return _REFS[cls.family](args[0])
        return super().__new__(cls)

    def __init__(self, **lookups: Any) -> None:
        leaves = (make_leaf(self.family, k, v) for k, v in lookups.items())
        super().__init__(*leaves, connector=AND)


class M(_FamilyExpr):
    """Meta fields (`dataset`, `schema`, `schemata`, `id`, ...): `M(schema="Person")`
    as a condition, `M("dataset")` as a reference."""

    family = "M"


class P(_FamilyExpr):
    """A specific FtM property: `P(name="Jane", amountEur__gte=1000)` as a
    condition, `P("amountEur")` as a reference."""

    family = "P"


class G(_FamilyExpr):
    """A property-type group: `G(countries="de")` as a condition,
    `G("countries")` as a reference."""

    family = "G"


class C(_FamilyExpr):
    """A context / storage column: `C(origin="crawl")` as a condition,
    `C("origin")` as a reference."""

    family = "C"


_REFS: dict[str, Callable[[str], Ref]] = {
    "M": make_meta_ref,
    "P": PropRef,
    "G": GroupRef,
    "C": ContextRef,
}


def combine(*nodes: Expr, connector: str = AND) -> Expr | None:
    """Combine a series of nodes with a single connector, skipping empties.

    Args:
        *nodes: The nodes to combine.
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
