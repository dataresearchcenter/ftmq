"""
The boolean expression tree and the `M` / `P` / `G` leaf constructors.

`Expr` nodes compose with `&`, `|`, `~` into arbitrary boolean trees; the
`M` (meta), `P` (property) and `G` (group) constructors turn `field__op=value`
kwargs into leaves of one family.
"""

from __future__ import annotations

from typing import Any, Iterator

from banal import hash_data
from followthemoney.proxy import EntityProxy

from ftmq.query.leaves import (
    Leaf,
    leaf_from_dict,
    make_context_leaf,
    make_group_leaf,
    make_meta_leaf,
    make_property_leaf,
)

AND = "AND"
OR = "OR"


class Expr:
    """A boolean node: a connector (`AND`/`OR`), an optional negation, and a
    list of children (nested `Expr` nodes and/or `Leaf` conditions)."""

    def __init__(
        self,
        *children: "Expr | Leaf",
        connector: str = AND,
        negated: bool = False,
    ) -> None:
        self.connector = connector
        self.negated = negated
        self.children: list[Expr | Leaf] = list(children)

    def __bool__(self) -> bool:
        return bool(self.children) or self.negated

    def _copy(self) -> "Expr":
        clone = Expr(connector=self.connector, negated=self.negated)
        clone.children = list(self.children)
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
            result = all(c.apply(entity) for c in self.children)
        return (not result) if self.negated else result

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

        Nested nodes that share the connector and are not negated are flattened
        (associativity) and children are sorted, so structurally-equivalent
        trees (e.g. built by different `where()` orderings) serialize
        identically and hash equal.

        Returns:
            A `{"and" | "or": [...], "not": bool}` mapping, round-trippable via
            [`from_dict`][ftmq.query.nodes.Expr.from_dict].
        """
        key = self.connector.lower()
        children: list[Any] = []
        for child in self.children:
            if isinstance(child, Expr):
                child_dict = child.to_dict()
                if not child.negated and child.connector == self.connector:
                    children.extend(child_dict[key])
                else:
                    children.append(child_dict)
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
    """Base for the `M`/`P`/`G` leaf constructors: parse `field__op=value`
    kwargs into leaves of one family, AND-combined."""

    @staticmethod
    def _make(key: str, value: Any) -> Leaf:
        raise NotImplementedError

    def __init__(self, **lookups: Any) -> None:
        super().__init__(connector=AND)
        for key, value in lookups.items():
            self.children.append(self._make(key, value))


class M(_FamilyExpr):
    """Meta-field conditions: dataset, schema, schemata, origin, id, ..."""

    @staticmethod
    def _make(key: str, value: Any) -> Leaf:
        return make_meta_leaf(key, value)


class P(_FamilyExpr):
    """Specific-property conditions, e.g. `P(name="Jane", amountEur__gte=1000)`."""

    @staticmethod
    def _make(key: str, value: Any) -> Leaf:
        return make_property_leaf(key, value)


class G(_FamilyExpr):
    """Property-type group conditions, e.g. `G(countries="de")`, `G(entities=id)`."""

    @staticmethod
    def _make(key: str, value: Any) -> Leaf:
        return make_group_leaf(key, value)


class C(_FamilyExpr):
    """Context / storage-column conditions, e.g. `C(origin="crawl")`,
    `C(first_seen__gte="2024-01")`."""

    @staticmethod
    def _make(key: str, value: Any) -> Leaf:
        return make_context_leaf(key, value)


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
