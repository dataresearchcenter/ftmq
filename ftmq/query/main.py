from __future__ import annotations

from itertools import islice
from typing import TYPE_CHECKING, Any, Iterable, Self, cast

from banal import ensure_list, hash_data
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry

from ftmq.query.aggregations import (
    A,
    Agg,
    Aggregator,
    aggregations_from_dict,
    aggregations_to_dict,
)
from ftmq.query.aleph import (
    aggregations_to_params,
    expr_to_params,
    normalize_multidict,
    params_to_aggregations,
    params_to_expr,
    params_to_string,
    string_to_params,
)
from ftmq.query.exceptions import QueryError
from ftmq.query.leaves import (
    ContextLeaf,
    DatasetLeaf,
    GroupLeaf,
    IdLeaf,
    Leaf,
    PropertyLeaf,
    SchemaLeaf,
    SchemataLeaf,
)
from ftmq.query.nodes import Expr, combine
from ftmq.query.rql import parse_rql
from ftmq.query.rql import to_rql as serialize_rql
from ftmq.query.sql import Sql, SqlSource
from ftmq.types import EntityProxies
from ftmq.util import prop_is_numeric

if TYPE_CHECKING:
    from sqlalchemy import Select


def _make_slice(limit: int | None, offset: int | None) -> slice | None:
    if limit is None and not offset:
        return None
    start = offset or 0
    stop = (start + limit) if limit is not None else None
    return slice(start, stop)


class Sort:
    """An ordering over one or more entity properties."""

    def __init__(self, values: Iterable[str], ascending: bool | None = True) -> None:
        self.values = tuple(values)
        self.ascending = ascending

    def apply(self, entity: EntityProxy) -> tuple[Any, ...]:
        """Compute the sort key for an entity.

        Args:
            entity: The entity to read the sort values from.

        Returns:
            A tuple of the entity's values for the sort properties (numeric
            properties are cast to numbers).
        """
        values: tuple[Any, ...] = tuple()
        for v in self.values:
            p_values: list[Any] = entity.get(v, quiet=True) or []
            if prop_is_numeric(entity.schema, v):
                p_values = list(map(registry.number.to_number, p_values))
            values = values + tuple(p_values)
        return values

    def apply_iter(self, entities: EntityProxies) -> EntityProxies:
        """Sort a stream of entities.

        Args:
            entities: The entities to sort.

        Yields:
            The entities in sorted order.
        """
        yield from sorted(
            entities, key=lambda x: self.apply(x), reverse=not self.ascending
        )

    def serialize(self) -> list[str]:
        """Serialize to a list of field names, descending fields prefixed `-`.

        Returns:
            The ordered field names, e.g. `["name"]` or `["-date"]`.
        """
        if self.ascending:
            return list(self.values)
        return [f"-{v}" for v in self.values]


class Query:
    """
    A filter over FtM entities, built from composable `M` / `P` / `G` nodes.

    Examples:
        ```python
        from ftmq import Query, M, P, G

        q = Query().where(M(schema="Person"), P(name__ilike="jane%"))
        q = q.where(G(countries="de") | G(countries="at"))
        q = q.order_by("name")[:10]
        ```
    """

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Query):
            return NotImplemented
        return hash(self) == hash(other)

    def __init__(
        self,
        *nodes: Expr,
        q: Expr | None = None,
        aggregations: Iterable[Agg] | None = None,
        aggregator: Aggregator | None = None,
        sort: Sort | None = None,
        slice: slice | None = None,
    ):
        self.q: Expr | None = q if q is not None else combine(*nodes)
        self.aggregations: set[Agg] = set(aggregations or [])
        self.aggregator = aggregator
        self.sort = sort
        self.slice = slice

    def __getitem__(self, value: Any) -> Self:
        """
        Implement list-like slicing. No negative values allowed.

        Examples:
            >>> q[1]
            # 2nd element (0-index)
            >>> q[:10]
            # first 10 elements
            >>> q[10:20]
            # next 10 elements

        Returns:
            The updated `Query` instance
        """
        if isinstance(value, int):
            if value < 0:
                raise QueryError(f"Invalid slicing: `{value}`")
            return self._chain(slice=slice(value, value + 1))
        if isinstance(value, slice):
            if value.step is not None:
                raise QueryError(f"Invalid slicing: `{value}`")
            return self._chain(slice=value)
        raise NotImplementedError

    def __bool__(self) -> bool:
        """
        Detect if any filter, ordering or slicing is defined

        Examples:
            >>> bool(Query())
            False
            >>> bool(Query().where(M(dataset="my_dataset")))
            True
        """
        return bool(self.to_dict())

    def __hash__(self) -> int:
        """
        Generate a unique key of the current state, useful for caching.

        Like any Python object this is a within-process hash (not stable
        across processes); `hash_data` normalizes ordering so equal queries
        hash equal.
        """
        return hash(hash_data(self.to_dict()))

    def _chain(self, **kwargs: Any) -> Self:
        data: dict[str, Any] = dict(
            q=self.q,
            aggregations=self.aggregations,
            aggregator=self.aggregator,
            sort=self.sort,
            slice=self.slice,
        )
        data.update(kwargs)
        return self.__class__(**data)

    # --- filter accessors (tree-walking collectors) ------------------------

    @property
    def _leaves(self) -> list[Leaf]:
        return list(self.q.iter_leaves()) if self.q else []

    @property
    def limit(self) -> int | None:
        """
        The current limit (inferred from a slice)
        """
        if self.slice is None:
            return None
        start, stop = self.slice.start, self.slice.stop
        if start and stop:
            return int(stop) - int(start)
        return None if stop is None else int(stop)

    @property
    def offset(self) -> int | None:
        """
        The current offset (inferred from a slice)

        A start-less slice (`q[:10]`) reports offset `0`, so it serializes and
        round-trips identically to `q[0:10]`.
        """
        if self.slice is None:
            return None
        return int(self.slice.start or 0)

    @property
    def sql(self) -> "Sql":
        """
        An adapter of this query for sql interfaces, against the default
        nomenklatura statement table. For a custom / extended table pass a
        [`SqlSource`][ftmq.query.sql.SqlSource] to [`compile`][ftmq.Query.compile] or
        build `Sql(query, source)` directly.
        """
        return Sql(self)

    def compile(self, source: "SqlSource | None" = None) -> "Select[Any]":
        """
        Compile this query to a SQLAlchemy `Select` of statements against a
        [`SqlSource`][ftmq.query.sql.SqlSource] (a store's table descriptor).

        Args:
            source: The SQL source to compile against (default: the base
                nomenklatura statement table).

        Returns:
            The statements `Select`.
        """
        return Sql(self, source).statements

    @property
    def ids(self) -> set[IdLeaf]:
        """
        The current id filters
        """
        return {f for f in self._leaves if isinstance(f, IdLeaf)}

    @property
    def datasets(self) -> set[DatasetLeaf]:
        """
        The current dataset filters
        """
        return {f for f in self._leaves if isinstance(f, DatasetLeaf)}

    @property
    def dataset_names(self) -> set[str]:
        """
        The names of the current filtered datasets
        """
        names: set[str] = set()
        for f in self.datasets:
            names.update(ensure_list(f.value))
        return names

    @property
    def schemata(self) -> set[SchemaLeaf]:
        """
        The current schema filters
        """
        return {f for f in self._leaves if isinstance(f, SchemaLeaf)}

    @property
    def schemata_names(self) -> set[str]:
        """
        The names of the current filtered schemas

        Exact `schema` leaves contribute their name; `schemata` (is-a) leaves
        expand to the schema plus its non-abstract descendants.
        """
        names: set[str] = set()
        for f in self._leaves:
            if isinstance(f, SchemataLeaf):
                for schema in f.schemata:
                    names.add(schema.name)
                    names.update(d.name for d in schema.descendants if not d.abstract)
            elif isinstance(f, SchemaLeaf):
                names.update(ensure_list(f.value))
        return names

    @property
    def context(self) -> set[ContextLeaf]:
        """
        The current context filters (the `C` family, e.g. `origin`)
        """
        return {f for f in self._leaves if isinstance(f, ContextLeaf)}

    @property
    def countries(self) -> set[str]:
        """
        The current filtered countries
        """
        names: set[str] = set()
        for f in self._leaves:
            if isinstance(f, GroupLeaf) and f.key == "countries":
                names.update(ensure_list(f.value))
        return names

    @property
    def groups(self) -> set[GroupLeaf]:
        """
        The current property groups lookup filters
        """
        return {f for f in self._leaves if isinstance(f, GroupLeaf)}

    @property
    def properties(self) -> set[PropertyLeaf]:
        """
        The current property lookup filters
        """
        return {f for f in self._leaves if isinstance(f, PropertyLeaf)}

    # --- serialization -----------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """
        Lossless nested-tree representation of the current object.

        Example:
            ```python
            q = Query().where(M(dataset__in=["d1", "d2"]))
            q = q.where(P(name="Jane") | P(name__ilike="j%"))
            data = q.to_dict()
            assert Query.from_dict(data).to_dict() == data
            ```
        """
        data: dict[str, Any] = {}
        if self.q:
            data["q"] = self.q.to_dict()
        if self.sort:
            data["order_by"] = self.sort.serialize()
        if self.slice:
            data["limit"] = self.limit
            data["offset"] = self.offset
        if self.aggregations:
            data["aggregations"] = aggregations_to_dict(self.aggregations)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Rebuild a `Query` from its [`to_dict`][ftmq.Query.to_dict] output."""
        q = Expr.from_dict(data["q"]) if data.get("q") else None
        sort = None
        if data.get("order_by"):
            values = list(data["order_by"])
            ascending = not (values and str(values[0]).startswith("-"))
            sort = Sort(
                values=[str(v).lstrip("-") for v in values], ascending=ascending
            )
        slice_ = _make_slice(data.get("limit"), data.get("offset"))
        aggregations = None
        if data.get("aggregations"):
            aggregations = aggregations_from_dict(data["aggregations"])
        return cls(q=q, sort=sort, slice=slice_, aggregations=aggregations)

    def to_params(self) -> dict[str, list[str]]:
        """
        Project to an Aleph-style filter param dict (`filter:` / `exclude:` /
        `empty:` keys, `metric:` / `facet` aggregation keys, plus `sort` /
        `limit` / `offset`).

        Raises `QueryError` for queries outside the flat Aleph-expressible
        subset (cross-field OR, negated groups).
        """
        params = {k: list(v) for k, v in expr_to_params(self.q).items()}
        if self.aggregations:
            params.update(aggregations_to_params(self.aggregations))
        if self.sort:
            params["sort"] = [
                f"{v[1:]}:desc" if v.startswith("-") else f"{v}:asc"
                for v in self.sort.serialize()
            ]
        if self.slice:
            if self.offset:
                params["offset"] = [str(self.offset)]
            if self.limit is not None:
                params["limit"] = [str(self.limit)]
        return params

    @classmethod
    def from_params(cls, args: Any) -> Self:
        """Build a `Query` from an Aleph-style param dict / MultiDict."""
        items = normalize_multidict(args)
        q = params_to_expr(items)
        aggregations = params_to_aggregations(items) or None
        sort = None
        if items.get("sort"):
            svalues: list[str] = []
            ascending = True
            for value in items["sort"]:
                field, _, direction = value.partition(":")
                svalues.append(field)
                ascending = direction != "desc"
            sort = Sort(values=svalues, ascending=ascending)
        slice_ = None
        if "limit" in items or "offset" in items:
            offset = int((items.get("offset") or ["0"])[0] or 0)
            _limit = items.get("limit")
            limit = int(_limit[0]) if _limit else None
            slice_ = _make_slice(limit, offset)
        return cls(q=q, sort=sort, slice=slice_, aggregations=aggregations)

    def to_string(self) -> str:
        """
        Project to an Aleph URL query string, e.g.
        `filter:properties.name=Jane&filter:schemata=LegalEntity`.
        """
        return params_to_string(self.to_params())

    @classmethod
    def from_string(cls, value: str) -> Self:
        """Build a `Query` from an Aleph URL query string."""
        return cls.from_params(string_to_params(value))

    @classmethod
    def from_rql(cls, value: str) -> Self:
        """Build a `Query` from an [RQL](https://github.com/pjwerneck/pyrql) string.

        Unlike the flat Aleph grammar, RQL expresses arbitrary `& | ~` nesting,
        e.g. `and(eq(schema,Person),or(eq(properties.name,jane),eq(countries,de)))`,
        and carries aggregations via its `sum` / `aggregate(...)` operators.
        """
        expr, aggregations = parse_rql(value)
        return cls(q=expr, aggregations=aggregations)

    def to_rql(self) -> str:
        """Serialize the filter tree and aggregations to an
        [RQL](https://github.com/pjwerneck/pyrql) string.

        RQL is the only string surface that preserves arbitrary `& | ~` nesting
        (unlike the flat Aleph params) and carries aggregations losslessly, so it
        is the way to hand a full query to another HTTP-like connector. Raises
        `QueryError` for a comparator with no RQL equivalent (`null`,
        `startswith`, `endswith`, ...).
        """
        return serialize_rql(self.q, self.aggregations)

    # --- building ----------------------------------------------------------

    def where(self, *nodes: Expr) -> Self:
        """
        AND another set of `M` / `P` / `G` nodes into the current `Query`.

        Example:
            ```python
            q = Query().where(M(schema="Payment"), P(date__gte="2024-10"))
            q = q.where(G(countries="de") | G(countries="at"))
            ```

        Args:
            *nodes: `M` / `P` / `G` nodes (optionally composed with `&`/`|`/`~`)

        Returns:
            The updated `Query` instance
        """
        new = combine(*nodes)
        if new is None:
            return self._chain()
        q = new if self.q is None else (self.q & new)
        return self._chain(q=q)

    def order_by(self, *values: str, ascending: bool | None = True) -> Self:
        """
        Add or update the current sorting.

        Args:
            *values: Fields to order by
            ascending: Ascending or descending

        Returns:
            The updated `Query` instance.
        """
        self.sort = Sort(values=values, ascending=ascending)
        return self._chain()

    def aggregate(self, *nodes: A) -> Self:
        """Add aggregation projections to the query.

        Example:
            ```python
            from ftmq import Query, M, A

            q = Query().where(M(schema="Payment")).aggregate(
                A(sum="amountEur", by="beneficiary"),
                A(avg="amountEur"),
            )
            ```

        Args:
            *nodes: `A` nodes, e.g. `A(sum="amountEur", by="beneficiary")`.

        Returns:
            The updated `Query` instance.
        """
        aggs = set(self.aggregations)
        for node in nodes:
            aggs.update(node.aggs)
        return self._chain(aggregations=aggs)

    def get_aggregator(self) -> Aggregator:
        """Build an in-memory `Aggregator` from the query's aggregation specs.

        Returns:
            A fresh accumulator over this query's aggregations.
        """
        return Aggregator(self.aggregations)

    # --- execution ---------------------------------------------------------

    def apply(self, entity: EntityProxy) -> bool:
        """
        Test if a entity matches the current `Query` instance.
        """
        if self.q is None:
            return True
        return self.q.apply(entity)

    def apply_iter(self, entities: EntityProxies) -> EntityProxies:
        """
        Apply the current `Query` instance to a generator of entities and return
        a generator of filtered entities

        Example:
            ```python
            entities = [...]
            q = Query().where(M(dataset="my_dataset"), M(schema="Company"))
            for entity in q.apply_iter(entities):
                assert entity.schema.name == "Company"
            ```

        Yields:
            A generator of `EntityProxy` or a sub-type
        """
        if not self:
            yield from entities
            return

        entities = (e for e in entities if self.apply(e))
        if self.sort:
            entities = self.sort.apply_iter(entities)
        if self.slice:
            entities = islice(
                entities, self.slice.start, self.slice.stop, self.slice.step
            )
        if self.aggregations:
            self.aggregator = self.get_aggregator()
            entities = self.aggregator.apply(cast(Any, entities))
        yield from entities
