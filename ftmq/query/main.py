from __future__ import annotations

from itertools import islice
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Self, cast

from banal import ensure_list, hash_data
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry

from ftmq.query.aggregations import (
    DEFAULT_FACET_SIZE,
    A,
    Agg,
    Aggregator,
    FacetOrder,
    aggregations_from_dict,
    aggregations_to_dict,
    groupers,
    make_facet_order,
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
from ftmq.query.leaves import DatasetLeaf, Leaf, SchemaLeaf, SchemataLeaf
from ftmq.query.nodes import Expr, combine
from ftmq.query.refs import GroupRef, PropRef, Ref, ref_from_wire
from ftmq.query.rql import parse_rql
from ftmq.query.rql import to_rql as serialize_rql
from ftmq.query.sql import Sql, SqlSource
from ftmq.types import EntityProxies

if TYPE_CHECKING:
    from sqlalchemy import Select


def _single(items: dict[str, list[str]], key: str) -> str | None:
    """The one value of a param, `None` if absent."""
    values = items.get(key)
    if not values:
        return None
    if len(values) > 1:
        raise QueryError(f"`{key}` takes a single value: `{values}`")
    return values[0]


def _make_slice(limit: int | None, offset: int | None) -> slice | None:
    if limit is None and not offset:
        return None
    start = offset or 0
    stop = (start + limit) if limit is not None else None
    return slice(start, stop)


class Sort:
    """A single-property ordering: `Sort(P("date"))`."""

    def __init__(self, ref: Ref, ascending: bool = True) -> None:
        if not isinstance(ref, PropRef):
            raise QueryError(
                f"Invalid sort field: `{getattr(ref, 'wire', ref)}` - only a "
                'property is sortable, e.g. `P("date")`'
            )
        self.ref = ref
        self.ascending = ascending

    def apply(self, entity: EntityProxy) -> tuple[Any, ...]:
        """The entity's values of the sort property, numeric ones as numbers."""
        values: list[Any] = list(self.ref.values(entity))
        if self.ref.is_numeric:
            values = [registry.number.to_number(v) for v in values]
        return tuple(values)

    def apply_iter(self, entities: EntityProxies) -> EntityProxies:
        """Sort a stream of entities, those without the property last."""
        keyed = [(self.apply(e), e) for e in entities]
        present = [(k, e) for k, e in keyed if k]
        present.sort(key=lambda x: x[0], reverse=not self.ascending)
        yield from (e for _, e in present)
        yield from (e for k, e in keyed if not k)

    def serialize(self) -> str:
        """The field's wire spelling, prefixed `-` when descending."""
        return self.ref.wire if self.ascending else f"-{self.ref.wire}"

    @classmethod
    def deserialize(cls, value: str) -> Self:
        """Rebuild from `serialize` output."""
        ascending = not value.startswith("-")
        return cls(ref_from_wire(value.removeprefix("-")), ascending=ascending)


class Query:
    """A query over FtM entities: a filter tree of `M` / `P` / `G` / `C` nodes.

    Examples:
        ```python
        from ftmq import Query, M, P, G

        q = Query().where(M(schema="Person"), P(name__ilike="jane%"))
        q = q.where(G(countries="de") | G(countries="at"))
        q = q.order_by(P("name"))[:10]
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
        sort: Sort | None = None,
        slice: slice | None = None,
        selection: Iterable[Ref] | None = None,
        facet_sort: FacetOrder | None = None,
        facet_sizes: Mapping[Ref, int] | None = None,
    ):
        self.q: Expr | None = q if q is not None else combine(*nodes)
        self.aggregations: set[Agg] = set(aggregations or [])
        self.aggregator: Aggregator | None = None
        self.sort = sort
        self.slice = slice
        self.selection: tuple[Ref, ...] = tuple(sorted(set(selection or ())))
        if facet_sort is not None and not any(
            facet_sort.orders(agg) and agg.groups for agg in self.aggregations
        ):
            raise QueryError(
                f"Invalid facet sort: `{facet_sort.wire}` - not a grouped "
                "aggregation of the query"
            )
        self.facet_sort = facet_sort
        facets = groupers(self.aggregations)
        for ref, size in (facet_sizes or {}).items():
            if ref not in facets:
                raise QueryError(
                    f"Invalid facet size: `{ref.wire}` - not a facet of the query"
                )
            if not isinstance(size, int) or size < 1:
                raise QueryError(f"Invalid facet size for `{ref.wire}`: `{size}`")
        self.facet_sizes: dict[Ref, int] = dict(sorted((facet_sizes or {}).items()))

    def __getitem__(self, value: Any) -> Self:
        """Slice like a list; no negative values or steps.

        Examples:
            >>> q[1]
            # 2nd element (0-index)
            >>> q[:10]
            # first 10 elements
            >>> q[10:20]
            # next 10 elements

        Returns:
            The updated `Query` instance.
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
        """Whether anything is set: filter, aggregation, projection, sort or slice.

        Examples:
            >>> bool(Query())
            False
            >>> bool(Query().where(M(dataset="my_dataset")))
            True
        """
        return bool(self.to_dict())

    def __hash__(self) -> int:
        """A within-process cache key; equal queries hash equal."""
        return hash(hash_data(self.to_dict()))

    def _chain(self, **kwargs: Any) -> Self:
        data: dict[str, Any] = dict(
            q=self.q,
            aggregations=self.aggregations,
            sort=self.sort,
            slice=self.slice,
            selection=self.selection,
            facet_sort=self.facet_sort,
            facet_sizes=self.facet_sizes,
        )
        data.update(kwargs)
        return self.__class__(**data)

    @property
    def _leaves(self) -> list[Leaf]:
        return list(self.q.iter_leaves()) if self.q else []

    @property
    def limit(self) -> int | None:
        """The limit, inferred from the slice."""
        if self.slice is None:
            return None
        start, stop = self.slice.start, self.slice.stop
        if start and stop:
            return int(stop) - int(start)
        return None if stop is None else int(stop)

    @property
    def offset(self) -> int | None:
        """The offset, inferred from the slice (`0` for `q[:10]`)."""
        if self.slice is None:
            return None
        return int(self.slice.start or 0)

    @property
    def sql(self) -> "Sql":
        """A [`Sql`][ftmq.query.sql.Sql] adapter against the default statement table.

        For another table use [`compile`][ftmq.Query.compile].
        """
        return Sql(self)

    def compile(self, source: "SqlSource | None" = None) -> "Select[Any]":
        """Compile to a SQLAlchemy `Select` of statements.

        Args:
            source: The [`SqlSource`][ftmq.query.sql.SqlSource] to compile against
                (default: the nomenklatura statement table).

        Returns:
            The statements `Select`.
        """
        return Sql(self, source).statements

    @property
    def dataset_names(self) -> set[str]:
        """The dataset names any filter leaf refers to, ignoring polarity."""
        names: set[str] = set()
        for f in self._leaves:
            if isinstance(f, DatasetLeaf):
                names.update(ensure_list(f.value))
        return names

    @property
    def schemata_names(self) -> set[str]:
        """The schema names any filter leaf refers to, ignoring polarity.

        A `schemata` (is-a) leaf expands to its non-abstract descendants.
        """
        names: set[str] = set()
        for f in self._leaves:
            if isinstance(f, SchemataLeaf):
                names.update(f.names)
            elif isinstance(f, SchemaLeaf):
                names.update(ensure_list(f.value))
        return names

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a lossless nested dict.

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
        if self.facet_sort:
            data["facet_sort"] = self.facet_sort.wire
        if self.facet_sizes:
            data["facet_size"] = {r.wire: n for r, n in self.facet_sizes.items()}
        if self.selection:
            data["select"] = [ref.wire for ref in self.selection]
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Rebuild a `Query` from its [`to_dict`][ftmq.Query.to_dict] output."""
        q = Expr.from_dict(data["q"]) if data.get("q") else None
        sort = None
        if data.get("order_by"):
            sort = Sort.deserialize(str(data["order_by"]))
        slice_ = _make_slice(data.get("limit"), data.get("offset"))
        aggregations = None
        if data.get("aggregations"):
            aggregations = aggregations_from_dict(data["aggregations"])
        selection = [ref_from_wire(f) for f in data.get("select") or []]
        facet_sort = None
        if data.get("facet_sort"):
            facet_sort = FacetOrder.from_wire(str(data["facet_sort"]))
        facet_sizes = {
            ref_from_wire(k): v for k, v in (data.get("facet_size") or {}).items()
        }
        return cls(
            q=q,
            sort=sort,
            slice=slice_,
            aggregations=aggregations,
            selection=selection,
            facet_sort=facet_sort,
            facet_sizes=facet_sizes,
        )

    def to_params(self) -> dict[str, list[str]]:
        """Serialize to an Aleph-style param dict.

        Keys: `filter:` / `exclude:` / `empty:`, the aggregation keys (`metric:`,
        `facet`, `facet_sort`, `facet_size:`), `select`, `sort`, `limit`, `offset`.

        Raises:
            QueryError: For a query outside the flat Aleph subset (cross-field
                OR, negated groups).
        """
        params = expr_to_params(self.q)
        if self.aggregations:
            params.update(aggregations_to_params(self.aggregations))
        if self.facet_sort:
            params["facet_sort"] = [self.facet_sort.wire]
        for ref, size in self.facet_sizes.items():
            params[f"facet_size:{ref.wire}"] = [str(size)]
        if self.selection:
            params["select"] = [ref.wire for ref in self.selection]
        if self.sort:
            direction = "asc" if self.sort.ascending else "desc"
            params["sort"] = [f"{self.sort.ref.wire}:{direction}"]
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
        if value := _single(items, "sort"):
            field, _, direction = value.partition(":")
            sort = Sort(ref_from_wire(field), ascending=direction != "desc")
        facet_sort = None
        if value := _single(items, "facet_sort"):
            facet_sort = FacetOrder.from_wire(value)
        facet_sizes: dict[Ref, int] = {}
        for key in items:
            if key.startswith("facet_size:"):
                size = _single(items, key) or ""
                if not size.isdigit():
                    raise QueryError(f"Invalid facet size for `{key}`: `{size}`")
                facet_sizes[ref_from_wire(key[len("facet_size:") :])] = int(size)
        offset = int(_single(items, "offset") or 0)
        limit = _single(items, "limit")
        slice_ = _make_slice(int(limit) if limit else None, offset)
        return cls(
            q=q,
            sort=sort,
            slice=slice_,
            aggregations=aggregations,
            selection=[ref_from_wire(f) for f in items.get("select", [])],
            facet_sort=facet_sort,
            facet_sizes=facet_sizes,
        )

    def to_string(self) -> str:
        """Serialize to an Aleph URL query string: `filter:properties.name=Jane&...`."""
        return params_to_string(self.to_params())

    @classmethod
    def from_string(cls, value: str) -> Self:
        """Build a `Query` from an Aleph URL query string."""
        return cls.from_params(string_to_params(value))

    @classmethod
    def from_rql(cls, value: str) -> Self:
        """Build a `Query` from an [RQL](https://github.com/pjwerneck/pyrql) string.

        Carries arbitrary `& | ~` nesting, aggregations and the projection.
        """
        if not value:
            return cls()
        expr, aggregations, selection = parse_rql(value)
        return cls(q=expr, aggregations=aggregations, selection=selection)

    def to_rql(self) -> str:
        """Serialize to an [RQL](https://github.com/pjwerneck/pyrql) string.

        The only string surface preserving arbitrary `& | ~` nesting; carries
        aggregations and the projection, but no sort or slice.

        Raises:
            QueryError: For a comparator with no RQL equivalent (`null`,
                `startswith`, `endswith`, ...).
        """
        return serialize_rql(self.q, self.aggregations, self.selection)

    def where(self, *nodes: Expr) -> Self:
        """AND nodes into the filter tree.

        Example:
            ```python
            q = Query().where(M(schema="Payment"), P(date__gte="2024-10"))
            q = q.where(G(countries="de") | G(countries="at"))
            ```

        Args:
            *nodes: `M` / `P` / `G` / `C` nodes, optionally composed with
                `&` / `|` / `~`.

        Returns:
            The updated `Query` instance.
        """
        new = combine(*nodes)
        if new is None:
            return self._chain()
        q = new if self.q is None else (self.q & new)
        return self._chain(q=q)

    def order_by(self, ref: Ref, *, ascending: bool = True) -> Self:
        """Sort by a single property: `order_by(P("date"), ascending=False)`.

        Args:
            ref: The property reference; only properties are sortable.
            ascending: Ascending or descending.

        Returns:
            The updated `Query` instance.
        """
        return self._chain(sort=Sort(ref, ascending=ascending))

    def aggregate(self, *nodes: A) -> Self:
        """Add aggregation projections.

        Example:
            ```python
            from ftmq import Query, M, P, A

            q = Query().where(M(schema="Payment")).aggregate(
                A(sum=P("amountEur"), by=P("beneficiary")),
                A(avg=P("amountEur")),
            )
            ```

        Args:
            *nodes: [`A`][ftmq.A] nodes, e.g.
                `A(sum=P("amountEur"), by=P("beneficiary"))`.

        Returns:
            The updated `Query` instance.
        """
        aggs = set(self.aggregations)
        for node in nodes:
            aggs.update(node.aggs)
        return self._chain(aggregations=aggs)

    def order_facets(self, *, ascending: bool = False, **func: Ref) -> Self:
        """Rank facet buckets by a grouped metric: `order_facets(sum=P("amountEur"))`.

        Descending by default; also decides which buckets the facet size keeps.

        Args:
            ascending: Rank the smallest values first.
            **func: Exactly one `func=<ref>` pair of a grouped aggregation.

        Returns:
            The updated `Query` instance.
        """
        if len(func) != 1:
            raise QueryError("Facet sort takes exactly one `func=<ref>` pair")
        [(name, ref)] = func.items()
        return self._chain(facet_sort=make_facet_order(name, ref, ascending))

    def facet_size(self, ref: Ref, size: int) -> Self:
        """Set how many (top) buckets a facet returns.

        Args:
            ref: A facet of the query, e.g. `P("beneficiary")`.
            size: The number of buckets (default 20).

        Returns:
            The updated `Query` instance.
        """
        return self._chain(facet_sizes={**self.facet_sizes, ref: size})

    def get_facet_size(self, ref: Ref) -> int:
        """The number of buckets the facet `ref` returns."""
        return self.facet_sizes.get(ref, DEFAULT_FACET_SIZE)

    def select(self, *refs: Ref) -> Self:
        """Restrict which properties the matching entities are read with.

        A projection, not a filter: it never changes which entities match. An
        entity holding none of the selected properties still comes back; caption
        and edges of a projected entity may be incomplete.

        Example:
            ```python
            from ftmq import Query, M, P

            q = Query().where(M(schemata="Document")).select(P("title"), P("fileName"))
            ```

        Args:
            *refs: The `P` / `G` refs to keep, e.g. `P("title")`,
                `G("countries")`.

        Returns:
            The updated `Query` instance.

        Raises:
            QueryError: For a ref of any other family.
        """
        for ref in refs:
            if not isinstance(ref, (PropRef, GroupRef)):
                raise QueryError(
                    f"Cannot select `{ref.wire}`: only a property "
                    "(`P`) or property-type group (`G`) can be projected"
                )
        return self._chain(selection=set(self.selection) | set(refs))

    def _project(self, entity: EntityProxy) -> EntityProxy:
        """Prune an entity to the selected properties, on a clone if anything drops."""
        drop = [
            prop
            for prop in entity.iterprops()
            if not any(ref.selects(prop) for ref in self.selection)
        ]
        if not drop:
            return entity
        clone = entity.clone()
        for prop in drop:
            clone.pop(prop)
        return clone

    def get_aggregator(self) -> Aggregator:
        """Build a fresh in-memory aggregator for the query's aggregations.

        Returns:
            The [`Aggregator`][ftmq.query.aggregations.Aggregator].
        """
        return Aggregator(self.aggregations, self.facet_sizes, self.facet_sort)

    def apply(self, entity: EntityProxy) -> bool:
        """Whether an entity matches the filter tree."""
        if self.q is None:
            return True
        return self.q.apply(entity)

    def apply_iter(self, entities: EntityProxies) -> EntityProxies:
        """Filter, sort, slice, aggregate and project a stream of entities.

        Aggregation results are collected into `self.aggregator`.

        Example:
            ```python
            entities = [...]
            q = Query().where(M(dataset="my_dataset"), M(schema="Company"))
            for entity in q.apply_iter(entities):
                assert entity.schema.name == "Company"
            ```

        Yields:
            The matching entities.
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
        if self.selection:
            # last, so filters, sort and aggregations read the full entity
            entities = (self._project(e) for e in entities)
        yield from entities
