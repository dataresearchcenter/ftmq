from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property, singledispatchmethod
from typing import TYPE_CHECKING, Any, Callable, Iterable, TypeAlias

from banal import as_bool
from nomenklatura.db import make_statement_table
from sqlalchemy import (
    NUMERIC,
    Boolean,
    BooleanClauseList,
    Column,
    MetaData,
    Select,
    and_,
    case,
    desc,
    distinct,
    func,
    literal_column,
    not_,
    or_,
    select,
    text,
    true,
    union_all,
)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from ftmq.query.aggregations import Agg, groupers
from ftmq.query.exceptions import QueryError
from ftmq.query.leaves import (
    GroupLeaf,
    Leaf,
    PropertyLeaf,
    SchemaLeaf,
    SchemataLeaf,
    group_conjunction,
    row_scoped_groups,
)
from ftmq.query.nodes import OR, Expr
from ftmq.query.refs import (
    CanonicalIdRef,
    ContextRef,
    DatasetRef,
    EntityIdRef,
    GroupRef,
    IdRef,
    PropRef,
    Ref,
    SchemaRef,
    YearRef,
)

if TYPE_CHECKING:
    from ftmq.query.main import Query


# a query -> partition-values function for one prune column (e.g. `bucket`).
PruneFn: TypeAlias = Callable[["Query"], Iterable[str] | None]


def prune_by_schema(get_partition: Callable[[str], str]) -> PruneFn:
    """Build a prune rule for a partition column derived from the schema.

    Used as a [`SqlSource`][ftmq.query.sql.SqlSource] `prune` rule (the lake
    store's `bucket`): maps the query's `schema` / `schemata` filters to the
    partitions of their schemata. Without a schema filter, or with a `not` /
    `not_in` schema comparator, nothing is pruned.

    Args:
        get_partition: Maps a schema name to its partition value.

    Returns:
        The prune rule.
    """

    def prune(q: "Query") -> set[str] | None:
        for leaf in q._leaves:
            if isinstance(leaf, (SchemaLeaf, SchemataLeaf)):
                if leaf.comparator not in ("eq", "in"):
                    return None
        return {get_partition(s) for s in q.schemata_names}

    return prune


@dataclass
class Lookup:
    """Where a ref reads from: its value expression and optional row predicate."""

    value: Any
    where: Any | None = None

    @property
    def clauses(self) -> list[Any]:
        return [] if self.where is None else [self.where]


# sqlite numeric: contains a digit, and nothing a number can't contain
SQLITE_NUMERIC_GLOB = ("*[0-9]*", "*[^0-9.eE+-]*")


class NumericValue(FunctionElement[Any]):
    """A statement `value` read as a number, `NULL` (not an error) if it isn't one."""

    name = "numeric_value"
    type = NUMERIC()
    inherit_cache = True


@compiles(NumericValue)
def _compile_numeric_value(element: NumericValue, compiler: Any, **kw: Any) -> str:
    # duckdb spelling: the lake store compiles against the default dialect
    return f"TRY_CAST({compiler.process(element.clauses, **kw)} AS NUMERIC)"


@compiles(NumericValue, "sqlite")
def _compile_numeric_value_sqlite(
    element: NumericValue, compiler: Any, **kw: Any
) -> str:
    # sqlite casts a non-number to 0 instead of raising; the GLOB guard makes it NULL
    value = compiler.process(element.clauses, **kw)
    has_digit, has_other = SQLITE_NUMERIC_GLOB
    guard = f"{value} GLOB '{has_digit}' AND NOT {value} GLOB '{has_other}'"
    return f"(CASE WHEN {guard} THEN CAST({value} AS NUMERIC) END)"


@compiles(NumericValue, "postgresql")
def _compile_numeric_value_postgresql(
    # pg_input_is_valid needs postgres 16+
    element: NumericValue,
    compiler: Any,
    **kw: Any,
) -> str:
    value = compiler.process(element.clauses, **kw)
    return f"(CASE WHEN pg_input_is_valid({value},'numeric') THEN CAST({value} AS NUMERIC) END)"


def numeric_value(column: Any) -> Any:
    """Read a canonical-format statement `value` as a number, else `NULL`."""
    return NumericValue(column)


class SqlSource:
    """The statement source a [`Query`][ftmq.Query] compiles against.

    Stores own one and pass it to [`Sql`][ftmq.query.sql.Sql] /
    [`Query.compile`][ftmq.Query.compile]; a store with extra columns (a lake /
    sharded table) supplies its own.

    Args:
        table: The SQLAlchemy `Table` / `TableClause` to query.
        id_column: The entity-identity column name (default `canonical_id`).
        prune: Partition-pruning rules as `{column: function}`: each function
            returns that column's possible values for a query (`None` or empty
            for no pruning), folded in as a `column IN (...)` row predicate (see
            [`prune_by_schema`][ftmq.query.sql.prune_by_schema]). A rule for a
            missing column is ignored; rules only run for a flat positive
            conjunction.
        base_filter: A predicate folded into every compiled select and
            sub-select (e.g. a lake store's view filter).
    """

    def __init__(
        self,
        table: Any,
        id_column: str = "canonical_id",
        prune: dict[str, PruneFn] | None = None,
        base_filter: Any | None = None,
    ) -> None:
        self.table = table
        self.id_column = id_column
        self.prune = prune or {}
        self.base_filter = base_filter


class Sql:
    """Compile a [`Query`][ftmq.Query] to SQLAlchemy selects over a statement table.

    Every leaf lifts to an entity-level `canonical_id IN (...)` membership, so a
    filter selects whole entities, assembled from all of their statements.

    Args:
        q: The query.
        source: The statement source (default: the nomenklatura statement table).
        scope: Restrict to entities with a statement in one of these datasets.
    """

    COMPARATORS = {
        "eq": "__eq__",
        "not": "__ne__",
        "in": "in_",
        "not_in": "not_in",
        "gt": "__gt__",
        "gte": "__ge__",
        "lt": "__lt__",
        "lte": "__le__",
    }

    def __init__(
        self,
        q: Query,
        source: SqlSource | None = None,
        scope: Iterable[str] | None = None,
    ) -> None:
        self.q = q
        self.metadata = MetaData()
        if source is None:
            source = SqlSource(make_statement_table(self.metadata))
        self.source = source
        self.table = source.table
        self.id_col = self.table.c[source.id_column]
        self.scope: set[str] | None = set(scope) if scope else None

    @cached_property
    def _base_clauses(self) -> list[Any]:
        """The source's base filter, folded into every select and sub-select."""
        if self.source.base_filter is not None:
            return [self.source.base_filter]
        return []

    def get_expression(self, column: Column, f: Leaf):
        c = f.comparator
        if c == "null":
            # presence test; property / group presence is `_family_clause`
            return column.is_(None) if f.value else column.is_not(None)
        # autoescape so `%` / `_` in the value match literally
        if c in ("like", "notlike"):
            like = column.contains(f.value, autoescape=True)
            return not_(like) if c == "notlike" else like
        if c in ("ilike", "notilike"):
            like = column.icontains(f.value, autoescape=True)
            return not_(like) if c == "notilike" else like
        if c == "startswith":
            return column.startswith(f.value, autoescape=True)
        if c == "endswith":
            return column.endswith(f.value, autoescape=True)
        op = self.COMPARATORS.get(c)
        if op is None:
            raise QueryError(f"Comparator not supported in SQL: `{c}`")
        value = f.value
        # leaf values are strings, a Boolean column (`external`) needs bools
        if isinstance(column.type, Boolean):
            if isinstance(value, (set, frozenset, list, tuple)):
                value = sorted({as_bool(v) for v in value})
            else:
                value = as_bool(value)
        return getattr(column, op)(value)

    @staticmethod
    def _is_null(f: Leaf) -> bool:
        return f.comparator == "null"

    def _entity_ids(self, pred: Any) -> Select:
        """Sub-select of the entity ids with a (base-filtered) row matching `pred`."""
        return (
            select(self.id_col)
            .distinct()
            .where(and_(true(), *self._base_clauses, pred))
        )

    def _absent(self, present: Any) -> Any:
        """Lift a row predicate to an entity-level absence (anti-join)."""
        return self.id_col.not_in(self._entity_ids(present))

    def _membership(self, pred: Any) -> Any:
        """Lift a row predicate to an entity-level membership: some row matches."""
        return self.id_col.in_(self._entity_ids(pred))

    def _family_clause(self, leaf: Leaf, lookup: Lookup) -> Any:
        """Clause for a property / group leaf; `null` tests presence of such a row."""
        if self._is_null(leaf):
            if leaf.value:
                return self._absent(lookup.where)
            return self._membership(lookup.where)
        return self._membership(
            and_(lookup.where, self.get_expression(lookup.value, leaf))
        )

    def _schema_clause(self, f: Leaf) -> Any:
        """Entity-level schema / is-a clause: membership, or anti-join if negated."""
        negated = f.comparator in ("not", "not_in")
        if isinstance(f, SchemataLeaf):
            positive = self.table.c.schema.in_(f.names)
        elif negated:
            values = f.value if isinstance(f.value, (set, frozenset)) else {f.value}
            positive = self.table.c.schema.in_(sorted(values))
        else:
            return self._membership(self.get_expression(self.table.c.schema, f))
        if negated:
            return self._absent(positive)
        return self._membership(positive)

    def _row_membership(self, leaves: Iterable[Leaf]) -> Any:
        """One membership for co-referring row-scoped leaves, all on a single row."""
        rows = [
            self.get_expression(self.lookup(f.ref).value, f)
            for f in sorted(leaves, key=lambda f: (f.key, f.comparator))
        ]
        return self._membership(and_(true(), *rows))

    def _bound_clause(self, leaves: list[Leaf]) -> Any:
        """One membership for bounds on one property / group, all on a single row."""
        lookup = self.lookup(leaves[0].ref)
        return self._membership(
            and_(
                lookup.where,
                *(
                    self.get_expression(lookup.value, f)
                    for f in sorted(leaves, key=lambda f: f.comparator)
                ),
            )
        )

    def _leaf_clause(self, leaf: Leaf) -> Any:
        """Lift one leaf to an entity-level `canonical_id IN (...)` clause."""
        if isinstance(leaf, (SchemaLeaf, SchemataLeaf)):
            # already entity-level, membership or anti-join
            return self._schema_clause(leaf)
        lookup = self.lookup(leaf.ref)
        if lookup.where is not None:  # a property / group family
            return self._family_clause(leaf, lookup)
        column = lookup.value
        if self._is_null(leaf) and leaf.value:
            return self._absent(column.is_not(None))
        row = self.get_expression(column, leaf)
        if column is self.id_col:
            # already true for every row of a matching entity
            return row
        return self._membership(row)

    def _expr_clause(self, expr: Expr) -> Any:
        """Compile a node; co-referring leaves of an AND share one sub-select."""
        leaves = [c for c in expr.children if isinstance(c, Leaf)]
        if expr.connector == OR:
            clauses = {leaf: self._leaf_clause(leaf) for leaf in leaves}
        else:
            clauses = self._conjunction_clauses(leaves)
        parts: list[Any] = []
        for child in expr.children:
            if isinstance(child, Expr):
                parts.append(self._expr_clause(child))
            elif child in clauses:
                parts.append(clauses[child])
        # an empty node is `true` (`false` when negated)
        combined = (
            or_(*parts) if parts and expr.connector == OR else and_(true(), *parts)
        )
        return not_(combined) if expr.negated else combined

    def _conjunction_clauses(self, leaves: Iterable[Leaf]) -> dict[Leaf, Any]:
        """Clauses for an AND node's leaves, co-referring ones joined, keyed by leaf."""
        groups = group_conjunction(leaves)
        joined = {id(g) for g in row_scoped_groups(groups)}
        row_scoped = [leaf for g in groups if id(g) in joined for leaf in g]
        clauses: dict[Leaf, Any] = {}
        for group in groups:
            if id(group) in joined:
                if row_scoped:
                    clauses[group[0]] = self._row_membership(row_scoped)
                    row_scoped = []
            elif len(group) == 1:
                clauses[group[0]] = self._leaf_clause(group[0])
            elif isinstance(group[0], (PropertyLeaf, GroupLeaf)):
                clauses[group[0]] = self._bound_clause(group)
            else:
                # entity-scoped field: per-leaf and joined clauses agree
                for leaf in group:
                    clauses[leaf] = self._leaf_clause(leaf)
        return clauses

    @cached_property
    def _is_flat_and(self) -> bool:
        """Whether the tree is a plain conjunction with at most one leaf per field."""

        def walk(expr: Expr) -> bool:
            if expr.negated or (expr.connector == OR and len(expr.children) > 1):
                return False
            return all(walk(c) for c in expr.children if isinstance(c, Expr))

        if self.q.q is None:
            return True
        if not walk(self.q.q):
            return False
        keys = [(type(f).__name__, f.key) for f in self.q._leaves]
        return len(keys) == len(set(keys))

    @cached_property
    def _prune_clauses(self) -> list[Any]:
        """One `column IN (...)` row predicate per prune rule, for a flat AND only."""
        if not self.source.prune or not self._is_flat_and:
            return []
        clauses: list[Any] = []
        for column, prune_fn in self.source.prune.items():
            if column not in self.table.c:
                continue
            values = prune_fn(self.q)
            if values:
                clauses.append(self.table.c[column].in_(sorted(set(values))))
        return clauses

    @cached_property
    def _clauses(self) -> list[Any]:
        """The filter tree and view scope as entity-level predicates."""
        clauses = [self._expr_clause(self.q.q)] if self.q.q is not None else []
        # the scope selects entities, not rows: a match keeps its out-of-scope rows
        if self.scope:
            clauses.append(
                self._membership(self.table.c.dataset.in_(sorted(self.scope)))
            )
        return clauses

    @cached_property
    def clause(self) -> BooleanClauseList:
        # `and_(true(), x)` collapses to `x`; an empty conjunction is `true`
        return and_(true(), *self._base_clauses, *self._prune_clauses, *self._clauses)

    @cached_property
    def _projection_clauses(self) -> list[Any]:
        """Projection row filter for statement selects only, keeping the `id` row."""
        if not self.q.selection:
            return []
        rows = [w for r in self.q.selection if (w := self.lookup(r).where) is not None]
        return [or_(*rows, self.table.c.prop == "id")]

    @property
    def _limit(self) -> int | None:
        # an offset without limit renders `LIMIT -1`, which duckdb rejects
        if self.q.limit is None and self.q.offset:
            return 2**63 - 1
        return self.q.limit

    @cached_property
    def canonical_ids(self) -> Select:
        q = select(self.id_col).distinct().where(self.clause)
        if self.q.sort is None:
            if self.q.slice is not None:
                # pages need a total order, or they repeat and skip entities
                q = q.order_by(self.id_col)
            # omit a redundant offset 0
            q = q.limit(self._limit).offset(self.q.offset or None)
        return q

    @cached_property
    def _unsorted_statements(self) -> Select:
        # a slice (even limit 0) applies in the `canonical_ids` sub-select
        if self.q.slice is not None:
            where = and_(
                true(),
                *self._base_clauses,
                *self._projection_clauses,
                self.id_col.in_(self.canonical_ids),
            )
        else:
            # entity-level clause: already true for every row of a matching entity
            where = and_(true(), self.clause, *self._projection_clauses)
        return select(self.table).where(where).order_by(self.id_col)

    @cached_property
    def _sorted_statements(self) -> Select:
        prop = self.q.sort.ref.key
        value = self.table.c.value
        if self.q.sort.ref.is_numeric:
            value = numeric_value(self.table.c.value)
        group_func = func.min if self.q.sort.ascending else func.max

        def order(col: Any) -> Any:
            # a missing value sorts last either way, as in memory
            return (col.asc() if self.q.sort.ascending else col.desc()).nulls_last()

        # the `id` rows keep entities without the sort prop (NULL value)
        sortable_value = group_func(case((self.table.c.prop == prop, value)))
        inner = (
            select(self.id_col, sortable_value.label("sortable_value"))
            .where(and_(self.table.c.prop.in_([prop, "id"]), self.clause))
            .group_by(self.id_col)
            .limit(self._limit)
            .offset(self.q.offset or None)
        )
        # explicit subquery: an implicit one via `Select.c` is deprecated
        sub = inner.order_by(order(literal_column("sortable_value")), self.id_col)
        sub = sub.subquery()
        sortable = sub.c["sortable_value"]
        outer = select(
            self.table.join(sub, self.id_col == sub.c[self.source.id_column])
        )
        # the joined rows still need the base filter and the projection
        read = [*self._base_clauses, *self._projection_clauses]
        if read:
            outer = outer.where(*read)
        return outer.order_by(order(sortable), self.id_col)

    @cached_property
    def statements(self) -> Select:
        if self.q.sort:
            return self._sorted_statements
        return self._unsorted_statements

    @cached_property
    def count(self) -> Select:
        return (
            select(func.count(self.id_col.distinct()))
            .select_from(self.table)
            .where(self.clause)
        )

    @singledispatchmethod
    def lookup(self, ref: Ref) -> Lookup:
        """Where a field reference reads from in this source.

        A meta / context ref reads its own column (no row predicate); a property
        or group ref reads `value`, selecting its rows by `prop` / `prop_type`.
        """
        raise QueryError(f"Cannot compile field reference: `{ref!r}`")

    @lookup.register
    def _(self, ref: IdRef) -> Lookup:
        # the resolved entity id, not the referent id in a `prop = "id"` row
        return Lookup(self.id_col)

    @lookup.register
    def _(self, ref: CanonicalIdRef) -> Lookup:
        return Lookup(self.table.c.canonical_id)

    @lookup.register
    def _(self, ref: EntityIdRef) -> Lookup:
        return Lookup(self.table.c.entity_id)

    @lookup.register
    def _(self, ref: DatasetRef) -> Lookup:
        return Lookup(self.table.c.dataset)

    @lookup.register
    def _(self, ref: SchemaRef) -> Lookup:
        return Lookup(self.table.c.schema)

    @lookup.register
    def _(self, ref: PropRef) -> Lookup:
        return Lookup(self.table.c.value, self.table.c.prop == ref.key)

    @lookup.register
    def _(self, ref: GroupRef) -> Lookup:
        return Lookup(self.table.c.value, self.table.c.prop_type == str(ref.prop_type))

    @lookup.register
    def _(self, ref: YearRef) -> Lookup:
        return Lookup(
            func.substring(self.table.c.value, 1, 4),
            self.table.c.prop_type == str(ref.prop_type),
        )

    @lookup.register
    def _(self, ref: ContextRef) -> Lookup:
        if ref.key not in self.table.c:
            raise QueryError(f"Unknown context column: `{ref.key}`")
        return Lookup(self.table.c[ref.key])

    def get_group_counts(
        self, group: Ref, extra_where: BooleanClauseList | None = None
    ) -> Select:
        count = func.count(self.id_col.distinct()).label("count")
        lookup = self.lookup(group)
        where = and_(true(), *lookup.clauses, self.clause)
        if extra_where is not None:
            where = and_(where, extra_where)
        return (
            select(lookup.value, count)
            .where(where)
            .group_by(lookup.value)
            .order_by(desc(count), lookup.value)
        )

    @cached_property
    def date_range(self) -> Select:
        return select(
            func.min(self.table.c.value),
            func.max(self.table.c.value),
        ).where(self.table.c.prop_type == "date", self.clause)

    @property
    def _specs(self) -> list[Agg]:
        return sorted(self.q.aggregations, key=lambda a: (a.func, a.key))

    @staticmethod
    def _tags(agg: Agg) -> tuple[Any, Any]:
        """The `(field, func)` literals naming a spec's rows in a union."""
        return text(f"'{agg.key}'"), text(f"'{agg.func}'")

    def _aggregator(self, agg: Agg) -> Any:
        """The aggregate expression for one spec, over its ref's value."""
        value = self.lookup(agg.ref).value
        if agg.func == "count":
            # distinct raw values, no numeric cast
            return func.count(distinct(value))
        if agg.ref.is_numeric:
            # min / max too: a lexicographic min over numbers is wrong
            value = numeric_value(value)
        return getattr(func, agg.func)(value)

    @cached_property
    def aggregations(self) -> Select:
        qs = [
            select(*self._tags(agg), self._aggregator(agg)).where(
                *self.lookup(agg.ref).clauses, self.clause
            )
            for agg in self._specs
        ]
        return union_all(*qs)

    def grouped_aggregations(self, grouper: Ref, limit: int | None = None) -> Select:
        """Every aggregation spec grouped by `grouper`, as one unioned select.

        Rows are `(field, func, group_value, value)`. Specs aggregate over the
        distinct `(entity, group value)` pairs of the matching entities, so a
        multi-valued group property does not multiply rows.

        Args:
            grouper: The field reference to group by.
            limit: Keep the top `limit` group values, by entity count or by
                the query's `facet_sort` metric, ties by value.

        Returns:
            The unioned select.
        """
        g = self.lookup(grouper)
        pairs = (
            select(self.id_col.label("cid"), g.value.label("gval"))
            .where(and_(true(), *g.clauses, self.clause))
            .distinct()
        )
        if limit is not None:
            top = self._top_groups(grouper, pairs.subquery(), limit)
            pairs = pairs.where(g.value.in_(select(top.c[0])))
        sub = pairs.subquery()
        qs = []
        for agg in self._specs:
            if grouper in agg.groups:
                grouped = self._grouped_value(agg, sub)
                columns = grouped.selected_columns
                qs.append(grouped.with_only_columns(*self._tags(agg), *columns))
        return union_all(*qs)

    def _top_groups(self, grouper: Ref, pairs: Any, limit: int) -> Any:
        """Top `limit` values of `grouper`, by facet sort metric or entity count."""
        order = self.q.facet_sort
        ranking = next(
            (
                a
                for a in self.q.aggregations
                if order is not None and order.orders(a) and grouper in a.groups
            ),
            None,
        )
        if order is not None and ranking is not None:
            ranked = self._grouped_value(ranking, pairs)
            value = ranked.selected_columns[1]
            rank = (value.asc() if order.ascending else value.desc()).nulls_last()
        else:
            count = func.count().label("count")
            ranked = select(pairs.c.gval, count).group_by(pairs.c.gval)
            rank = desc(ranked.selected_columns[1])
        return ranked.order_by(rank, pairs.c.gval).limit(limit).subquery()

    def _grouped_value(self, agg: Agg, pairs: Any) -> Select:
        """`(gval, value)` rows of `agg` per group value of `pairs`."""
        lookup = self.lookup(agg.ref)
        return (
            select(pairs.c.gval, self._aggregator(agg))
            .select_from(self.table.join(pairs, self.id_col == pairs.c.cid))
            .where(and_(true(), *self._base_clauses, *lookup.clauses))
            .group_by(pairs.c.gval)
        )

    @cached_property
    def group_props(self) -> set[Ref]:
        return groupers(self.q.aggregations)
