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
    desc,
    distinct,
    func,
    not_,
    or_,
    select,
    text,
    true,
    union_all,
)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from ftmq.query.aggregations import Agg
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
from ftmq.query.nodes import OR, Expr
from ftmq.query.refs import (
    NUMERIC_PROPS,
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


# a schema-value -> partition-value function (e.g. schema name -> `bucket`)
PruneFn: TypeAlias = Callable[[str], str]


@dataclass
class Lookup:
    """Where a [`Ref`][ftmq.query.refs.Ref] reads from in a statement table:
    the expression carrying its value, and the row predicate selecting its rows
    (none for a column every statement of an entity carries)."""

    value: Any
    where: Any | None = None

    @property
    def clauses(self) -> list[Any]:
        return [] if self.where is None else [self.where]


# sqlite numeric: contains a digit, and nothing a number can't contain
SQLITE_NUMERIC_GLOB = ("*[0-9]*", "*[^0-9.eE+-]*")


class NumericValue(FunctionElement[Any]):
    """A statement `value` read as a number, `NULL` if it isn't one.

    The `value` column is text: a non-numeric value in it (an unmigrated store,
    or a value `cast_number` couldn't parse and passed through) makes a plain
    `CAST` raise on postgres and duckdb, which fails the whole aggregation.
    Every dialect renders the error-tolerant spelling instead, so a stray value
    drops out of the aggregate the way it does in memory (where
    `registry.number.to_number` returns `None` and the aggregator skips it).
    """

    name = "numeric_value"
    type = NUMERIC()
    inherit_cache = True


@compiles(NumericValue)
def _compile_numeric_value(element: NumericValue, compiler: Any, **kw: Any) -> str:
    # TRY_CAST is the duckdb spelling - the lake store compiles against the
    # default dialect, so it is what the generic compiler has to emit
    return f"TRY_CAST({compiler.process(element.clauses, **kw)} AS NUMERIC)"


@compiles(NumericValue, "sqlite")
def _compile_numeric_value_sqlite(
    element: NumericValue, compiler: Any, **kw: Any
) -> str:
    # sqlite has no TRY_CAST and raises nothing to begin with: it reads the
    # numeric prefix of a value and falls back to 0, so `"n/a"` would count as
    # a 0 in a min / avg where the other dialects skip it. The same GLOB guard
    # gets it to NULL as well.
    value = compiler.process(element.clauses, **kw)
    has_digit, has_other = SQLITE_NUMERIC_GLOB
    guard = f"{value} GLOB '{has_digit}' AND NOT {value} GLOB '{has_other}'"
    return f"(CASE WHEN {guard} THEN CAST({value} AS NUMERIC) END)"


@compiles(NumericValue, "postgresql")
def _compile_numeric_value_postgresql(
    # use postgres 16+ built-in test
    element: NumericValue,
    compiler: Any,
    **kw: Any,
) -> str:
    value = compiler.process(element.clauses, **kw)
    return f"(CASE WHEN pg_input_is_valid({value},'numeric') THEN CAST({value} AS NUMERIC) END)"


def numeric_value(column: Any) -> Any:
    """Read a statement `value` as a number, `NULL` if it isn't one (see
    [`NumericValue`][ftmq.query.sql.NumericValue]). Stored values should be in
    the canonical format written by
    [`ftmq.statements.cast_number`][ftmq.statements.cast_number] (no thousands
    separators, no unit suffix); a store fed raw display-formatted amounts has
    to be migrated first (`ftmq statements cast-types`), otherwise its values
    read as `NULL` instead of the number they display."""
    return NumericValue(column)


class SqlSource:
    """Describes the SQL statement source a [`Query`][ftmq.Query] compiles
    against: the SQLAlchemy table (or view), the entity-identity column, and an
    optional partition-pruning rule.

    Stores own one and pass it to [`Sql`][ftmq.query.sql.Sql] /
    [`Query.compile`][ftmq.Query.compile]. A downstream store with extra
    columns (a lake / sharded table) supplies its own `SqlSource` so the same
    `Query` compiles against it unchanged.

    Args:
        table: The SQLAlchemy `Table` / `TableClause` to query.
        id_column: The entity-identity column name (default `canonical_id`).
        prune_schema: Optional function folding a schema/schemata filter into a
            `prune_column IN (...)` partition predicate on every compiled query
            (e.g. the lake store's schema -> `bucket` mapping).
        prune_column: The partition column the pruned values target
            (e.g. `bucket`).
        base_filter: Optional SQLAlchemy predicate folded into every compiled
            select *and* sub-select (e.g. a lake store's view filter). Unlike a
            predicate added post-hoc to the top-level select, this also scopes
            the entity-level membership / absence subqueries.
    """

    def __init__(
        self,
        table: "Any",
        id_column: str = "canonical_id",
        prune_schema: "PruneFn | None" = None,
        prune_column: str | None = None,
        base_filter: "Any | None" = None,
    ) -> None:
        self.table = table
        self.id_column = id_column
        self.prune_schema = prune_schema
        self.prune_column = prune_column
        self.base_filter = base_filter


class Sql:
    COMPARATORS = {
        "eq": "__eq__",
        "not": "__ne__",
        "in": "in_",
        "gt": "__gt__",
        "gte": "__ge__",
        "lt": "__lt__",
        "lte": "__le__",
    }

    def __init__(
        self,
        q: "Query",
        source: "SqlSource | None" = None,
        scope: "Iterable[str] | None" = None,
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
        """Row predicates folded into every select *and* sub-select: the
        source's base filter (e.g. a lake view filter), which defines the
        statement rows that exist at all for this source.

        The view `scope` is *not* a row predicate - see `clause`.
        """
        if self.source.base_filter is not None:
            return [self.source.base_filter]
        return []

    def get_expression(self, column: Column, f: Leaf):
        c = f.comparator
        if c == "null":
            # `null` tests presence, not a value: `null=True` means the column
            # is unset. (For the `prop` / `prop_type` families presence is a
            # row-existence test, handled in `clause`.)
            return column.is_(None) if f.value else column.is_not(None)
        # substring / prefix / suffix comparators: autoescape so `%` and `_` in
        # the value match literally, like the in-memory substring test
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
        # the leaf layer stringifies values, but typed columns (e.g. the
        # Boolean `external`) need the original type to compare correctly
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
        """A sub-select of the entity ids having a row matching `pred`,
        over the rows visible to this source (the base filter)."""
        return select(self.id_col.distinct()).where(
            and_(true(), *self._base_clauses, pred)
        )

    def _absent(self, present: Any) -> Any:
        """Lift a row-presence predicate to an entity-level absence check.

        `null=True` asks whether an entity has *no* such row at all, which no
        single statement row can answer - it becomes a `canonical_id` anti-join.
        """
        return self.id_col.not_in(self._entity_ids(present))

    def _membership(self, pred: Any) -> Any:
        """Lift a row predicate to an entity-level membership clause: the
        entity has at least one row matching it."""
        return self.id_col.in_(self._entity_ids(pred))

    def _family_clause(self, leaf: Leaf, selector: Callable[[Any], Any]) -> Any:
        """One entity-level clause for a property / group leaf.

        `selector` builds the family predicate (e.g. `prop = "name"`). `null`
        tests presence of such a row, not the value: `null=False` is any row
        for the family, `null=True` the absence of one.
        """
        family = selector(leaf)
        if self._is_null(leaf):
            if leaf.value:
                return self._absent(family)
            return self._membership(family)
        return self._membership(
            and_(family, self.get_expression(self.table.c.value, leaf))
        )

    def _prop_selector(self, f: Any) -> Any:
        return self.table.c.prop == f.key

    def _group_selector(self, f: Any) -> Any:
        return self.table.c.prop_type == str(f.prop_type)

    def _schema_clause(self, f: Leaf) -> Any:
        """A clause for exact-schema / is-a (`schemata`) filters.

        Positive comparators stay row predicates (an is-a filter expands to the
        schema plus its non-abstract descendants). `not` / `not_in` compile as
        entity-level anti-joins: in-memory they test the entity's single
        resolved schema, and a row predicate would wrongly match any merged
        entity that has one statement row outside the excluded set.
        """
        negated = f.comparator in ("not", "not_in")
        if isinstance(f, SchemataLeaf):
            names: set[str] = set()
            for schema in f.schemata:
                names.add(schema.name)
                names.update(d.name for d in schema.descendants if not d.abstract)
            positive = self.table.c.schema.in_(names)
        elif negated:
            values = f.value if isinstance(f.value, (set, frozenset)) else {f.value}
            positive = self.table.c.schema.in_(sorted(values))
        else:
            return self.get_expression(self.table.c.schema, f)
        if negated:
            return self._absent(positive)
        return positive

    def _context_column(self, f: ContextLeaf) -> Any:
        if f.key not in self.table.c:
            raise QueryError(f"Unknown context column: `{f.key}`")
        return self.table.c[f.key]

    def _id_column(self, f: IdLeaf) -> Any:
        # `M(id=...)` addresses the entity: in a statement table that is the
        # resolved id column, not `statement.id` (the statement's own id)
        if f.key == "id":
            return self.id_col
        return self.table.c[f.key]

    def _context_clauses(self) -> tuple[list[Any], list[Any]]:
        """`(row, entity)` clauses for context / storage columns
        (`C(origin=...)`, `C(fragment=...)`, ...).

        A single column stays a row predicate; several distinct columns each
        lift to an entity-level membership - in-memory a context value is the
        entity's aggregate over its statements, so `C(origin=..) & C(lang=..)`
        may be satisfied by two different rows.
        """
        rows: list[Any] = []
        entities: list[Any] = []
        context = sorted(self.q.context, key=lambda f: f.key)
        entity_level = len(context) > 1
        for f in context:
            # "unset" means no row carries the column
            if self._is_null(f) and f.value:
                entities.append(self._absent(self._context_column(f).is_not(None)))
            elif entity_level:
                entities.append(
                    self._membership(self.get_expression(self._context_column(f), f))
                )
            else:
                rows.append(self.get_expression(self._context_column(f), f))
        return rows, entities

    def _leaf_clause(self, leaf: Leaf) -> Any:
        """An entity-level predicate for a single leaf.

        Lifting every leaf to entity level (`canonical_id IN (...)`) is what
        makes an arbitrary `& | ~` tree composable: `OR` / `NOT` over row
        predicates would ask a single statement row a question about the whole
        entity ("this entity has no name" is not a property of any one row).
        """
        if isinstance(leaf, PropertyLeaf):
            return self._family_clause(leaf, self._prop_selector)
        if isinstance(leaf, GroupLeaf):
            return self._family_clause(leaf, self._group_selector)
        if isinstance(leaf, (SchemaLeaf, SchemataLeaf)):
            clause = self._schema_clause(leaf)
            if leaf.comparator in ("not", "not_in"):
                return clause  # already an entity-level anti-join
            return self._membership(clause)
        if isinstance(leaf, ContextLeaf):
            if self._is_null(leaf) and leaf.value:
                return self._absent(self._context_column(leaf).is_not(None))
            row = self.get_expression(self._context_column(leaf), leaf)
        elif isinstance(leaf, IdLeaf):
            row = self.get_expression(self._id_column(leaf), leaf)
        elif isinstance(leaf, DatasetLeaf):
            row = self.get_expression(self.table.c.dataset, leaf)
        else:
            raise QueryError(f"Cannot compile filter to sql: `{leaf.key}`")
        return self._membership(row)

    def _expr_clause(self, expr: Expr) -> Any:
        """Compile a boolean node by combining its children's entity-level
        predicates - the general path for trees the flat collectors below
        cannot represent (cross-field `OR`, negation)."""
        parts = [
            self._expr_clause(c) if isinstance(c, Expr) else self._leaf_clause(c)
            for c in expr.children
        ]
        # an empty node matches everything - unless negated, when it matches
        # nothing (`not_(true())` compiles to `false`)
        combined = (
            or_(*parts) if parts and expr.connector == OR else and_(true(), *parts)
        )
        return not_(combined) if expr.negated else combined

    @cached_property
    def _is_flat_and(self) -> bool:
        """Whether the query tree is a plain conjunction with at most one leaf
        per field - the shape the flat collectors below represent losslessly.
        Anything else (OR, negation, repeated fields, whose leaves AND in the
        language) compiles through `_expr_clause`."""

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
    def _prune_values(self) -> set[str] | None:
        """Partition values for a schema/schemata filter (e.g. the lake
        `bucket` column), folded into every compiled query - so `count` prunes
        partitions too, not just statements.

        Pruning is only sound for positive schema conjuncts: under `~` / `|` or
        a `not` comparator the filter no longer restricts matching entities to
        those partitions, so any such shape disables pruning entirely.
        """
        prune_fn = self.source.prune_schema
        if (
            prune_fn is None
            or not self.source.prune_column
            or self.source.prune_column not in self.table.c
            or not self._is_flat_and
            or not self.q.schemata_names
        ):
            return None
        for f in self.q._leaves:
            if isinstance(f, (SchemaLeaf, SchemataLeaf)):
                if f.comparator not in ("eq", "in"):
                    return None
        return {prune_fn(s) for s in self.q.schemata_names}

    @cached_property
    def _clauses(self) -> tuple[list[Any], list[Any]]:
        """The compiled query as `(row_clauses, entity_clauses)`.

        A *row* clause constrains individual statement rows (`dataset = 'x'`);
        an *entity* clause is a `canonical_id` membership or anti-join, which is
        already true for every row of a matching entity. Keeping them apart lets
        the statement / facet selects skip the `canonical_id IN (...)`
        indirection when nothing is row-constrained - it would be a second pass
        over the same rows for the same answer.
        """
        if self._is_flat_and:
            rows, entities = self._flat_clauses()
        else:
            # a boolean tree compiles entirely to entity-level predicates
            rows, entities = [], [self._expr_clause(self.q.q)]
        if self._prune_values:
            rows.append(self.table.c[self.source.prune_column].in_(self._prune_values))
        # the view scope selects *entities* (those with at least one statement
        # in a scoped dataset), matching the in-memory store views: filters and
        # assembly still see the full canonical entity. A row-level `dataset`
        # predicate would instead silently drop the out-of-scope fragments of
        # matching entities.
        if self.scope:
            entities.append(
                self._membership(self.table.c.dataset.in_(sorted(self.scope)))
            )
        return rows, entities

    @cached_property
    def clause(self) -> BooleanClauseList:
        rows, entities = self._clauses
        # `and_(true(), x)` collapses to `x`; an empty conjunction is `true`
        return and_(true(), *self._base_clauses, *rows, *entities)

    @cached_property
    def _all_entities(self) -> Any:
        """A predicate matching every row of the entities this query selects,
        ignoring any slice.

        When nothing is row-constrained the clause already says exactly that,
        so it is used as-is; otherwise the id set has to be materialized first.
        """
        rows, entities = self._clauses
        if rows:
            return self.id_col.in_(self.all_canonical_ids)
        return and_(true(), *entities)

    def _flat_clauses(self) -> tuple[list[Any], list[Any]]:
        """Compile a flat conjunction from the query's leaf collectors into
        `(row, entity)` clauses: one per field, AND-ed together (`_is_flat_and`
        guarantees at most one leaf per field)."""
        rows: list[Any] = []
        entities: list[Any] = []
        by_key: Callable[[Leaf], str] = lambda f: f.key  # noqa: E731
        # the different id fields (`id` / `entity_id` / `canonical_id`) are
        # separate fields and AND together like any other
        for f in sorted(self.q.ids, key=by_key):
            rows.append(self.get_expression(self._id_column(f), f))
        for f in self.q.datasets:  # at most one in a flat tree
            rows.append(self.get_expression(self.table.c.dataset, f))
        # exact-schema and is-a (`schemata`) filters; negations compile as
        # entity-level anti-joins
        schema_leaves = list(self.q.schemata) + [
            s for s in self.q._leaves if isinstance(s, SchemataLeaf)
        ]
        for f in schema_leaves:
            clause = self._schema_clause(f)
            if f.comparator in ("not", "not_in"):
                entities.append(clause)
            else:
                rows.append(clause)
        context_rows, context_entities = self._context_clauses()
        rows.extend(context_rows)
        entities.extend(context_entities)
        # properties and prop-type groups: one entity-level clause per field, so
        # they AND across fields ("has a name AND a german country"). A single
        # row predicate would instead force one statement row to satisfy every
        # field at once, which no row can - a row holds exactly one prop.
        for f in sorted(self.q.properties, key=by_key):
            entities.append(self._family_clause(f, self._prop_selector))
        # the reverse lookup `G(entities=...)` is not special here, it is just
        # the `entity` prop-type group
        for f in sorted(self.q.groups, key=by_key):
            entities.append(self._family_clause(f, self._group_selector))
        return rows, entities

    @property
    def _limit(self) -> int | None:
        # sqlalchemy renders an offset without a limit as `LIMIT -1`, which
        # duckdb rejects - emit an explicit no-op limit instead
        if self.q.limit is None and self.q.offset:
            return 2**63 - 1
        return self.q.limit

    @cached_property
    def canonical_ids(self) -> Select:
        q = select(self.id_col.distinct()).where(self.clause)
        if self.q.sort is None:
            # offset 0 (a start-less slice) is redundant; omit it from the SQL
            q = q.limit(self._limit).offset(self.q.offset or None)
        return q

    @cached_property
    def all_canonical_ids(self) -> Select:
        return self.canonical_ids.limit(None).offset(None)

    @cached_property
    def _unsorted_statements(self) -> Select:
        rows, entities = self._clauses
        # a slice (even offset-only or limit 0) must go through the
        # `canonical_ids` sub-select, where limit/offset are applied. So must
        # any mix of row and entity clauses, or only the row-matching rows of
        # the matching entities come back instead of the whole entity.
        if self.q.slice is not None or (rows and entities):
            where = and_(
                true(), *self._base_clauses, self.id_col.in_(self.canonical_ids)
            )
        else:
            # the clause is either purely entity-level (already true for every
            # row of a matching entity) or a deliberate row filter - either way
            # it needs no second pass
            where = self.clause
        return select(self.table).where(where).order_by(self.id_col)

    @cached_property
    def _sorted_statements(self) -> Select:
        prop = self.q.sort.value
        value = self.table.c.value
        if prop in NUMERIC_PROPS:
            value = numeric_value(self.table.c.value)
        group_func = func.min if self.q.sort.ascending else func.max
        inner = (
            select(
                self.id_col,
                group_func(value).label("sortable_value"),
            )
            .where(
                and_(
                    true(),
                    *self._base_clauses,
                    self.table.c.prop == prop,
                    self.id_col.in_(self.canonical_ids),
                )
            )
            .group_by(self.id_col)
            .limit(self._limit)
            .offset(self.q.offset or None)
        )
        inner_order = (
            "sortable_value" if self.q.sort.ascending else desc("sortable_value")
        )
        # an explicit subquery: reading `.c` off a `Select` builds one
        # implicitly, which sqlalchemy deprecates
        sub = inner.order_by(inner_order, self.id_col).subquery()
        sortable = sub.c["sortable_value"]
        outer = select(
            self.table.join(sub, self.id_col == sub.c[self.source.id_column])
        )
        # the join rows still need the base scope - a matching entity may
        # have out-of-scope statements
        if self._base_clauses:
            outer = outer.where(*self._base_clauses)
        return outer.order_by(
            sortable if self.q.sort.ascending else desc(sortable), self.id_col
        )

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

        One registration per ref family, so nothing has to recover a field's
        family from its name. A meta / context ref reads its own column and
        needs no row predicate (every statement of an entity carries it); a
        property or group ref reads the shared `value` column and selects its
        rows via `prop` / `prop_type`.
        """
        raise QueryError(f"Cannot compile field reference: `{ref!r}`")

    @lookup.register
    def _(self, ref: IdRef) -> Lookup:
        # `M("id")` addresses the *entity*: in a statement table that is the
        # resolved id column, not the `value` of a `prop = "id"` row (which
        # holds the unresolved referent id)
        return Lookup(self.id_col)

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
        # a derived dimension: the year is part of the value expression, so it
        # groups and filters like any other lookup
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
        self,
        group: Ref,
        limit: int | None = None,
        extra_where: BooleanClauseList | None = None,
    ) -> Select:
        count = func.count(self.id_col.distinct()).label("count")
        # group over the rows of matching entities (entity-level) so flat and
        # tree queries facet identically
        lookup = self.lookup(group)
        where = and_(true(), *self._base_clauses, *lookup.clauses, self._all_entities)
        if extra_where is not None:
            where = and_(where, extra_where)
        return (
            select(lookup.value, count)
            .where(where)
            .group_by(lookup.value)
            .order_by(desc(count))
            .limit(limit)
        )

    @cached_property
    def date_range(self) -> Select:
        return select(
            func.min(self.table.c.value),
            func.max(self.table.c.value),
        ).where(
            *self._base_clauses,
            self.table.c.prop_type == "date",
            self._all_entities,
        )

    def _aggregator(self, agg: Agg) -> Any:
        """The aggregate expression for one spec, over its ref's value."""
        value = self.lookup(agg.ref).value
        if agg.func == "count":
            # `count` stays over the raw values - it counts distinct readings,
            # which needs no arithmetic
            return func.count(distinct(value))
        if agg.ref.is_numeric:
            # min / max included: a lexicographic min over numbers is wrong,
            # and returning a string for min / max but a number for sum / avg
            # of the same property makes every consumer parse defensively
            value = numeric_value(value)
        return getattr(func, agg.func)(value)

    @cached_property
    def aggregations(self) -> Select:
        qs = []
        for agg in sorted(self.q.aggregations, key=lambda a: (a.func, a.key)):
            qs.append(
                select(
                    text(f"'{agg.key}'"),
                    text(f"'{agg.func}'"),
                    self._aggregator(agg),
                ).where(
                    *self._base_clauses,
                    *self.lookup(agg.ref).clauses,
                    self._all_entities,
                )
            )
        return union_all(*qs)

    def grouped_aggregations(self, grouper: Ref, limit: int | None = None) -> Select:
        """Every aggregation spec grouped by `grouper`, as one select per spec
        unioned to `(field, func, group_value, value)` rows.

        The specs' rows join against the distinct `(entity, group value)` pairs
        of the matching entities - distinct, so a multi-valued group property
        does not multiply the aggregated rows within its buckets. One round
        trip per grouper, instead of one per group value.

        Args:
            grouper: The field reference to group by.
            limit: Only the `limit` most frequent group values (by entity
                count, matching `get_group_counts`).
        """
        g = self.lookup(grouper)
        pairs = (
            select(self.id_col.label("cid"), g.value.label("gval"))
            .where(and_(true(), *self._base_clauses, *g.clauses, self._all_entities))
            .distinct()
        )
        if limit is not None:
            top = self.get_group_counts(grouper, limit=limit).subquery()
            pairs = pairs.where(g.value.in_(select(top.c[0])))
        sub = pairs.subquery()
        qs = []
        for agg in sorted(self.q.aggregations, key=lambda a: (a.func, a.key)):
            if grouper not in agg.groups:
                continue
            lookup = self.lookup(agg.ref)
            qs.append(
                select(
                    text(f"'{agg.key}'"),
                    text(f"'{agg.func}'"),
                    sub.c.gval,
                    self._aggregator(agg),
                )
                .select_from(self.table.join(sub, self.id_col == sub.c.cid))
                .where(and_(true(), *self._base_clauses, *lookup.clauses))
                .group_by(sub.c.gval)
            )
        return union_all(*qs)

    @cached_property
    def group_props(self) -> set[Ref]:
        refs: set[Ref] = set()
        for agg in self.q.aggregations:
            refs.update(agg.groups)
        return refs
