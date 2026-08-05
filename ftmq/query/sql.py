from collections import defaultdict
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Iterable, TypeAlias

from banal import as_bool
from followthemoney.types import PropertyType, registry
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

from ftmq.enums import (
    Aggregations,
    Comparators,
    Fields,
    Intervals,
    Properties,
    PropertyTypes,
    PropertyTypesMap,
    Things,
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
from ftmq.query.nodes import OR, Expr

if TYPE_CHECKING:
    from ftmq.query.main import Query


Field: TypeAlias = Properties | PropertyTypes | Fields

# a schema-value -> partition-value function (e.g. schema name -> `bucket`)
PruneFn: TypeAlias = Callable[[str], str]


def _by_key(leaves: "Iterable[Any]") -> "dict[str, list[Any]]":
    """Group leaves by the field they filter on."""
    grouped: dict[str, list[Any]] = defaultdict(list)
    for f in leaves:
        grouped[f.key].append(f)
    return grouped


class SqlSource:
    """Describes the SQL statement source a [`Query`][ftmq.Query] compiles
    against: the SQLAlchemy table (or view), the entity-identity column, and an
    optional partition-pruning rule.

    Stores own one and pass it to [`Sql`][ftmq.query.sql.Sql] /
    [`Query.compile`][ftmq.Query.compile], replacing the old
    `query.table` mutation. A downstream store with extra columns (a lake /
    sharded table) supplies its own `SqlSource` so the same `Query` compiles
    against it unchanged.

    Args:
        table: The SQLAlchemy `Table` / `TableClause` to query.
        id_column: The entity-identity column name (default `canonical_id`).
        prune: Optional `{meta_field: fn}` mapping folding a partition filter
            into every compiled query - e.g. `{"schema": get_schema_bucket}`
            maps a schema/schemata filter to a `prune_column IN (...)` predicate.
        prune_column: The partition column the `prune` values target
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
        prune: "dict[str, PruneFn] | None" = None,
        prune_column: str | None = None,
        base_filter: "Any | None" = None,
    ) -> None:
        self.table = table
        self.id_column = id_column
        self.prune = prune or {}
        self.prune_column = prune_column
        self.base_filter = base_filter


class Sql:
    COMPARATORS = {
        Comparators["eq"]: "__eq__",
        Comparators["not"]: "__ne__",
        Comparators["in"]: "in_",
        Comparators.gt: "__gt__",
        Comparators.gte: "__ge__",
        Comparators.lt: "__lt__",
        Comparators.lte: "__le__",
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
        self.META_COLUMNS = {
            "id": self.id_col,
            "dataset": self.table.c.dataset,
            "schema": self.table.c.schema,
        }

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
        c = str(f.comparator)
        if c == "null":
            # `null` tests presence, not a value: `null=True` means the column
            # is unset. (For the `prop` / `prop_type` families presence is a
            # row-existence test, handled in `clause`.)
            return column.is_(None) if f.value else column.is_not(None)
        if c == "between":
            # the leaf layer casts values to a single scalar, so a two-bound
            # between cannot be expressed yet (in-memory raises the same)
            raise QueryError(f"Comparator not implemented: `{c}`")
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
        return str(f.comparator) == Comparators.null

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

    def _family_clauses(
        self, leaves: "list[Any]", selector: Callable[[Any], Any]
    ) -> tuple[list[Any], list[Any]]:
        """Split leaves over one statement family (`prop` / `prop_type`) into
        row predicates to OR together, plus entity-level absence clauses.

        `selector` builds the family predicate for a leaf (e.g.
        `prop = "name"`). `null` tests presence of such a row, not the value:
        `null=False` is any row for the family, `null=True` the absence of one.
        """
        rows: list[Any] = []
        absent: list[Any] = []
        for f in leaves:
            family = selector(f)
            if self._is_null(f):
                if f.value:
                    absent.append(self._absent(family))
                else:
                    rows.append(family)
            else:
                rows.append(and_(family, self.get_expression(self.table.c.value, f)))
        return rows, absent

    def _membership(self, rows: list[Any]) -> Any:
        """Lift row predicates to an entity-level membership clause: the entity
        has at least one row matching any of them."""
        return self.id_col.in_(self._entity_ids(or_(*rows)))

    def _family_clause(
        self, leaves: "list[Any]", selector: Callable[[Any], Any]
    ) -> Any:
        """One entity-level clause for the leaves of a single family key
        (e.g. every `P(name=...)` leaf), OR-ed together."""
        rows, absent = self._family_clauses(leaves, selector)
        clauses = list(absent)
        if rows:
            clauses.append(self._membership(rows))
        return and_(*clauses)

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
        negated = str(f.comparator) in ("not", "not_in")
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
        (`C(origin=...)`, `C(fragment=...)`, ...), OR-ed per column.

        A single column stays a row predicate; several distinct columns each
        lift to an entity-level membership - in-memory a context value is the
        entity's aggregate over its statements, so `C(origin=..) & C(lang=..)`
        may be satisfied by two different rows.
        """
        row_clauses: list[Any] = []
        entity_clauses: list[Any] = []
        context_by_key: dict[str, list[ContextLeaf]] = defaultdict(list)
        for f in self.q.context:
            context_by_key[f.key].append(f)
        entity_level = len(context_by_key) > 1
        for _, fs in sorted(context_by_key.items()):
            rows = []
            for f in sorted(fs):
                # "unset" means no row carries the column
                if self._is_null(f) and f.value:
                    entity_clauses.append(
                        self._absent(self._context_column(f).is_not(None))
                    )
                else:
                    rows.append(self.get_expression(self._context_column(f), f))
            if rows:
                row_clause = or_(*rows)
                if entity_level:
                    entity_clauses.append(self._membership([row_clause]))
                else:
                    row_clauses.append(row_clause)
        return row_clauses, entity_clauses

    def _leaf_clause(self, leaf: Leaf) -> Any:
        """An entity-level predicate for a single leaf.

        Lifting every leaf to entity level (`canonical_id IN (...)`) is what
        makes an arbitrary `& | ~` tree composable: `OR` / `NOT` over row
        predicates would ask a single statement row a question about the whole
        entity ("this entity has no name" is not a property of any one row).
        """
        if isinstance(leaf, PropertyLeaf):
            return self._family_clause([leaf], self._prop_selector)
        if isinstance(leaf, GroupLeaf):
            return self._family_clause([leaf], self._group_selector)
        if isinstance(leaf, (SchemaLeaf, SchemataLeaf)):
            clause = self._schema_clause(leaf)
            if str(leaf.comparator) in ("not", "not_in"):
                return clause  # already an entity-level anti-join
            return self._membership([clause])
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
        return self._membership([row])

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
        prune_fn = self.source.prune.get("schema")
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
                if str(f.comparator) not in ("eq", "in"):
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
                self.id_col.in_(
                    self._entity_ids(self.table.c.dataset.in_(sorted(self.scope)))
                )
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
        `(row, entity)` clauses: one per field, AND-ed together. `_is_flat_and`
        guarantees at most one leaf per field here."""
        rows: list[Any] = []
        entities: list[Any] = []
        # the different id fields (`id` / `entity_id` / `canonical_id`) are
        # separate fields and AND together like any other
        for _, fs in sorted(_by_key(self.q.ids).items()):
            rows.append(
                or_(self.get_expression(self._id_column(f), f) for f in sorted(fs))
            )
        for f in sorted(self.q.datasets):
            rows.append(self.get_expression(self.table.c.dataset, f))
        # exact-schema and is-a (`schemata`) filters; negations compile as
        # entity-level anti-joins
        schema_leaves = sorted(self.q.schemata) + sorted(
            s for s in self.q._leaves if isinstance(s, SchemataLeaf)
        )
        for f in schema_leaves:
            clause = self._schema_clause(f)
            if str(f.comparator) in ("not", "not_in"):
                entities.append(clause)
            else:
                rows.append(clause)
        context_rows, context_entities = self._context_clauses()
        rows.extend(context_rows)
        entities.extend(context_entities)
        # properties and prop-type groups: one entity-level clause per field, so
        # they AND across fields ("has a name AND a german country") while the
        # leaves of one field OR together. A single row predicate would instead
        # force one statement row to satisfy every field at once, which no row
        # can - a row holds exactly one prop.
        for _, fs in sorted(_by_key(self.q.properties).items()):
            entities.append(self._family_clause(sorted(fs), self._prop_selector))
        # the reverse lookup `G(entities=...)` is not special here, it is just
        # the `entity` prop-type group
        for _, fs in sorted(_by_key(self.q.groups).items()):
            entities.append(self._family_clause(sorted(fs), self._group_selector))
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
        rows, _ = self._clauses
        # a slice (even offset-only or limit 0) must go through the
        # `canonical_ids` sub-select, where limit/offset are applied. So must a
        # row-constrained filter on prop / prop_type / context rows, or only
        # the matching rows come back instead of the whole entity.
        row_constrained = bool(rows) and bool(
            self.q.properties or self.q.groups or self.q.context
        )
        if self.q.slice is not None or row_constrained:
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
        if self.q.sort:
            if len(self.q.sort.values) > 1:
                raise ValueError(
                    f"Multi-valued sort not supported for `{self.__class__.__name__}`"
                )
            prop = self.q.sort.values[0]
            value = self.table.c.value
            if PropertyTypesMap[prop].value == registry.number:
                value = func.cast(self.table.c.value, NUMERIC)
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

            order_by = "sortable_value"
            if not self.q.sort.ascending:
                order_by = desc(order_by)

            # an explicit subquery: reading `.c` off a `Select` builds one
            # implicitly, which sqlalchemy deprecates
            sub = inner.order_by(order_by, self.id_col).subquery()
            sortable = sub.c["sortable_value"]
            order_by = [
                sortable if self.q.sort.ascending else desc(sortable),
                self.id_col,
            ]

            outer = select(
                self.table.join(sub, self.id_col == sub.c[self.source.id_column])
            )
            # the join rows still need the base scope - a matching entity may
            # have out-of-scope statements
            if self._base_clauses:
                outer = outer.where(*self._base_clauses)
            return outer.order_by(*order_by)

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

    def _get_lookup_column(self, field: Field) -> Column:
        if field in self.META_COLUMNS:
            return self.META_COLUMNS[field]
        if isinstance(field, PropertyType):
            return self.table.c.prop_type
        if field in Properties:
            return self.table.c.prop
        if field in PropertyTypes or field == Fields.year:
            return self.table.c.prop_type
        raise NotImplementedError("Unknown field: `%s`" % field)

    def get_group_counts(
        self,
        group: Field,
        limit: int | None = None,
        extra_where: BooleanClauseList | None = None,
    ) -> Select:
        count = func.count(self.id_col.distinct()).label("count")
        column = self._get_lookup_column(group)
        group = str(group)
        if group in self.META_COLUMNS:
            # group over the rows of matching entities (entity-level, like the
            # value groupers below) so flat and tree queries facet identically
            grouper = column
            where = and_(true(), *self._base_clauses, self._all_entities)
        else:
            grouper = self.table.c.value
            where = and_(
                true(),
                *self._base_clauses,
                column == group,
                self._all_entities,
            )
        if extra_where is not None:
            where = and_(where, extra_where)
        return (
            select(grouper, count)
            .where(where)
            .group_by(grouper)
            .order_by(desc(count))
            .limit(limit)
        )

    @cached_property
    def datasets(self) -> Select:
        return self.get_group_counts("dataset")

    @cached_property
    def schemata(self) -> Select:
        return self.get_group_counts("schema")

    @cached_property
    def countries(self) -> Select:
        return self.get_group_counts(registry.country)

    @cached_property
    def countries_flat(self) -> Select:
        return select(self.table.c.value.distinct()).where(
            and_(
                true(),
                *self._base_clauses,
                self.table.c.prop_type == str(registry.country),
                self._all_entities,
            )
        )

    @cached_property
    def things(self) -> Select:
        return self.get_group_counts(
            "schema", extra_where=self.table.c.schema.in_([str(x) for x in Things])
        )

    @cached_property
    def things_countries(self) -> Select:
        return self.get_group_counts(
            registry.country,
            extra_where=self.table.c.schema.in_([str(x) for x in Things]),
        )

    @cached_property
    def intervals(self) -> Select:
        return self.get_group_counts(
            "schema", extra_where=self.table.c.schema.in_([str(x) for x in Intervals])
        )

    @cached_property
    def intervals_countries(self) -> Select:
        return self.get_group_counts(
            registry.country,
            extra_where=self.table.c.schema.in_([str(x) for x in Intervals]),
        )

    @cached_property
    def dates(self) -> Select:
        return self.get_group_counts(registry.date)

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

    @cached_property
    def aggregations(self) -> Select:
        qs = []
        for agg in sorted(self.q.aggregations, key=lambda a: (a.func, a.prop)):
            sql_agg = getattr(func, agg.func)
            sql_agg_value = self.table.c.value
            if agg.func == Aggregations.count:
                sql_agg_value = distinct(sql_agg_value)
            elif agg.func in (Aggregations.sum, Aggregations.avg):
                sql_agg_value = func.cast(sql_agg_value, NUMERIC)
            aggregator = sql_agg(sql_agg_value)
            qs.append(
                select(
                    text(f"'{agg.prop}'"),
                    text(f"'{agg.func}'"),
                    aggregator,
                ).where(
                    *self._base_clauses,
                    self.table.c.prop == str(agg.prop),
                    self._all_entities,
                )
            )
        return union_all(*qs)

    def _get_grouping_where(self, grouper: Field, value: str) -> BooleanClauseList:
        column = self._get_lookup_column(grouper)
        clauses = [*self._base_clauses, self._all_entities]
        if grouper in Properties:
            clauses.extend([column == str(grouper), self.table.c.value == value])
            return clauses
        if grouper == Fields.year:
            clauses.extend(
                [
                    column == str(registry.date),
                    func.substring(self.table.c.value, 1, 4) == str(value),
                ]
            )
            return clauses
        clauses.append(column == value)
        return clauses

    def get_group_aggregations(self, grouper: Field, group: str) -> Select:
        qs = []
        for agg in sorted(self.q.aggregations, key=lambda a: (a.func, a.prop)):
            if grouper in agg.groups:
                if agg.prop in self.META_COLUMNS:
                    sql_agg_value = self._get_lookup_column(agg.prop)
                else:
                    sql_agg_value = self.table.c.value
                sql_agg = getattr(func, agg.func)
                if agg.func == Aggregations.count:
                    sql_agg_value = distinct(sql_agg_value)
                elif agg.func in (Aggregations.sum, Aggregations.avg):
                    sql_agg_value = func.cast(sql_agg_value, NUMERIC)
                aggregator = sql_agg(sql_agg_value)

                inner = select(self.id_col.distinct()).where(
                    *self._get_grouping_where(grouper, group)
                )

                qs.append(
                    select(
                        text(f"'{agg.prop}'"),
                        text(f"'{agg.func}'"),
                        aggregator,
                    ).where(
                        *self._base_clauses,
                        self.table.c.prop == str(agg.prop),
                        self.id_col.in_(inner),
                    )
                )
        return union_all(*qs)

    @cached_property
    def group_props(self) -> set[Field]:
        props: set[Field] = set()
        for agg in self.q.aggregations:
            if agg.groups:
                props.update(agg.groups)
        return props
