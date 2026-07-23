from collections import defaultdict
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, TypeAlias

from followthemoney.types import PropertyType, registry
from nomenklatura.db import make_statement_table
from sqlalchemy import (
    NUMERIC,
    BooleanClauseList,
    Column,
    MetaData,
    Select,
    and_,
    desc,
    distinct,
    func,
    or_,
    select,
    text,
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
from ftmq.query.leaves import ContextLeaf, Leaf, SchemataLeaf

if TYPE_CHECKING:
    from ftmq.query.main import Query


Field: TypeAlias = Properties | PropertyTypes | Fields

# a schema-value -> partition-value function (e.g. schema name -> `bucket`)
PruneFn: TypeAlias = Callable[[str], str]


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
    """

    def __init__(
        self,
        table: "Any",
        id_column: str = "canonical_id",
        prune: "dict[str, PruneFn] | None" = None,
        prune_column: str | None = None,
    ) -> None:
        self.table = table
        self.id_column = id_column
        self.prune = prune or {}
        self.prune_column = prune_column


class Sql:
    COMPARATORS = {
        Comparators["eq"]: "__eq__",
        Comparators["not"]: "__ne__",
        Comparators["in"]: "in_",
        Comparators.null: "is_",
        Comparators.gt: "__gt__",
        Comparators.gte: "__ge__",
        Comparators.lt: "__lt__",
        Comparators.lte: "__le__",
    }

    def __init__(self, q: "Query", source: "SqlSource | None" = None) -> None:
        self.q = q
        self.metadata = MetaData()
        if source is None:
            source = SqlSource(make_statement_table(self.metadata))
        self.source = source
        self.table = source.table
        self.id_col = self.table.c[source.id_column]
        self.META_COLUMNS = {
            "id": self.id_col,
            "dataset": self.table.c.dataset,
            "schema": self.table.c.schema,
        }

    def get_expression(self, column: Column, f: Leaf):
        value = f.value
        if f.comparator in (Comparators.ilike, Comparators.like):
            value = f"%{value}%"
        op = self.COMPARATORS.get(str(f.comparator), str(f.comparator))
        op = getattr(column, op)
        return op(value)

    @cached_property
    def clause(self) -> BooleanClauseList:
        clauses = []
        if self.q.ids:
            clauses.append(
                or_(
                    self.get_expression(self.table.c[f.key], f)
                    for f in sorted(self.q.ids)
                )
            )
        if self.q.datasets:
            clauses.append(
                or_(
                    self.get_expression(self.table.c.dataset, f)
                    for f in sorted(self.q.datasets)
                )
            )
        if self.q.schemata:
            clauses.append(
                or_(
                    self.get_expression(self.table.c.schema, f)
                    for f in sorted(self.q.schemata)
                )
            )
        # is-a schema filters (`M(schemata=...)`): expand to the schema plus its
        # non-abstract descendants and match on the `schema` column
        for f in sorted(s for s in self.q._leaves if isinstance(s, SchemataLeaf)):
            names: set[str] = set()
            for schema in f.schemata:
                names.add(schema.name)
                names.update(d.name for d in schema.descendants if not d.abstract)
            if str(f.comparator) in ("not", "not_in"):
                clauses.append(self.table.c.schema.not_in(names))
            else:
                clauses.append(self.table.c.schema.in_(names))
        # context / storage columns (`C(origin=...)`, `C(fragment=...)`, ...)
        context_by_key: dict[str, list[ContextLeaf]] = defaultdict(list)
        for f in self.q.context:
            context_by_key[f.key].append(f)
        for key, fs in context_by_key.items():
            if key not in self.table.c:
                raise QueryError(f"Unknown context column: `{key}`")
            clauses.append(
                or_(self.get_expression(self.table.c[key], f) for f in sorted(fs))
            )
        if self.q.properties:
            clauses.append(
                or_(
                    and_(
                        self.table.c.prop == f.key,
                        self.get_expression(self.table.c.value, f),
                    )
                    for f in sorted(self.q.properties)
                )
            )
        # prop-type groups: `G(countries=...)`, `G(entities=...)` (reverse), ...
        # Each is an entity-level membership ("the entity has a row of this
        # prop_type matching the value"), so it lifts to a `canonical_id`
        # subquery rather than a plain row predicate - the reverse `entities`
        # group is not special, it is just the `entity` prop-type. A row
        # predicate would force one row to satisfy both the group and any
        # property filter at once, which no single statement row can.
        if self.q.groups:
            gclause = or_(
                and_(
                    self.table.c.prop_type == str(f.prop_type),
                    self.get_expression(self.table.c.value, f),
                )
                for f in sorted(self.q.groups)
            )
            clauses.append(
                self.id_col.in_(select(self.id_col.distinct()).where(gclause))
            )
        # partition pruning: a schema/schemata filter narrows to the matching
        # partition values (e.g. the lake `bucket` column), folded into every
        # compiled query - so `count` prunes partitions too, not just statements
        prune_fn = self.source.prune.get("schema")
        if (
            prune_fn is not None
            and self.source.prune_column
            and self.q.schemata_names
            and self.source.prune_column in self.table.c
        ):
            values = {prune_fn(s) for s in self.q.schemata_names}
            clauses.append(self.table.c[self.source.prune_column].in_(values))
        return and_(*clauses)

    @cached_property
    def canonical_ids(self) -> Select:
        q = select(self.id_col.distinct()).where(self.clause)
        if self.q.sort is None:
            # offset 0 (a start-less slice) is redundant; omit it from the SQL
            q = q.limit(self.q.limit).offset(self.q.offset or None)
        return q

    @cached_property
    def all_canonical_ids(self) -> Select:
        return self.canonical_ids.limit(None).offset(None)

    @cached_property
    def _unsorted_statements(self) -> Select:
        where = self.clause
        if self.q.properties or self.q.groups or self.q.context or self.q.limit:
            where = self.id_col.in_(self.canonical_ids)
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
                        self.table.c.prop == prop,
                        self.id_col.in_(self.canonical_ids),
                    )
                )
                .group_by(self.id_col)
                .limit(self.q.limit)
                .offset(self.q.offset or None)
            )

            order_by = "sortable_value"
            if not self.q.sort.ascending:
                order_by = desc(order_by)
            order_by = [order_by, self.id_col]

            inner = inner.order_by(*order_by)

            return select(
                self.table.join(inner, self.id_col == inner.c.canonical_id)
            ).order_by(*order_by)

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
            grouper = column
            where = self.clause
        else:
            grouper = self.table.c.value
            where = and_(column == group, self.id_col.in_(self.all_canonical_ids))
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
                self.table.c.prop_type == str(registry.country),
                self.id_col.in_(self.all_canonical_ids),
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
            self.table.c.prop_type == "date",
            self.id_col.in_(self.all_canonical_ids),
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
                    self.table.c.prop == str(agg.prop),
                    self.id_col.in_(self.all_canonical_ids),
                )
            )
        return union_all(*qs)

    def _get_grouping_where(self, grouper: Field, value: str) -> BooleanClauseList:
        column = self._get_lookup_column(grouper)
        clauses = [self.id_col.in_(self.all_canonical_ids)]
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
