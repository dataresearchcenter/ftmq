from functools import cached_property
from typing import TYPE_CHECKING, TypeAlias

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
from ftmq.filters import F

if TYPE_CHECKING:
    from ftmq.query import Query


Field: TypeAlias = Properties | PropertyTypes | Fields


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

    def __init__(self, q: "Query") -> None:
        self.q = q
        self.metadata = MetaData()
        if q.table is None:
            self.table = make_statement_table(self.metadata)
        else:
            self.table = q.table
        self.META_COLUMNS = {
            "id": self.table.c.canonical_id,
            "dataset": self.table.c.dataset,
            "schema": self.table.c.schema,
        }

    def get_expression(self, column: Column, f: F):
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
        if self.q.origins:
            clauses.append(
                or_(
                    self.get_expression(self.table.c.origin, f)
                    for f in sorted(self.q.origins)
                )
            )
        if self.q.reversed:
            rclause = or_(
                and_(
                    self.table.c.prop_type == str(registry.entity),
                    self.get_expression(self.table.c.value, f),
                )
                for f in sorted(self.q.reversed)
            )
            rq = select(self.table.c.canonical_id.distinct()).where(
                and_(rclause, *clauses)
            )
            clauses.append(self.table.c.canonical_id.in_(rq))
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
        return and_(*clauses)

    @cached_property
    def canonical_ids(self) -> Select:
        q = select(self.table.c.canonical_id.distinct()).where(self.clause)
        if self.q.sort is None:
            q = q.limit(self.q.limit).offset(self.q.offset)
        return q

    @cached_property
    def all_canonical_ids(self) -> Select:
        return self.canonical_ids.limit(None).offset(None)

    @cached_property
    def _unsorted_statements(self) -> Select:
        where = self.clause
        if self.q.properties or self.q.reversed or self.q.limit:
            where = self.table.c.canonical_id.in_(self.canonical_ids)
        return select(self.table).where(where).order_by(self.table.c.canonical_id)

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
                    self.table.c.canonical_id,
                    group_func(value).label("sortable_value"),
                )
                .where(
                    and_(
                        self.table.c.prop == prop,
                        self.table.c.canonical_id.in_(self.canonical_ids),
                    )
                )
                .group_by(self.table.c.canonical_id)
                .limit(self.q.limit)
                .offset(self.q.offset)
            )

            order_by = "sortable_value"
            if not self.q.sort.ascending:
                order_by = desc(order_by)
            order_by = [order_by, self.table.c.canonical_id]

            inner = inner.order_by(*order_by)

            return select(
                self.table.join(
                    inner, self.table.c.canonical_id == inner.c.canonical_id
                )
            ).order_by(*order_by)

    @cached_property
    def statements(self) -> Select:
        if self.q.sort:
            return self._sorted_statements
        return self._unsorted_statements

    @cached_property
    def count(self) -> Select:
        return (
            select(func.count(self.table.c.canonical_id.distinct()))
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
        count = func.count(self.table.c.canonical_id.distinct()).label("count")
        column = self._get_lookup_column(group)
        group = str(group)
        if group in self.META_COLUMNS:
            grouper = column
            where = self.clause
        else:
            grouper = self.table.c.value
            where = and_(
                column == group, self.table.c.canonical_id.in_(self.all_canonical_ids)
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
                self.table.c.prop_type == registry.country,
                self.table.c.canonical_id.in_(self.all_canonical_ids),
            )
        )

    @cached_property
    def things(self) -> Select:
        return self.get_group_counts(
            "schema", extra_where=self.table.c.schema.in_(Things)
        )

    @cached_property
    def things_countries(self) -> Select:
        return self.get_group_counts(
            registry.country, extra_where=self.table.c.schema.in_(Things)
        )

    @cached_property
    def intervals(self) -> Select:
        return self.get_group_counts(
            "schema", extra_where=self.table.c.schema.in_(Intervals)
        )

    @cached_property
    def intervals_countries(self) -> Select:
        return self.get_group_counts(
            registry.country, extra_where=self.table.c.schema.in_(Intervals)
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
            self.table.c.canonical_id.in_(self.all_canonical_ids),
        )

    @cached_property
    def aggregations(self) -> Select:
        qs = []
        for agg in self.q.aggregations:
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
                    self.table.c.prop == agg.prop,
                    self.table.c.canonical_id.in_(self.all_canonical_ids),
                )
            )
        return union_all(*qs)

    def _get_grouping_where(self, grouper: Field, value: str) -> BooleanClauseList:
        column = self._get_lookup_column(grouper)
        clauses = [self.table.c.canonical_id.in_(self.all_canonical_ids)]
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
        for agg in self.q.aggregations:
            if grouper in agg.group_props:
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

                inner = select(self.table.c.canonical_id.distinct()).where(
                    *self._get_grouping_where(grouper, group)
                )

                qs.append(
                    select(
                        text(f"'{agg.prop}'"),
                        text(f"'{agg.func}'"),
                        aggregator,
                    ).where(
                        self.table.c.prop == agg.prop,
                        self.table.c.canonical_id.in_(inner),
                    )
                )
        return union_all(*qs)

    @cached_property
    def group_props(self) -> set[Field]:
        props: set[Field] = set()
        for agg in self.q.aggregations:
            if agg.group_props:
                props.update(agg.group_props)
        return props
