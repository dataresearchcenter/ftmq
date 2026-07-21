import os
from collections import defaultdict
from decimal import Decimal

from anystore.util import clean_dict
from followthemoney.dataset.dataset import Dataset
from nomenklatura.db import get_metadata
from nomenklatura.store import sql as nk
from sqlalchemy import select

from ftmq.enums import Fields
from ftmq.model.stats import DatasetStats, compile_stats
from ftmq.query import M, Query
from ftmq.query.aggregations import AggregatorResult
from ftmq.query.sql import Sql, SqlSource
from ftmq.store.base import Store, View
from ftmq.types import StatementEntities
from ftmq.util import get_scope_dataset

MAX_SQL_AGG_GROUPS = int(os.environ.get("MAX_SQL_AGG_GROUPS", 10))


def clean_agg_value(value: str | Decimal) -> str | float | int | None:
    if isinstance(value, Decimal):
        return float(value)
    return value


class SQLQueryView(View, nk.SQLView):
    store: "SQLStore"

    def _sql(self, query: Query) -> Sql:
        return Sql(query, self.store.source)

    def ensure_scoped_query(self, query: Query) -> Query:
        if not query.datasets:
            return query.where(M(dataset__in=self.dataset_names))
        if query.dataset_names - self.dataset_names:
            raise ValueError("Query datasets outside view scope")
        return query

    def query(self, query: Query | None = None) -> StatementEntities:
        if query:
            query = self.ensure_scoped_query(query)
            yield from self.store._iterate(self._sql(query).statements)
        else:
            view = self.store.view(self.scope)
            yield from view.entities()

    def stats(self, query: Query | None = None) -> DatasetStats:
        query = self.ensure_scoped_query(query or Query())

        sql = self._sql(query)

        def ex(sub):
            return self.store._execute(sub, stream=False)

        stats = compile_stats(
            things=ex(sql.things),
            intervals=ex(sql.intervals),
            things_countries=ex(sql.things_countries),
            intervals_countries=ex(sql.intervals_countries),
            date_range=next(iter(ex(sql.date_range)), None),
            entity_count=self.count(query),
        )
        return stats

    def count(self, query: Query | None = None) -> int:
        if query is not None:
            for res in self.store._execute(self._sql(query).count, stream=False):
                for count in res:
                    return count
        return 0

    def aggregations(self, query: Query) -> AggregatorResult | None:
        if not query.aggregations:
            return
        query = self.ensure_scoped_query(query)
        sql = self._sql(query)
        res: AggregatorResult = defaultdict(dict)

        for prop, func, value in self.store._execute(sql.aggregations, stream=False):
            res[func][prop] = clean_agg_value(value)

        if sql.group_props:
            res["groups"] = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
            for prop in sql.group_props:
                if prop == Fields.year:
                    start, end = self.stats(query).years
                    if start or end:
                        groups = range(start or end, (end or start) + 1)
                    else:
                        groups = []
                else:
                    groups = [
                        r[0]
                        for r in self.store._execute(
                            sql.get_group_counts(prop, limit=MAX_SQL_AGG_GROUPS),
                            stream=False,
                        )
                    ]
                for group in groups:
                    for agg_prop, func, value in self.store._execute(
                        sql.get_group_aggregations(prop, group), stream=False
                    ):
                        res["groups"][prop][func][agg_prop][group] = clean_agg_value(
                            value
                        )
        res = clean_dict(res)
        return res


class SQLStore(Store, nk.SQLStore):
    def __init__(self, *args, **kwargs) -> None:
        # nomenklatura caches a single global MetaData; clear it so
        # `make_statement_table` re-defines a fresh `statement` table instead
        # of raising on the already-registered one.
        get_metadata.cache_clear()
        super().__init__(*args, **kwargs)

    @property
    def source(self) -> SqlSource:
        """The SQL source (statement table) queries compile against."""
        return SqlSource(self.table)

    def get_scope(self) -> Dataset:
        q = select(self.table.c.dataset).distinct()
        names: set[str] = set()
        for row in self._execute(q, stream=False):
            names.add(row[0])
        return get_scope_dataset(*names)

    def view(
        self, scope: Dataset | None = None, external: bool = False
    ) -> "SQLQueryView":
        scope = scope or self.dataset
        return SQLQueryView(self, scope, external=external)
