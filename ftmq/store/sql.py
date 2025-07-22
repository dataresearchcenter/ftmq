import os
from collections import defaultdict
from decimal import Decimal

from anystore.util import clean_dict
from followthemoney.dataset.dataset import Dataset
from nomenklatura.db import get_metadata
from nomenklatura.store import sql as nk
from sqlalchemy import select

from ftmq.aggregations import AggregatorResult
from ftmq.enums import Fields
from ftmq.model.stats import Collector, DatasetStats
from ftmq.query import Query
from ftmq.store.base import Store, View
from ftmq.types import StatementEntities
from ftmq.util import get_scope_dataset

MAX_SQL_AGG_GROUPS = int(os.environ.get("MAX_SQL_AGG_GROUPS", 10))


def clean_agg_value(value: str | Decimal) -> str | float | int | None:
    if isinstance(value, Decimal):
        return float(value)
    return value


class SQLQueryView(View, nk.SQLView):
    def ensure_scoped_query(self, query: Query) -> Query:
        if not query.datasets:
            return query.where(dataset__in=self.dataset_names)
        if query.dataset_names - self.dataset_names:
            raise ValueError("Query datasets outside view scope")
        return query

    def query(self, query: Query | None = None) -> StatementEntities:
        if query:
            query = self.ensure_scoped_query(query)
            yield from self.store._iterate(query.sql.statements)
        else:
            view = self.store.view(self.scope)
            yield from view.entities()

    def stats(self, query: Query | None = None) -> DatasetStats:
        query = self.ensure_scoped_query(query or Query())
        key = f"stats-{hash(query)}"
        if key in self._cache:
            return self._cache[key]

        c = Collector()
        for schema, count in self.store._execute(query.sql.things, stream=False):
            c.things[schema] = count
        for schema, count in self.store._execute(query.sql.intervals, stream=False):
            c.intervals[schema] = count
        for country, count in self.store._execute(
            query.sql.things_countries, stream=False
        ):
            if country is not None:
                c.things_countries[country] = count
        for country, count in self.store._execute(
            query.sql.intervals_countries, stream=False
        ):
            if country is not None:
                c.intervals_countries[country] = count

        stats = c.export()
        for start, end in self.store._execute(query.sql.date_range, stream=False):
            if start:
                stats.start = start
            if end:
                stats.end = end
            break

        stats.entity_count = self.count(query)
        self._cache[key] = stats
        return stats

    def count(self, query: Query | None = None) -> int:
        if query is not None:
            for res in self.store._execute(query.sql.count, stream=False):
                for count in res:
                    return count
        return 0

    def aggregations(self, query: Query) -> AggregatorResult | None:
        if not query.aggregations:
            return
        query = self.ensure_scoped_query(query)
        key = f"agg-{hash(query)}"
        if key in self._cache:
            return self._cache[key]
        res: AggregatorResult = defaultdict(dict)

        for prop, func, value in self.store._execute(
            query.sql.aggregations, stream=False
        ):
            res[func][prop] = clean_agg_value(value)

        if query.sql.group_props:
            res["groups"] = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
            for prop in query.sql.group_props:
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
                            query.sql.get_group_counts(prop, limit=MAX_SQL_AGG_GROUPS),
                            stream=False,
                        )
                    ]
                for group in groups:
                    for agg_prop, func, value in self.store._execute(
                        query.sql.get_group_aggregations(prop, group), stream=False
                    ):
                        res["groups"][prop][func][agg_prop][group] = clean_agg_value(
                            value
                        )
        res = clean_dict(res)
        self._cache[key] = res
        return res


class SQLStore(Store, nk.SQLStore):
    def __init__(self, *args, **kwargs) -> None:
        get_metadata.cache_clear()  # FIXME
        super().__init__(*args, **kwargs)

    def get_scope(self) -> Dataset:
        q = select(self.table.c.dataset).distinct()
        names: set[str] = set()
        for row in self._execute(q, stream=False):
            names.add(row[0])
        return get_scope_dataset(*names)

    def view(
        self, scope: Dataset | None = None, external: bool = False
    ) -> SQLQueryView:
        scope = scope or self.dataset
        return SQLQueryView(self, scope, external=external)
