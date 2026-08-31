import os
from collections import defaultdict
from decimal import Decimal
from typing import Any

from anystore.util import clean_dict
from followthemoney import Statement, model
from followthemoney.dataset.dataset import Dataset
from nomenklatura.db import get_metadata
from nomenklatura.store import sql as nk
from sqlalchemy import select
from sqlalchemy.sql import Select

from ftmq.model.stats import DatasetStats, compile_stats
from ftmq.query import Query
from ftmq.query.aggregations import AggregatorResult
from ftmq.query.refs import GroupRef, SchemaRef
from ftmq.query.sql import Sql, SqlSource
from ftmq.store.base import Store, View
from ftmq.types import StatementEntities
from ftmq.util import get_scope_dataset

MAX_SQL_AGG_GROUPS = int(os.environ.get("MAX_SQL_AGG_GROUPS", 10))

# schema-name partitions of the model, for the dataset coverage stats
THINGS = sorted(k for k, s in model.schemata.items() if s.is_a("Thing"))
INTERVALS = sorted(k for k, s in model.schemata.items() if s.is_a("Interval"))


def clean_agg_value(value: str | Decimal) -> str | float | int | None:
    if isinstance(value, Decimal):
        return float(value)
    return value


class SQLQueryView(View, nk.SQLView):
    store: "SQLStore"

    def _sql(self, query: Query) -> Sql:
        # the view scope compiles to an entity-level membership conjunct, so
        # it composes with any dataset filters in the query - including
        # `~` / `|` trees. An out-of-scope dataset filter matches nothing.
        return Sql(query, self.store.source, scope=self.dataset_names)

    def query(self, query: Query | None = None) -> StatementEntities:
        if query:
            yield from self.store._iterate(self._sql(query).statements)
        else:
            view = self.store.view(self.scope)
            yield from view.entities()

    def stats(self, query: Query | None = None) -> DatasetStats:
        query = query or Query()
        sql = self._sql(query)
        things = sql.table.c.schema.in_(THINGS)
        intervals = sql.table.c.schema.in_(INTERVALS)
        countries = GroupRef("countries")

        def ex(sub):
            return self.store._execute(sub, stream=False)

        stats = compile_stats(
            things=ex(sql.get_group_counts(SchemaRef(), extra_where=things)),
            intervals=ex(sql.get_group_counts(SchemaRef(), extra_where=intervals)),
            things_countries=ex(sql.get_group_counts(countries, extra_where=things)),
            intervals_countries=ex(
                sql.get_group_counts(countries, extra_where=intervals)
            ),
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
        sql = self._sql(query)
        res: AggregatorResult = defaultdict(dict)

        for field, func, value in self.store._execute(sql.aggregations, stream=False):
            res[func][field] = clean_agg_value(value)

        if sql.group_props:
            res["groups"] = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
            for ref in sorted(sql.group_props):
                # one round trip per grouper: the select carries the group
                # value per row, capped to the most frequent group values
                grouped = sql.grouped_aggregations(ref, limit=MAX_SQL_AGG_GROUPS)
                for field, func, group, value in self.store._execute(
                    grouped, stream=False
                ):
                    res["groups"][ref.wire][func][field][group] = clean_agg_value(value)
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

    def _iterate(self, q: Select[Any], stream: bool = True) -> StatementEntities:
        """Assemble a statement row stream into entities, grouped by
        `canonical_id`.

        nomenklatura groups the stream by `entity_id` while every select
        feeding it orders by (or filters on) `canonical_id` - `SQLView.entities`,
        `SQLView.get_entity` and the compiled query above. A merged cluster
        then breaks apart into one partial entity per referent, all carrying
        the same canonical id, and `get_entity` returns whichever fragment
        comes first. Group by the column the rows are actually ordered by.

        Without merges the two keys are the same value, so this only changes
        what a store holding resolved data reads back.
        """
        stmts: list[Statement] = []
        current_id: str | None = None
        for stmt in self._iterate_stmts(q, stream=stream):
            if current_id is not None and stmt.canonical_id != current_id:
                entity = self.assemble(stmts)
                if entity is not None:
                    yield entity
                stmts = []
            current_id = stmt.canonical_id
            stmts.append(stmt)
        entity = self.assemble(stmts)
        if entity is not None:
            yield entity

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
