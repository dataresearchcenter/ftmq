from collections import defaultdict
from decimal import Decimal

from anystore.util import clean_dict
from followthemoney import model
from followthemoney.dataset.dataset import Dataset
from nomenklatura.store import sql as nk
from sqlalchemy import and_, select

from ftmq.model.stats import DatasetStats, compile_stats
from ftmq.query import Query
from ftmq.query.aggregations import AggregatorResult
from ftmq.query.refs import GroupRef, SchemaRef
from ftmq.query.sql import Sql, SqlSource
from ftmq.store.base import Store, View, get_engine
from ftmq.types import StatementEntities, Statements
from ftmq.util import ensure_dataset, get_scope_dataset

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
        # the scope is an entity-level conjunct: out-of-scope datasets match nothing
        source = self.store.source
        if not self.external:
            # hide enrichment candidates, as nomenklatura's own views do
            rows = self.store.table.c.external.is_(False)
            if source.base_filter is not None:
                rows = and_(source.base_filter, rows)
            source = SqlSource(source.table, source.id_column, source.prune, rows)
        return Sql(query, source, scope=self.dataset_names)

    def query(self, query: Query | None = None) -> StatementEntities:
        if query:
            yield from self.store._iterate(self._sql(query).statements)
        else:
            yield from self.entities()

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
        query = query or Query()
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
                # one round trip per grouper, capped to its top buckets
                limit = query.get_facet_size(ref)
                grouped = sql.grouped_aggregations(ref, limit=limit)
                for field, func, group, value in self.store._execute(
                    grouped, stream=False
                ):
                    res["groups"][ref.wire][func][field][group] = clean_agg_value(value)
        res = clean_dict(res)
        return res


class SQLStore(Store, nk.SQLStore):
    view_class = SQLQueryView

    def __init__(self, *args, **kwargs) -> None:
        # nomenklatura takes an engine, not a uri
        kwargs["engine"] = get_engine(kwargs.get("uri"))
        super().__init__(*args, **kwargs)

    @property
    def source(self) -> SqlSource:
        """The SQL source (statement table) queries compile against."""
        return SqlSource(self.table)

    def statements(self, dataset: str | Dataset | None = None) -> Statements:
        """The stored statement rows in scope (see `Store.statements`)."""
        scope = ensure_dataset(dataset) if dataset is not None else self.scope
        q = select(self.table).where(self.table.c.dataset.in_(scope.leaf_names))
        yield from self._iterate_stmts(q)

    def get_scope(self) -> Dataset:
        q = select(self.table.c.dataset).distinct()
        names: set[str] = set()
        for row in self._execute(q, stream=False):
            names.add(row[0])
        return get_scope_dataset(*names)
