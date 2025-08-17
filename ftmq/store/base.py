from typing import Generator, Iterable
from urllib.parse import urlparse

from anystore.functools import weakref_cache as cache
from followthemoney import DefaultDataset
from followthemoney.dataset.dataset import Dataset
from nomenklatura import store as nk
from nomenklatura.db import get_engine
from nomenklatura.resolver import Resolver

from ftmq.aggregations import AggregatorResult
from ftmq.logging import get_logger
from ftmq.model.stats import Collector, DatasetStats
from ftmq.query import Query
from ftmq.similar import get_similar
from ftmq.types import StatementEntities, StatementEntity
from ftmq.util import ensure_dataset

log = get_logger(__name__)

DEFAULT_ORIGIN = "default"


@cache
def get_resolver(uri: str | None = None) -> Resolver[StatementEntity]:
    if uri and "sql" in urlparse(uri).scheme:
        return Resolver.make_default(get_engine(uri))
    return Resolver.make_default(get_engine("sqlite:///:memory:"))


class Store(nk.Store):
    """
    Feature add-ons to `nomenklatura.store.Store`
    """

    def __init__(
        self,
        dataset: Dataset | str | None = None,
        linker: Resolver | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize a store. This should be called via
        [`get_store`][ftmq.store.get_store]

        Args:
            dataset: A `followthemoney.Dataset` instance to limit the scope to
            linker: A `nomenklatura.Resolver` instance with linked / deduped data
        """
        dataset = ensure_dataset(dataset)
        linker = linker or get_resolver(kwargs.get("uri"))
        super().__init__(dataset=dataset, linker=linker, **kwargs)
        # implicit set all datasets as default store scope:
        if dataset == DefaultDataset and not dataset.leaf_names:
            self.dataset = self.get_scope()

    def get_scope(self) -> Dataset:
        """
        Return implicit `Dataset` computed from current datasets in store
        """
        raise NotImplementedError

    def iterate(self, dataset: str | Dataset | None = None) -> StatementEntities:
        """
        Iterate all the entities, optional filter for a dataset.

        Args:
            dataset: `Dataset` instance or name to limit scope to

        Yields:
            Generator of `nomenklatura.entity.CompositeEntity`
        """
        if dataset is not None:
            dataset = ensure_dataset(dataset)
            view = self.view(dataset)
        else:
            view = self.view(self.get_scope())
        yield from view.entities()


class View(nk.View):
    """
    Feature add-ons to `nomenklatura.store.base.View`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache = {}

    def query(self, query: Query | None = None) -> StatementEntities:
        """
        Get the entities of a store, optionally filtered by a
        [`Query`][ftmq.Query] object.

        Args:
            query: The Query filter object

        Yields:
            Generator of `followthemoney.StatementEntity`
        """
        view = self.store.view(self.scope)
        if query:
            yield from query.apply_iter(view.entities())
        else:
            yield from view.entities()

    def get_adjacents(
        self, proxies: Iterable[StatementEntity], inverted: bool | None = False
    ) -> set[StatementEntity]:
        seen: set[StatementEntity] = set()
        for proxy in proxies:
            for _, adjacent in self.get_adjacent(proxy, inverted=bool(inverted)):
                if adjacent.id not in seen:
                    seen.add(adjacent)
        return seen

    def stats(self, query: Query | None = None) -> DatasetStats:
        key = f"stats-{hash(query)}"
        if key in self._cache:
            return self._cache[key]
        c = Collector()
        cov = c.collect_many(self.query(query))
        self._cache[key] = cov
        return cov

    def count(self, query: Query | None = None) -> int:
        return self.stats(query).entity_count or 0

    def aggregations(self, query: Query) -> AggregatorResult | None:
        if not query.aggregations:
            return
        key = f"agg-{hash(query)}"
        if key in self._cache:
            return self._cache[key]
        _ = [x for x in self.query(query)]
        if query.aggregator:
            res = dict(query.aggregator.result)
            self._cache[key] = res
            return res

    def similar(
        self, entity_id: str, limit: int | None = None
    ) -> Generator[tuple[StatementEntity, float], None, None]:
        for candidate_id, score in get_similar(entity_id, self.store.linker, limit):
            entity = self.get_entity(candidate_id)
            if entity:
                yield entity, score
