from functools import cache
from typing import TYPE_CHECKING, Literal, TypeAlias

from anystore.logging import get_logger
from fastapi import HTTPException

from ftmq.api.settings import Settings
from ftmq.model import Catalog, Dataset
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.query.aggregations import AggregatorResult
from ftmq.store import Store
from ftmq.store import get_store as _get_store
from ftmq.types import Entities, Entity, StatementEntity
from ftmq.util import get_dehydrated_entity, get_featured_entity

if TYPE_CHECKING:
    from ftmq.api.query import RetrieveParams

log = get_logger(__name__)
settings = Settings()


@cache
def get_catalog() -> Catalog:
    if settings.catalog is not None:
        return Catalog._from_uri(settings.catalog)
    return Catalog()


@cache
def get_dataset(name: str) -> Dataset:
    catalog = get_catalog()
    dataset = catalog.get(name)
    if dataset is None:
        raise HTTPException(404, detail=[f"Dataset `{name}` not found."])
    return dataset


@cache
def get_store(dataset: str | None = None) -> Store:
    if dataset is not None:
        get_dataset(dataset)  # 404 guard against the catalog
        # scope by name: the ftmq store expects a runtime dataset (or its name),
        # not the pydantic catalog model
        return _get_store(uri=settings.store_uri, dataset=dataset)
    return _get_store(uri=settings.store_uri)


def retrieve_entities(entities: Entities, params: "RetrieveParams") -> Entities:
    for proxy in entities:
        if params.dehydrate:
            proxy = get_dehydrated_entity(proxy)
        elif params.featured:
            proxy = get_featured_entity(proxy)
        yield proxy


class View:
    """A wrapper around a store's default [`View`][ftmq.store.base.View] scoped
    to the api's use cases."""

    def __init__(self, dataset: str | None = None) -> None:
        self.store = get_store(dataset)
        self.dataset = dataset
        self.view = self.store.default_view()

    def get_entity(self, entity_id: str, params: "RetrieveParams") -> Entity:
        canonical = self.store.linker.get_canonical(entity_id)
        proxy = self.view.get_entity(canonical)
        if proxy is None:
            # fall back to the original id
            proxy = self.view.get_entity(entity_id)
            if proxy is None:
                raise HTTPException(404, detail=[f"Entity `{entity_id}` not found."])
        if params.dehydrate:
            return get_dehydrated_entity(proxy)
        if params.featured:
            return get_featured_entity(proxy)
        return proxy

    def get_entities(self, query: Query, params: "RetrieveParams") -> Entities:
        yield from retrieve_entities(self.view.query(query), params)

    def get_adjacents(self, proxies: Entities) -> set[StatementEntity]:
        return self.view.get_adjacents(proxies)

    def get_adjacent(self, proxy: StatementEntity):
        return self.view.get_adjacent(proxy)

    def stats(self, query: Query | None = None) -> DatasetStats:
        return self.view.stats(query)

    def count(self, query: Query | None = None) -> int:
        return self.view.count(query)

    def aggregations(self, query: Query) -> AggregatorResult | None:
        return self.view.aggregations(query)


@cache
def get_view(dataset: str | None = None) -> View:
    return View(dataset)


# cache at boot time
catalog = get_catalog()
Datasets: TypeAlias = Literal[tuple(catalog.names or ["default"])]  # type: ignore[valid-type]
