from functools import cache
from typing import TYPE_CHECKING, Literal, TypeAlias

from anystore.logging import get_logger
from fastapi import HTTPException

from ftmq.api.settings import settings
from ftmq.model import Catalog, Dataset
from ftmq.store import Store
from ftmq.store import get_store as _get_store
from ftmq.store.base import View, get_linker
from ftmq.types import Entity
from ftmq.util import get_dehydrated_entity, get_featured_entity

if TYPE_CHECKING:
    from ftmq.api.query import RetrieveParams

log = get_logger(__name__)


def get_store_datasets() -> set[str]:
    """The dataset names the configured store actually holds."""
    try:
        return set(get_store().scope.leaf_names)
    except Exception as e:
        # never make the catalog (and with it the app's import) depend on the
        # store being reachable - an unconfigured catalog degrades to empty
        log.error(f"Cannot read datasets from store: `{e}`", store=settings.store_uri)
        return set()


@cache
def get_catalog() -> Catalog:
    """The catalog of queryable datasets, reconciled with the store.

    `settings.catalog` *describes* datasets, but the store *decides* which
    exist. A name present in the store and missing from the catalog is added
    as a bare dataset, so a catalog naming something else - or no catalog at
    all - can never leave the store's own datasets un-queryable (the dataset
    filter validates against this catalog).
    """
    catalog = Catalog()
    if settings.catalog is not None:
        catalog = Catalog._from_uri(settings.catalog)
    undeclared = get_store_datasets() - catalog.names
    if undeclared:
        if catalog.names:
            log.warning(
                f"Datasets in store but not in catalog: `{', '.join(sorted(undeclared))}`",
                catalog=settings.catalog,
            )
        catalog.datasets = catalog.datasets + [
            Dataset(name=name, title=name) for name in sorted(undeclared)
        ]
    return catalog


@cache
def get_dataset(name: str) -> Dataset:
    catalog = get_catalog()
    dataset = catalog.get(name)
    if dataset is None:
        raise HTTPException(404, detail=[f"Dataset `{name}` not found."])
    return dataset


@cache
def get_store(dataset: str | None = None) -> Store:
    # a read-only api only needs the merge decisions, not the judgement
    # history: `resolver_uri` may be a sql database or a json edge dump.
    # Unset, the store falls back to a resolver table in its own database.
    linker = get_linker(settings.resolver_uri) if settings.resolver_uri else None
    # scoped by name: the store expects a runtime dataset, not the catalog model
    return _get_store(uri=settings.store_uri, dataset=dataset, linker=linker)


def shape(proxy: Entity, params: "RetrieveParams") -> Entity:
    """Dehydrate an entity, or reduce it to its featured properties, as asked."""
    if params.dehydrate:
        return get_dehydrated_entity(proxy)
    if params.featured:
        return get_featured_entity(proxy)
    return proxy


@cache
def get_view(dataset: str | None = None) -> View:
    return get_store(dataset).default_view()


def get_entity(entity_id: str, params: "RetrieveParams") -> Entity:
    """The entity by id, or by a referent id the store's linker resolves."""
    # the statements of a resolved store carry the canonical id
    view = get_view()
    proxy = view.get_entity(get_store().linker.get_canonical(entity_id))
    proxy = proxy or view.get_entity(entity_id)
    if proxy is None:
        raise HTTPException(404, detail=[f"Entity `{entity_id}` not found."])
    return shape(proxy, params)


# cache at boot time
catalog = get_catalog()
Datasets: TypeAlias = Literal[tuple(catalog.names or ["default"])]  # type: ignore[valid-type]
