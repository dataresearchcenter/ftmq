from functools import cache

from anystore.decorators import anycache
from anystore.store import Store, get_store
from anystore.util import make_data_checksum
from fastapi import HTTPException, Request
from fastapi.responses import RedirectResponse

from ftmq.api.query import RetrieveParams, build_query
from ftmq.api.serialize import (
    AutocompleteResponse,
    EntitiesResponse,
    EntityResponse,
    with_bucket_counts,
)
from ftmq.api.settings import settings
from ftmq.api.store import get_catalog, get_dataset, get_entity, get_view, shape
from ftmq.model import Catalog, Dataset
from ftmq.query import QueryError
from ftmq.search.store import get_store as get_search_store
from ftmq.store.base import View
from ftmq.types import Entity
from ftmq.util import get_dehydrated_entity


def get_cache_key(request: Request, *args, **kwargs) -> str | None:
    if not settings.use_cache:
        return None
    url = request.url
    return f"{url.hostname}{url.path}/{make_data_checksum(url.query)}"


@cache
def get_cache() -> Store:
    return get_store(**settings.cache.model_dump())


def get_nested(view: View, entities: list[Entity], params: RetrieveParams) -> list:
    """The entities the given ones reference, to inline with `nested=true`."""
    if not params.nested:
        return []
    adjacents = view.get_adjacents(entities)
    if params.dehydrate_nested:
        return [get_dehydrated_entity(e) for e in adjacents]
    return list(adjacents)


@anycache(store=get_cache(), key_func=get_cache_key, model=Catalog)
def dataset_list(request: Request) -> Catalog:
    catalog = get_catalog()
    for dataset in catalog.datasets:
        dataset.apply_stats(get_view(dataset.name).stats())
    return catalog


@anycache(store=get_cache(), key_func=get_cache_key, model=Dataset)
def dataset_detail(request: Request, name: str) -> Dataset:
    dataset = get_dataset(name)
    dataset.apply_stats(get_view(name).stats())
    return dataset


@anycache(store=get_cache(), key_func=get_cache_key, model=EntitiesResponse)
def entity_list(
    request: Request,
    retrieve_params: RetrieveParams,
    authenticated: bool | None = False,
) -> EntitiesResponse:
    view = get_view()
    try:
        query = build_query(request, authenticated)
        q = request.query_params.get("q")
        if q:
            # a `q` term routes to full-text search via ftmq.search: dehydrated,
            # relevance-ranked hits (no store-side aggregations / stats)
            if len(q) < settings.min_search_length:
                raise HTTPException(400, [f"Invalid search query: `{q}`"])
            hits = [e.to_proxy() for e in get_search_store().search(q, query)]
            return EntitiesResponse.from_view(
                request=request,
                entities=hits,
                query=query,
                total=len(hits),
                query_q=q,
            )
        entities: list[Entity] = []
        # `limit=0` returns only aggregations / stats (openaleph-style facets),
        # so the entity fetch is skipped
        if query.limit != 0:
            entities = [shape(e, retrieve_params) for e in view.query(query)]
        stats = view.stats(query) if retrieve_params.stats else None
        return EntitiesResponse.from_view(
            request=request,
            entities=entities,
            query=query,
            adjacents=get_nested(view, entities, retrieve_params),
            stats=stats,
            total=stats.entity_count if stats else view.count(query),
            aggregations=view.aggregations(with_bucket_counts(query)),
        )
    except QueryError as e:
        raise HTTPException(400, detail=[str(e)])


@anycache(store=get_cache(), key_func=get_cache_key, model=EntityResponse)
def entity_response(
    request: Request,
    entity_id: str,
    retrieve_params: RetrieveParams,
) -> EntityResponse:
    entity = get_entity(entity_id, retrieve_params)
    adjacents = get_nested(get_view(), [entity], retrieve_params)
    return EntityResponse.from_proxy(entity, adjacents)


def entity_detail(
    request: Request,
    entity_id: str,
    retrieve_params: RetrieveParams,
) -> EntityResponse | RedirectResponse:
    entity = entity_response(request, entity_id, retrieve_params)
    if entity.id != entity_id:  # merged into another entity
        path = f"{request.url.path.rsplit('/', 1)[0]}/{entity.id}"
        response = RedirectResponse(str(request.url.replace(path=path)))
        response.headers["X-Entity-ID"] = entity.id
        response.headers["X-Entity-Schema"] = entity.schema_
        return response
    return entity


@anycache(store=get_cache(), key_func=get_cache_key, model=AutocompleteResponse)
def autocomplete(request: Request, q: str) -> AutocompleteResponse:
    if len(q) < settings.min_search_length:
        raise HTTPException(400, [f"Invalid search query: `{q}`"])
    store = get_search_store()
    return AutocompleteResponse(candidates=store.autocomplete(q))
