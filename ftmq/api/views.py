from collections.abc import Iterable
from functools import cache
from typing import Annotated

from anystore.decorators import anycache
from anystore.store import Store, get_store
from anystore.util import make_data_checksum
from fastapi import HTTPException
from fastapi import Query as QueryField
from fastapi import Request
from fastapi.responses import RedirectResponse
from followthemoney import EntityProxy
from furl import furl

from ftmq.api.query import RetrieveParams, build_query
from ftmq.api.serialize import (
    AutocompleteResponse,
    EntitiesResponse,
    EntityResponse,
)
from ftmq.api.settings import Settings
from ftmq.api.store import get_catalog, get_dataset, get_view
from ftmq.model import Catalog, Dataset
from ftmq.query import QueryError
from ftmq.search.store import get_store as get_search_store
from ftmq.util import get_dehydrated_entity

settings = Settings()


def get_cache_key(request: Request, *args, **kwargs) -> str | None:
    if not settings.use_cache:
        return None
    f = furl(str(request.url))
    return f"{f.host}{f.path}/{make_data_checksum(f.querystr)}"


@cache
def get_cache() -> Store:
    return get_store(**settings.cache.model_dump())


def get_retrieve_params(
    nested: Annotated[
        bool, QueryField(description="Inline adjacent entities instead of their ids")
    ] = False,
    featured: Annotated[
        bool, QueryField(description="Only include featured properties and caption")
    ] = False,
    dehydrate: Annotated[
        bool, QueryField(description="Only include id, schema and caption")
    ] = False,
    dehydrate_nested: Annotated[
        bool, QueryField(description="Dehydrate nested entities")
    ] = True,
    stats: Annotated[
        bool, QueryField(description="Include statistics in response")
    ] = False,
) -> RetrieveParams:
    return RetrieveParams(
        nested=nested,
        featured=featured,
        dehydrate=dehydrate,
        dehydrate_nested=dehydrate_nested,
        stats=stats,
    )


@anycache(store=get_cache(), key_func=get_cache_key, model=Catalog)
def dataset_list(request: Request) -> Catalog:
    catalog = get_catalog()
    datasets: list[Dataset] = []
    for dataset in catalog.datasets:
        view = get_view(dataset.name)
        dataset.apply_stats(view.stats())
        datasets.append(dataset)
    catalog.datasets = datasets
    return catalog


@anycache(store=get_cache(), key_func=get_cache_key, model=Dataset)
def dataset_detail(request: Request, name: str) -> Dataset:
    view = get_view(name)
    dataset = get_dataset(name)
    dataset.apply_stats(view.stats())
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
                count=len(hits),
                query_q=q,
            )
        entities: list = []
        adjacents = []
        # `limit=0` returns only aggregations / stats (openaleph-style facets),
        # so the entity fetch is skipped
        if query.limit != 0:
            entities = [e for e in view.get_entities(query, retrieve_params)]
            if retrieve_params.nested:
                adjacents = view.get_adjacents(entities)
        aggregations = view.aggregations(query) if query.aggregations else None
        return EntitiesResponse.from_view(
            request=request,
            entities=entities,
            query=query,
            adjacents=adjacents,
            stats=view.stats(query) if retrieve_params.stats else None,
            count=view.count(query) if not retrieve_params.stats else 0,
            aggregations=aggregations,
        )
    except QueryError as e:
        raise HTTPException(400, detail=[str(e)])


@anycache(store=get_cache(), key_func=get_cache_key, serialization_mode="pickle")
def entity_detail(
    request: Request,
    entity_id: str,
    retrieve_params: RetrieveParams,
) -> EntityResponse | RedirectResponse:
    view = get_view()
    entity = view.get_entity(entity_id, retrieve_params)
    adjacents: Iterable[EntityProxy] = []
    if retrieve_params.nested:
        adjacents = [e[1] for e in view.get_adjacent(entity)]
        if retrieve_params.dehydrate_nested:
            adjacents = [get_dehydrated_entity(e) for e in adjacents]
    if entity.id != entity_id:  # we have a redirect to a merged entity
        url = furl(request.url)
        url.path.segments[-1] = entity.id
        response = RedirectResponse(url)
        response.headers["X-Entity-ID"] = entity.id
        response.headers["X-Entity-Schema"] = entity.schema.name
        return response
    return EntityResponse.from_entity(entity, adjacents)


@anycache(store=get_cache(), key_func=get_cache_key, model=AutocompleteResponse)
def autocomplete(request: Request, q: str) -> AutocompleteResponse:
    if q is None or len(q) < settings.min_search_length:
        raise HTTPException(400, [f"Invalid search query: `{q}`"])
    store = get_search_store()
    return AutocompleteResponse(candidates=store.autocomplete(q))
