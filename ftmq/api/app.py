import secrets
from typing import Annotated

from anystore.io import smart_read
from anystore.logging import get_logger
from fastapi import Depends, FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from ftmq import __version__
from ftmq.api import views
from ftmq.api.query import RetrieveParams
from ftmq.api.serialize import (
    AutocompleteResponse,
    EntitiesResponse,
    EntityResponse,
    ErrorResponse,
)
from ftmq.api.settings import DEFAULT_DESCRIPTION, Settings
from ftmq.api.store import Datasets
from ftmq.model import Catalog, Dataset

log = get_logger(__name__)
settings = Settings()


def get_description() -> str:
    if settings.info.description_uri:
        return smart_read(settings.info.description_uri)
    return DEFAULT_DESCRIPTION


app = FastAPI(
    debug=settings.debug,
    title=settings.info.title,
    contact=settings.info.contact.model_dump(),
    description=get_description(),
    redoc_url="/",
    version=__version__,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[*settings.allowed_origin, "http://localhost:3000"],
    allow_methods=["OPTIONS", "GET"],
)

log.info("Ftm store: %s" % settings.store_uri)


@app.get(
    "/catalog",
    response_model=Catalog,
    responses={
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def dataset_list(request: Request) -> Catalog:
    """
    Show metadata for catalog (as described in
    [followthemoney.Dataset](https://followthemoney.tech))

    This is basically a list of the available dataset within this api instance.
    """
    return views.dataset_list(request)


@app.get(
    "/catalog/{dataset}",
    response_model=Dataset,
    responses={
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def dataset_detail(request: Request, dataset: Datasets) -> Dataset:
    """
    Show metadata for given dataset (as described in
    [followthemoney.Dataset](https://followthemoney.tech))
    """
    return views.dataset_detail(request, dataset)


def get_authenticated(
    api_key: Annotated[
        str | None,
        Query(
            description="Secret api key to increase limit "
            "(useful for e.g. static site builders)"
        ),
    ] = None,
) -> bool:
    if not api_key:
        return False
    return secrets.compare_digest(api_key, settings.build_api_key)


@app.get(
    "/entities",
    response_model=EntitiesResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def entities(
    request: Request,
    retrieve_params: Annotated[RetrieveParams, Depends(views.get_retrieve_params)],
    authenticated: Annotated[bool, Depends(get_authenticated)],
) -> EntitiesResponse:
    """
    Retrieve a paginated list of entities based on filter criteria.

    Optionally inline (nest) adjacent entities.

    Entities can be "dehydrated", that means only their featured properties are
    returned. This is e.g. useful for static site builders to reduce the data
    amount.

    Filtering uses the Aleph / OpenAleph filter grammar.

    ## dataset scope

    Limit entities filter to one or more datasets from the catalog:

    `/entities?filter:dataset=my_dataset&filter:dataset=another_dataset`

    ## filter by schema and properties

    `/entities?filter:schema=Company&filter:properties.country=de`

    Use `filter:schemata=` for schema is-a matching including descendants
    (e.g. `filter:schemata=LegalEntity` includes companies, people, ...).

    Filtering works for all [FollowTheMoney](https://followthemoney.tech/explorer/)
    properties via `filter:properties.<name>=`, property-type groups via their
    group name (e.g. `filter:countries=de`, `filter:entities=<id>` for reverse
    lookups), and comparator prefixes:

    * range: `filter:gte:properties.date=2023`, `filter:lt:properties.amountEur=1000`
    * substring: `filter:ilike:properties.name=jane`
    * prefix: `filter:startswith:canonical_id=eu-`
    * negation: `exclude:properties.jurisdiction=eu`
    * absence: `empty:properties.deathDate=`

    ## sorting

    `?sort={prop}` or `?sort={prop}:desc`

    [Numeric](https://followthemoney.tech/explorer/types/number/)
    property types are casted via sql `CAST(value AS NUMERIC)` (ignoring
    errors, results in 0) before sorting, and the first property in the value
    array is used as the sorting value. (The entity property dict remains
    uncasted, aka all properties are multi values as string)

    ## pagination

    `?limit=100&offset=200`

    ## aggregations

    Aggregations ride on the same query (as in the Aleph api). Request metrics
    and optionally group them by properties or fields (`id`, `dataset`,
    `schema`, `year`):

        ?metric:sum=amountEur&metric:count=id&facet=year

    Set `limit=0` to return only the aggregations (plus `total` / `stats`), no
    entities:

        ?filter:schema=Payment&metric:sum=amountEur&limit=0

    ## searching

    A `q` term routes the query to full-text search via `ftmq.search`
    (relevance-ranked, dehydrated hits), combined with the same filters:

        ?q=jane+doe&filter:dataset=my_dataset&filter:countries=de

    Autocomplete on entity names is at `/autocomplete?q=<term>`.
    """
    return views.entity_list(request, retrieve_params, authenticated=authenticated)


@app.get(
    "/entities/{entity_id}",
    response_model=EntityResponse,
    responses={
        307: {"description": "The entity was merged into another ID"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def detail_entity(
    request: Request,
    entity_id: str,
    retrieve_params: Annotated[RetrieveParams, Depends(views.get_retrieve_params)],
) -> EntityResponse | RedirectResponse | ErrorResponse:
    """
    Retrieve a single entity.

    Optionally inline (nest) adjacent entities.

    If the requested entity was merged into another entity, a redirect to the
    new api endpoint is returned with additional headers to allow client side
    logic:

        `x-entity-id` - the new entity id
        `x-entity-schema` - the new entity schema
    """
    return views.entity_detail(request, entity_id, retrieve_params)


@app.get(
    "/autocomplete",
    response_model=AutocompleteResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def autocomplete(request: Request, q: str) -> AutocompleteResponse:
    """
    Simple autocomplete by names
    """
    return views.autocomplete(request, q)
