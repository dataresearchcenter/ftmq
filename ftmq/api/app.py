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
from ftmq.api.settings import DEFAULT_DESCRIPTION, settings
from ftmq.api.store import Datasets
from ftmq.model import Catalog, Dataset

log = get_logger(__name__)


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
    responses={500: {"model": ErrorResponse, "description": "Server error"}},
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origin,
    allow_methods=["OPTIONS", "GET"],
)

log.info("Ftm store: %s" % settings.store_uri)


@app.get(
    "/catalog",
    response_model=Catalog,
)
def dataset_list(request: Request) -> Catalog:
    """
    The catalog: metadata of all datasets in this api instance.
    """
    return views.dataset_list(request)


@app.get(
    "/catalog/{dataset}",
    response_model=Dataset,
)
def dataset_detail(request: Request, dataset: Datasets) -> Dataset:
    """
    Metadata of a single dataset.
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
    if not api_key or not settings.build_api_key:
        return False
    return secrets.compare_digest(api_key, settings.build_api_key)


@app.get(
    "/entities",
    response_model=EntitiesResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
    },
)
def entities(
    request: Request,
    retrieve_params: Annotated[RetrieveParams, Depends()],
    authenticated: Annotated[bool, Depends(get_authenticated)],
) -> EntitiesResponse:
    """
    Retrieve a paginated list of entities, filtered with the Aleph / OpenAleph
    grammar. `nested=true` inlines adjacent entities, `featured` / `dehydrate`
    reduce the returned properties.

    ## filtering

    * dataset: `filter:dataset=my_dataset&filter:dataset=another_dataset`
    * schema: `filter:schema=Company`, or `filter:schemata=LegalEntity` to
      include descendants
    * [property](https://followthemoney.tech/explorer/):
      `filter:properties.country=de`
    * property-type group: `filter:group.countries=de`,
      `filter:group.entities=<id>` (reverse lookup)
    * context column: `filter:context.origin=<origin>`

    Comparators:

    * range: `filter:gte:properties.date=2023`, `filter:lt:properties.amountEur=1000`
    * substring: `filter:ilike:properties.name=jane`
    * prefix: `filter:startswith:canonical_id=eu-`
    * negation: `exclude:properties.jurisdiction=eu`
    * absence: `empty:properties.deathDate=`

    Nested boolean filters (`or`, negated groups) go in an
    [RQL](https://github.com/pjwerneck/pyrql) `rql=` param, which replaces the
    flat filters (`sort` / `limit` / `offset` still apply):

        ?rql=or(eq(schema,Person),eq(group.countries,de))

    ## projection

    `select=properties.name&select=group.countries` returns only these
    properties; filters, sorting and aggregations still see the whole entity.

    ## sorting

    `sort=properties.<name>` or `sort=properties.<name>:desc`. Numeric
    properties sort as numbers; a multi-valued property sorts by its first value.

    ## pagination

    `limit=100&offset=200`

    ## aggregations

    `metric:<func>=<field>` (`min`, `max`, `sum`, `avg`, `count`), optionally
    grouped by `facet=<field>`. Fields take the `filter:` spelling, plus `year`:

        ?metric:sum=properties.amountEur&metric:count=id&facet=year

    A `facet` on its own groups an entity count; these are equivalent:

        ?facet=group.countries
        ?metric:count=id&facet=group.countries

    Grouped metrics come back as `facets` (buckets of `value`, `label`, `count`,
    `metrics`), ungrouped ones as `metrics`. Buckets rank by entity count, or by a
    metric via `facet_sort=<func>:<field>[:asc]`. A facet returns its top 20
    buckets (`facet_size:<field>=N`, at most 50); its `total` counts all distinct
    values:

        ?metric:sum=properties.amountEur&facet=properties.beneficiary
        &facet_sort=sum:properties.amountEur&facet_size:properties.beneficiary=5

    `limit=0` returns only the aggregations (plus `total` / `stats`):

        ?filter:schema=Payment&metric:sum=properties.amountEur&limit=0

    ## searching

    A `q` term runs a full-text search (relevance-ranked, dehydrated hits) with
    the same filters:

        ?q=jane+doe&filter:dataset=my_dataset&filter:group.countries=de
    """
    return views.entity_list(request, retrieve_params, authenticated=authenticated)


@app.get(
    "/entities/{entity_id}",
    response_model=EntityResponse,
    responses={
        307: {"description": "The entity was merged into another ID"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
    },
)
def detail_entity(
    request: Request,
    entity_id: str,
    retrieve_params: Annotated[RetrieveParams, Depends()],
) -> EntityResponse | RedirectResponse | ErrorResponse:
    """
    Retrieve a single entity, optionally with adjacent entities inlined.

    An entity merged into another one redirects there, with the target's
    `x-entity-id` and `x-entity-schema` headers.
    """
    return views.entity_detail(request, entity_id, retrieve_params)


@app.get(
    "/autocomplete",
    response_model=AutocompleteResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query"},
    },
)
def autocomplete(request: Request, q: str) -> AutocompleteResponse:
    """
    Autocomplete entity names.
    """
    return views.autocomplete(request, q)
