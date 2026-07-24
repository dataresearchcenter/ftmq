from anystore.model import StoreModel
from anystore.settings import BaseSettings
from nomenklatura.settings import DB_URL
from pydantic import BaseModel
from pydantic_settings import SettingsConfigDict

from ftmq import __version__

DEFAULT_DESCRIPTION = """
This api exposes a [followthemoney](https://followthemoney.tech) statement
store as a read-only endpoint that allows granular data fetching, aggregation
and searching.

* [Available datasets in this api instance](/catalog)
* [More about the FollowTheMoney model](https://followthemoney.tech/explorer/)

This api works for all store implementations found in
[`ftmq.store`](https://docs.investigraph.dev/lib/ftmq/reference/store/)

The main api endpoints:

* Retrieve a single entity based on its id, optionally with inlined adjacent
  entities: `/entities/{entity_id}`
* Retrieve a list of entities based on filter criteria and sorting, with
  pagination: `/entities?{params}`
* Aggregate property values on the same query (Aleph-style): add
  `metric:sum=amountEur&facet=year`, set `limit=0` for aggregations only
* Search for entities via full-text search: `/search?q=<search term>`
* Autocomplete names: `/autocomplete?q=<term>`

Filtering uses the Aleph / OpenAleph filter grammar, e.g.
`filter:schema=Payment`, `filter:gte:properties.date=2023`,
`exclude:properties.jurisdiction=eu`, sorting via `sort=name:desc` and
pagination via `limit` / `offset`.

Two more endpoints for catalog / dataset metadata:

* Catalog overview: [`/catalog`](/catalog)
* Dataset metadata: `/catalog/{dataset}`
"""


class ApiContact(BaseModel):
    name: str = "Data and Research Center – DARC"
    url: str = "https://dataresearchcenter.org"
    email: str = "hi@dataresearchcenter.org"


class ApiInfo(BaseModel):
    title: str = "FTMQ Api"
    contact: ApiContact = ApiContact()
    description_uri: str | None = None


class Settings(BaseSettings):
    """
    `anystore` settings management using
    [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)

    Note:
        All settings can be set via environment variables in uppercase,
        prepending `FTMQ_API_` (except for those with a given prefix)
    """

    model_config = SettingsConfigDict(
        env_prefix="ftmq_api_",
        env_nested_delimiter="_",
        nested_model_default_partial_update=True,
    )

    catalog: str | None = None
    """Catalog uri"""

    store_uri: str = DB_URL
    """ftmq store uri"""

    build_api_key: str = "secret-key-for-build"
    """Backend api key to use for build process (higher limit)"""

    min_search_length: int = 3
    """Minimum search query length"""

    use_cache: bool = False
    """Activate caching"""

    cache: StoreModel = StoreModel(
        uri=".cache", backend_config={"redis_prefix": f"ftmq-api/{__version__}"}
    )
    """Api cache (via anystore)"""

    allowed_origin: list[str] = ["http://localhost:3000"]
    """Allowed origins"""

    default_limit: int = 100
    """Default public pagination limit"""

    info: ApiInfo = ApiInfo()
    """Rendered information on redoc page"""
