from anystore.model import StoreModel
from anystore.settings import BaseSettings
from nomenklatura.settings import DB_URL
from pydantic import BaseModel
from pydantic_settings import SettingsConfigDict

from ftmq import __version__

DEFAULT_DESCRIPTION = """
Read-only api over a [followthemoney](https://followthemoney.tech/explorer/)
statement store: fetch, filter, aggregate and search entities.

* [`/catalog`](/catalog): the available datasets
* `/catalog/{dataset}`: metadata of one dataset
* `/entities?{params}`: filter, sort, paginate and aggregate entities with the
  Aleph / OpenAleph grammar, e.g. `filter:schema=Payment&sort=properties.date:desc`;
  `q=<term>` runs a full-text search
* `/entities/{entity_id}`: a single entity, optionally with adjacent entities
* `/autocomplete?q=<term>`: autocomplete names
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
    Api settings, read from `FTMQ_API_` prefixed environment variables.

    Nested fields join with `_`, e.g. `FTMQ_API_INFO_TITLE`.
    """

    model_config = SettingsConfigDict(
        env_prefix="ftmq_api_",
        env_nested_delimiter="_",
        env_nested_max_split=1,
        nested_model_default_partial_update=True,
    )

    catalog: str | None = None
    """Catalog uri"""

    store_uri: str = DB_URL
    """ftmq store uri"""

    resolver_uri: str | None = None
    """Resolver uri (a sql database with a `resolver` table, or a json lines edge
    dump); defaults to the store uri"""

    build_api_key: str | None = None
    """Api key that lifts the public limits (e.g. for build processes)"""

    min_search_length: int = 3
    """Minimum search query length"""

    use_cache: bool = False
    """Activate caching"""

    cache: StoreModel = StoreModel(
        uri=".cache", backend_config={"redis_prefix": f"ftmq-api/{__version__}"}
    )
    """Api cache (via anystore)"""

    allowed_origin: list[str] = ["http://localhost:3000"]
    """Allowed CORS origins"""

    default_limit: int = 100
    """Default and public maximum page size"""

    max_facet_size: int = 50
    """Public cap on the buckets per facet"""

    info: ApiInfo = ApiInfo()
    """ReDoc page information"""


settings = Settings()
