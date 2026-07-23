`ftmq.api` exposes a followthemoney statement store (and the [`ftmq.search`](./search.md) full-text index) as a read-only [FastAPI](https://fastapi.tiangolo.com/) application. It was formerly the standalone [`ftmq-api`](https://github.com/dataresearchcenter/ftmq-api) package (and before that, `ftmstore-fastapi`).

## Install

```bash
pip install ftmq[api]
```

## Setup

Build a statement store and, for the `/search` and `/autocomplete` endpoints, a search index:

```bash
ftmq -i entities.ftm.json -o sqlite:///nomenklatura.db
cat entities.ftm.json | ftmq search transform | ftmq search --uri sqlite:///nomenklatura.db index
```

Point the api at the store and a catalog document listing the datasets it serves, then run it with any ASGI server (install one, e.g. `pip install uvicorn`):

```bash
export NOMENKLATURA_DB_URL=sqlite:///nomenklatura.db
export FTMQ_API_CATALOG=./catalog.json
uvicorn ftmq.api.app:app
```

For production, use several workers, e.g. `gunicorn ftmq.api.app:app --workers 4 --worker-class uvicorn.workers.UvicornWorker`.

## Endpoints

| Path | Purpose |
|---|---|
| `/` | ReDoc api documentation |
| `/catalog` | Catalog metadata with per-dataset statistics |
| `/catalog/{dataset}` | Dataset metadata |
| `/entities` | Filtered, sorted, paginated entity lists |
| `/entities/{entity_id}` | Entity detail (307 redirect for merged entities) |
| `/aggregate` | Property value aggregations |
| `/search` | Full-text search via `ftmq.search` |
| `/autocomplete` | Name autocomplete via `ftmq.search` |

## Query dialect

The api speaks the Aleph / OpenAleph filter grammar, the same [`Query.from_params`](./query.md) surface used across the ftmq ecosystem:

```bash
/entities?filter:dataset=my_dataset&filter:schema=Payment
/entities?filter:schemata=LegalEntity                      # is-a matching incl. descendants
/entities?filter:properties.name=Jane                      # exact property match
/entities?filter:gte:properties.date=2023                  # ranges: gte, gt, lte, lt
/entities?filter:ilike:properties.name=jane                # substring: like, ilike
/entities?filter:startswith:canonical_id=eu-               # prefix: startswith, endswith
/entities?exclude:properties.jurisdiction=eu               # negation
/entities?empty:properties.deathDate=                      # absence
/entities?filter:countries=de                              # property-type groups
/entities?filter:entities=<entity-id>                      # reverse lookup (any edge)
/entities?sort=name:desc&limit=100&offset=200              # sorting and pagination
/aggregate?filter:schema=Payment&metric:sum=amountEur&facet=year
/search?q=jane+doe&filter:dataset=my_dataset&filter:countries=de
```

Retrieve flags shape the response: `nested` (inline adjacent entities), `featured`, `dehydrate`, `dehydrate_nested`, `stats`. A request with `api_key=<FTMQ_API_BUILD_API_KEY>` may exceed the public `limit` cap (useful for static site builders).

### Migrating from ftmq-api 3.x

The former package spoke its own param dialect; this table maps old params to the new grammar:

| ftmq-api 3.x | ftmq.api |
|---|---|
| `dataset=x` | `filter:dataset=x` |
| `schema=X` | `filter:schema=X` |
| `schema=X&schema_include_descendants=1` | `filter:schemata=X` |
| `name__ilike=%jane%` | `filter:ilike:properties.name=jane` |
| `date__gte=2023` | `filter:gte:properties.date=2023` |
| `jurisdiction__not=eu` | `exclude:properties.jurisdiction=eu` |
| `canonical_id__startswith=eu-` | `filter:startswith:canonical_id=eu-` |
| `reverse=<id>` | `filter:entities=<id>` |
| `country=de` (search) | `filter:countries=de` |
| `order_by=-date` | `sort=date:desc` |
| `page=3&limit=100` | `offset=200&limit=100` |
| `aggSum=amountEur&aggGroups=year` | `metric:sum=amountEur&facet=year` |

The `/similar` endpoint was removed (it had no backing implementation). The response `query` field now echoes the canonical query serialization instead of the raw params, and pagination urls use `offset`.

## Settings

Environment variables use the `FTMQ_API_` prefix (see [`Settings`][ftmq.api.settings.Settings]): `FTMQ_API_CATALOG` (catalog uri, required for multi-dataset instances), `FTMQ_API_STORE_URI` (defaults to nomenklatura's `NOMENKLATURA_DB_URL`), `FTMQ_API_DEFAULT_LIMIT` (public pagination cap, default 100), `FTMQ_API_BUILD_API_KEY`, `FTMQ_API_MIN_SEARCH_LENGTH`, `FTMQ_API_ALLOWED_ORIGIN`, `FTMQ_API_INFO_TITLE` / `FTMQ_API_INFO_DESCRIPTION_URI` (ReDoc landing page). The search store location comes from `FTMQ_SEARCH_URI` (defaults to the nomenklatura database when it is sqlite).

Caching: set `FTMQ_API_USE_CACHE=1` and point `FTMQ_API_CACHE_URI` at any [anystore](https://docs.investigraph.dev/lib/anystore) backend (a redis, a filesystem path, ...). Responses are cached keyed by request url.

The catalog, stores and views are cached at process start: changing the catalog or the underlying data requires a restart.
