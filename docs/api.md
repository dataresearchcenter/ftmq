`ftmq.api` exposes a followthemoney statement store (and the [`ftmq.search`](./search.md) full-text index) as a read-only [FastAPI](https://fastapi.tiangolo.com/) application. It was formerly the standalone [`ftmq-api`](https://github.com/dataresearchcenter/ftmq-api) package (and before that, `ftmstore-fastapi`).

## Install

```bash
pip install ftmq[api]
```

## End-to-end setup

This walks through serving a followthemoney entities file as a fully featured api instance, including full-text search, entirely from the command line. Start with an `entities.ftm.json` file (one entity json object per line).

### 1. Apply a dataset

The api serves entities scoped by dataset, so make sure every entity carries the dataset name you want to publish it under. If your entities already have proper datasets applied, skip this step.

```bash
cat entities.ftm.json | ftmq apply-dataset -d my_dataset --replace-dataset -o entities.my_dataset.ftm.json
```

`--replace-dataset` drops any datasets already present on the entities (including the implicit `default` assigned to raw entities); without the flag, `my_dataset` is added alongside them.

### 2. Load the statement store

```bash
ftmq -i entities.my_dataset.ftm.json -o sqlite:///ftm.store
```

Any [ftmq store backend](./stores.md) works as the target (`sqlite://`, `postgresql://`, `leveldb://`, `redis://`, ...); a sqlite file is the simplest to start with.

### 3. Build the search index

The `/search` and `/autocomplete` endpoints are backed by a [`ftmq.search`](./search.md) index. Transform the entities into search documents and index them:

```bash
cat entities.my_dataset.ftm.json | ftmq search transform | ftmq search --uri sqlite:///ftm.store index
```

The index can live in the same sqlite database as the statement store (as here) or anywhere else (`tantivy://` for larger datasets).

### 4. Describe the catalog

The api serves the datasets listed in a catalog document. Create a `catalog.json`:

```json
{
  "name": "my_catalog",
  "title": "My Data Catalog",
  "datasets": [{ "name": "my_dataset", "title": "My Dataset" }]
}
```

### 5. Configure and run

Point the api at the store, the search index and the catalog, then run it with [granian](https://github.com/emmett-framework/granian) (included in the `api` extra):

```bash
export FTMQ_API_STORE_URI=sqlite:///ftm.store
export FTMQ_SEARCH_URI=sqlite:///ftm.store
export FTMQ_API_CATALOG=./catalog.json
granian --interface asgi ftmq.api.app:app
```

`FTMQ_API_STORE_URI` defaults to nomenklatura's `NOMENKLATURA_DB_URL`, and `FTMQ_SEARCH_URI` defaults to that same database when it is sqlite, so with the single-file layout above only `FTMQ_API_CATALOG` is strictly required. The catalog and stores are read once at process start: after changing data, restart the server.

For production, use several workers: `granian --interface asgi --workers 4 ftmq.api.app:app`. Any other ASGI server (uvicorn, hypercorn, ...) works as well.

### 6. Verify

```bash
# catalog with computed dataset statistics
curl -s "localhost:8000/catalog"
# filtered, sorted entities
curl -s "localhost:8000/entities?filter:schema=Person&sort=name&limit=5"
# aggregation (rides on /entities; limit=0 returns only aggregations)
curl -s "localhost:8000/entities?filter:schema=Payment&metric:sum=amountEur&limit=0"
# full-text search and autocomplete
curl -s "localhost:8000/search?q=jane+doe&filter:dataset=my_dataset"
curl -s "localhost:8000/autocomplete?q=jan"
```

The interactive ReDoc documentation is served at [`localhost:8000/`](http://localhost:8000).

### Multiple datasets

One api instance serves any number of datasets. Apply each dataset name to its source file, load everything into the same store and search index, and list all datasets in the catalog:

```bash
cat dataset1.ftm.json | ftmq apply-dataset -d dataset1 --replace-dataset -o entities.dataset1.ftm.json
cat dataset2.ftm.json | ftmq apply-dataset -d dataset2 --replace-dataset -o entities.dataset2.ftm.json

ftmq -i entities.dataset1.ftm.json -o sqlite:///ftm.store
ftmq -i entities.dataset2.ftm.json -o sqlite:///ftm.store

cat entities.dataset1.ftm.json entities.dataset2.ftm.json | ftmq search transform | ftmq search --uri sqlite:///ftm.store index
```

```json
{
  "name": "my_catalog",
  "title": "My Data Catalog",
  "datasets": [
    { "name": "dataset1", "title": "Dataset 1" },
    { "name": "dataset2", "title": "Dataset 2" }
  ]
}
```

Requests span all datasets by default; scope them with one or more `filter:dataset=` params (this works on `/entities` and `/search` alike, an unknown dataset returns a 422):

```bash
curl -s "localhost:8000/catalog"                     # per-dataset statistics
curl -s "localhost:8000/catalog/dataset2"            # single dataset metadata
curl -s "localhost:8000/entities?filter:dataset=dataset2&limit=5"
curl -s "localhost:8000/entities?filter:dataset=dataset1&filter:schema=Payment&metric:sum=amountEur&limit=0"
curl -s "localhost:8000/search?q=jane+doe&filter:dataset=dataset1"
```

Remember that the dataset list is frozen at process start (from the catalog document): adding a dataset means updating the catalog, loading its data and restarting the server.

## Endpoints

| Path | Purpose |
|---|---|
| `/` | ReDoc api documentation |
| `/catalog` | Catalog metadata with per-dataset statistics |
| `/catalog/{dataset}` | Dataset metadata |
| `/entities` | Filtered, sorted, paginated entity lists (+ aggregations) |
| `/entities/{entity_id}` | Entity detail (307 redirect for merged entities) |
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
/entities?filter:schema=Payment&metric:sum=amountEur&facet=year&limit=0   # aggregations only
/search?q=jane+doe&filter:dataset=my_dataset&filter:countries=de
```

Aggregations ride on the entities query, as in the Aleph api: add `metric:<func>=<prop>` (and `facet=<field>` to group them). They are returned in the response `aggregations`; set `limit=0` to get only the aggregations (plus `total` / `stats`), no entities.

For nested boolean trees that the flat grammar cannot express (a cross-field `OR`, a negated group), pass a full [RQL](./query.md) string via `rql=`. It overrides the flat filter params, while `sort` / `limit` / `offset` still apply, and it also carries aggregations:

```bash
/entities?rql=and(eq(schema,Person),or(eq(countries,de),eq(countries,at)))
/entities?rql=aggregate(year,sum(amountEur))&limit=0
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

The `/similar` endpoint was removed (it had no backing implementation). The `/aggregate` endpoint was merged into `/entities` (Aleph-style): request aggregations on the entities query and set `limit=0` for aggregations only. The response `query` field now echoes the canonical query serialization instead of the raw params, and pagination urls use `offset`.

## Settings

Environment variables use the `FTMQ_API_` prefix (see [`Settings`][ftmq.api.settings.Settings]): `FTMQ_API_CATALOG` (catalog uri, required for multi-dataset instances), `FTMQ_API_STORE_URI` (defaults to nomenklatura's `NOMENKLATURA_DB_URL`), `FTMQ_API_DEFAULT_LIMIT` (public pagination cap, default 100), `FTMQ_API_BUILD_API_KEY`, `FTMQ_API_MIN_SEARCH_LENGTH`, `FTMQ_API_ALLOWED_ORIGIN`, `FTMQ_API_INFO_TITLE` / `FTMQ_API_INFO_DESCRIPTION_URI` (ReDoc landing page). The search store location comes from `FTMQ_SEARCH_URI` (defaults to the nomenklatura database when it is sqlite).

Caching: set `FTMQ_API_USE_CACHE=1` and point `FTMQ_API_CACHE_URI` at any [anystore](https://docs.investigraph.dev/lib/anystore) backend (a redis, a filesystem path, ...). Responses are cached keyed by request url.

The catalog, stores and views are cached at process start: changing the catalog or the underlying data requires a restart.
