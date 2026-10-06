`ftmq.api` serves a followthemoney statement store (and the [`ftmq.search`](./search.md) full-text index) as a read-only [FastAPI](https://fastapi.tiangolo.com/) application.

## Install

```bash
pip install ftmq[api]
```

## End-to-end setup

Serve an `entities.ftm.json` file (one entity json object per line) with full-text search, from the command line.

### 1. Apply a dataset

The api scopes entities by dataset. Skip this if your entities already carry the right datasets.

```bash
cat entities.ftm.json | ftmq apply-dataset -d my_dataset --replace-dataset -o entities.my_dataset.ftm.json
```

`--replace-dataset` drops existing datasets (including the implicit `default`); without it, `my_dataset` is added alongside them.

### 2. Load the statement store

```bash
ftmq -i entities.my_dataset.ftm.json -o sqlite:///ftm.store
```

Any [ftmq store backend](./stores.md) works (`sqlite://`, `postgresql://`, `leveldb://`, ...).

### 3. Build the search index

Full-text search (`/entities?q=`) and `/autocomplete` need a [`ftmq.search`](./search.md) index:

```bash
cat entities.my_dataset.ftm.json | ftmq search transform | ftmq search --uri sqlite:///ftm.store index
```

The index can share the store's sqlite database (as here) or live elsewhere (`tantivy://` for larger datasets).

### 4. Describe the catalog

An optional `catalog.json` adds dataset metadata (titles, descriptions, publishers):

```json
{
  "name": "my_catalog",
  "title": "My Data Catalog",
  "datasets": [{ "name": "my_dataset", "title": "My Dataset" }]
}
```

The *store* decides what is queryable: a dataset in the store but not in the catalog is served with a bare name (a warning is logged). `filter:dataset=` rejects names the store doesn't hold with a 422 listing the available ones.

### 5. Configure and run

Run with [granian](https://github.com/emmett-framework/granian) (included in the `api` extra):

```bash
export FTMQ_API_STORE_URI=sqlite:///ftm.store
export FTMQ_SEARCH_URI=sqlite:///ftm.store
export FTMQ_API_CATALOG=./catalog.json
granian --interface asgi ftmq.api.app:app
```

`FTMQ_API_STORE_URI` defaults to nomenklatura's `NOMENKLATURA_DB_URL`, and `FTMQ_SEARCH_URI` to the same database when it is sqlite, so for this single-file layout every variable is optional (without `FTMQ_API_CATALOG` the catalog is derived from the store). Catalog and stores are read once at process start: restart the server after changing data.

For production, run several workers (`granian --interface asgi --workers 4 ftmq.api.app:app`). Routes run in a thread pool, which helps I/O-bound backends (lake, postgres); for CPU-bound work add workers. Any other ASGI server (uvicorn, hypercorn, ...) works as well.

### 6. Verify

```bash
# catalog with computed dataset statistics
curl -s "localhost:8000/catalog"
# filtered, sorted entities
curl -s "localhost:8000/entities?filter:schema=Person&sort=properties.name&limit=5"
# aggregation (rides on /entities; limit=0 returns only aggregations)
curl -s "localhost:8000/entities?filter:schema=Payment&metric:sum=properties.amountEur&limit=0"
# full-text search and autocomplete
curl -s "localhost:8000/entities?q=jane+doe&filter:dataset=my_dataset"
curl -s "localhost:8000/autocomplete?q=jan"
```

ReDoc documentation is served at [`localhost:8000/`](http://localhost:8000).

### Multiple datasets

Apply each dataset name to its source file, load everything into the same store and search index, and list all datasets in the catalog:

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

Requests span all datasets; scope them with one or more `filter:dataset=` params (an unknown dataset returns a 422):

```bash
curl -s "localhost:8000/catalog"                     # per-dataset statistics
curl -s "localhost:8000/catalog/dataset2"            # single dataset metadata
curl -s "localhost:8000/entities?filter:dataset=dataset2&limit=5"
curl -s "localhost:8000/entities?filter:dataset=dataset1&filter:schema=Payment&metric:sum=properties.amountEur&limit=0"
curl -s "localhost:8000/entities?q=jane+doe&filter:dataset=dataset1"
```

The dataset list is fixed at process start: a new dataset needs its data loaded (and catalog entry, if any) and a restart.

## Endpoints

| Path | Purpose |
|---|---|
| `/` | ReDoc api documentation |
| `/catalog` | Catalog metadata with per-dataset statistics |
| `/catalog/{dataset}` | Dataset metadata |
| `/entities` | Entity lists, aggregations, and full-text search (`?q=`) |
| `/entities/{entity_id}` | Entity detail (307 redirect for merged entities) |
| `/autocomplete` | Name autocomplete via `ftmq.search` |

## Query dialect

The api speaks the Aleph / OpenAleph filter grammar ([`Query.from_params`](./query.md)):

```bash
/entities?filter:dataset=my_dataset&filter:schema=Payment
/entities?filter:schemata=LegalEntity                      # is-a matching incl. descendants
/entities?filter:properties.name=Jane                      # exact property match
/entities?filter:gte:properties.date=2023                  # ranges: gte, gt, lte, lt
/entities?filter:ilike:properties.name=jane                # substring: like, ilike
/entities?filter:startswith:canonical_id=eu-               # prefix: startswith, endswith
/entities?exclude:properties.jurisdiction=eu               # negation
/entities?empty:properties.deathDate=                      # absence
/entities?filter:group.countries=de                        # property-type groups
/entities?filter:group.entities=<entity-id>                # reverse lookup (any edge)
/entities?filter:context.origin=crawl                      # context columns
/entities?sort=properties.name:desc&limit=100&offset=200   # sorting and pagination
/entities?filter:schema=Payment&metric:sum=properties.amountEur&facet=year&limit=0   # aggregations only
/entities?q=jane+doe&filter:dataset=my_dataset&filter:group.countries=de
```

Aggregations ride on the entities query: `metric:<func>=<field>`, grouped by `facet=<field>`. Ungrouped ones are returned in `metrics`, grouped ones in `facets`; `limit=0` returns only those (plus `total`).

`metric:` and `facet` use the `filter:` field spelling (`properties.<name>`, `group.<name>`, `context.<name>`, bare meta fields and `year`), and the response keys metrics the same way. A bare `facet` groups an entity count: `?facet=group.countries` is short for `?metric:count=id&facet=group.countries`, and `metric:count=id` equals the response `total`.

Each facet bucket carries its entity `count` and the requested metrics under `metrics` (`{field: {func: value}}`). Buckets are ranked by entity count, or by a metric with `facet_sort=<func>:<field>[:asc]` (descending by default). Each facet returns its top 20 buckets, or `facet_size:<field>=N` (at most `FTMQ_API_MAX_FACET_SIZE`, 50, unless the request carries the `api_key`); the facet's `total` counts all its distinct values.

```bash
/entities?filter:schema=Payment&metric:sum=properties.amountEur&facet=properties.beneficiary&facet_sort=sum:properties.amountEur&limit=0
```

For nested trees (a cross-field `OR`, a negated group) pass an [RQL](./query.md#rql) string via `rql=`. It overrides the flat filter params and can carry aggregations; `sort` / `limit` / `offset` still apply:

```bash
/entities?rql=and(eq(schema,Person),or(eq(group.countries,de),eq(group.countries,at)))
/entities?rql=aggregate(year,sum(properties.amountEur))&limit=0
```

Response flags: `nested` (inline adjacent entities), `featured`, `dehydrate`, `dehydrate_nested`, `stats`. A request with `api_key=<FTMQ_API_BUILD_API_KEY>` may exceed the public `limit` cap (e.g. for static site builds).

## Response

`/entities` (list and `?q=` search) returns the OpenAleph api v2 envelope:

```json
{
  "status": "ok",
  "results": [{ "id": "...", "caption": "...", "schema": "Person", "properties": {}, "datasets": ["..."] }],
  "total": 1234,
  "total_type": "eq",
  "page": 1,
  "pages": 13,
  "limit": 100,
  "offset": 0,
  "next": "https://.../entities?...&offset=100&limit=100",
  "previous": null,
  "facets": { "year": { "values": [{ "value": "2011", "label": "2011", "count": 42, "metrics": { "properties.amountEur": { "sum": 1953402.15 } } }], "total": 10 } },
  "metrics": { "properties.amountEur": { "sum": 40589689.15 } },
  "filters": { "schema": ["Person"] },
  "query_q": null,
  "query": { "q": { "and": [] }, "limit": 100, "offset": 0 },
  "stats": null,
  "links": {}
}
```

`total_type` is always `eq` (exact counts). `query` (the canonical [`Query.to_dict`](./query.md)) and `stats` (dataset statistics, with `stats=1`) are ftmq extensions Aleph clients can ignore.

### Migrating from ftmq-api 3.x

| ftmq-api 3.x | ftmq.api |
|---|---|
| `dataset=x` | `filter:dataset=x` |
| `schema=X` | `filter:schema=X` |
| `schema=X&schema_include_descendants=1` | `filter:schemata=X` |
| `name__ilike=%jane%` | `filter:ilike:properties.name=jane` |
| `date__gte=2023` | `filter:gte:properties.date=2023` |
| `jurisdiction__not=eu` | `rql=ne(properties.jurisdiction,eu)` (`exclude:properties.jurisdiction=eu` also keeps entities without a jurisdiction) |
| `canonical_id__startswith=eu-` | `filter:startswith:canonical_id=eu-` |
| `reverse=<id>` | `filter:group.entities=<id>` |
| `country=de` (search) | `filter:group.countries=de` |
| `order_by=-date` | `sort=properties.date:desc` |
| `page=3&limit=100` | `offset=200&limit=100` |
| `aggSum=amountEur&aggGroups=year` | `metric:sum=properties.amountEur&facet=year` |

`/similar` is removed; `/aggregate` and `/search` are merged into `/entities` (aggregation params, `?q=<term>`). Pagination urls use `offset`.

## Settings

Environment variables use the `FTMQ_API_` prefix (see [`Settings`][ftmq.api.settings.Settings]):

- `FTMQ_API_CATALOG` - catalog uri (optional, adds dataset metadata)
- `FTMQ_API_STORE_URI` - defaults to nomenklatura's `NOMENKLATURA_DB_URL`
- `FTMQ_API_RESOLVER_URI` - deduplication decisions: a sql database with a `resolver` table or a json edge dump; defaults to the store's database
- `FTMQ_API_DEFAULT_LIMIT` - public pagination cap, default 100
- `FTMQ_API_MAX_FACET_SIZE` - public facet size cap, default 50
- `FTMQ_API_BUILD_API_KEY` - unset by default, so no request exceeds the caps
- `FTMQ_API_MIN_SEARCH_LENGTH`, `FTMQ_API_ALLOWED_ORIGIN`
- `FTMQ_API_INFO_TITLE` / `FTMQ_API_INFO_DESCRIPTION_URI` - ReDoc landing page
- `FTMQ_SEARCH_URI` - the search store (defaults to the nomenklatura database when it is sqlite)

Deduplication: the resolver maps a requested id to its canonical one, so `/entities/{referent_id}` serves the merged entity. It does **not** merge the data: the store's statements must already carry the canonical id (see [merged entities](./stores.md#merged-entities-resolver-linker)). Against an unresolved store only id lookups work; filters, counts, aggregations and search return the cluster members separately.

Caching: set `FTMQ_API_USE_CACHE=1` and point `FTMQ_API_CACHE_URI` at any [anystore](https://docs.investigraph.dev/lib/anystore) backend (redis, a filesystem path, ...). Responses are cached by request url.
