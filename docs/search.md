`ftmq.search` provides simple full-text search stores for [Follow The Money](https://followthemoney.tech) entities. Entities are transformed into flat search documents (names, fingerprints, countries, dates and a text blob) that are indexed into a search backend for shallow retrieval by keyword, with optional [`Query`](./query.md) filtering by dataset, schema and country. It was formerly the standalone [`ftmq-search`](https://github.com/dataresearchcenter/ftmq-search/) package.

Two backends are implemented: SQLite [FTS5](https://www.sqlite.org/fts5.html) (no extra dependencies) and [Tantivy](https://github.com/quickwit-oss/tantivy), persistent or in-memory. For a full-featured Elasticsearch based search stack, look into [openaleph-search](https://openaleph.org) or [yente](https://www.opensanctions.org/docs/yente/).

## Install

The tantivy backend needs the `search` extra:

```bash
pip install ftmq[search]
```

The SQLite FTS5 backend works with a plain `ftmq` install.

## Command line

The store uri is passed via `--uri` or the `FTMQ_SEARCH_URI` environment variable. `sqlite:///...` selects the FTS5 backend, `tantivy://<path>` a persistent Tantivy index and `memory:///` an in-memory Tantivy index.

Transform an entity stream into search documents:

```bash
cat entities.ftm.json | ftmq search transform > documents.ndjson
```

Index the documents into a store:

```bash
ftmq search --uri sqlite:///ftmqs.db index -i documents.ndjson
ftmq search --uri tantivy://tantivy.db index -i documents.ndjson
```

Search and autocomplete (a bare query routes to the `search` subcommand):

```bash
ftmq search --uri sqlite:///ftmqs.db "jane doe"
ftmq search --uri sqlite:///ftmqs.db autocomplete jan
```

## Python

```python
from ftmq import G, M, Query
from ftmq.io import smart_read_proxies
from ftmq.search import get_store, index_entities

store = get_store("tantivy://tantivy.db")
index_entities(smart_read_proxies("entities.ftm.json"), store)

# search, optionally filtered by a Query
for result in store.search("jane doe", Query().where(M(schema="Person"), G(countries="de"))):
    print(result.id, result.score, result.entity.caption)

for result in store.autocomplete("jan"):
    print(result.id, result.name)
```

Search results are `EntitySearchResult` objects carrying a shallow `EntityModel` (id, caption, names, countries) and the match score; `result.to_proxy()` converts back to an `EntityProxy`.

## Settings

Environment variables use the `FTMQ_SEARCH_` prefix: `FTMQ_SEARCH_URI` (store uri, defaults to the nomenklatura sqlite database if configured, else `sqlite:///ftmqs.db`), `FTMQ_SEARCH_SQL_TABLE_NAME` (table name for the FTS5 backend, default `ftmqs`), `FTMQ_SEARCH_YAML_URI` / `FTMQ_SEARCH_JSON_URI` (load a store configuration document).
