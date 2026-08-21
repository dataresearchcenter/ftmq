`ftmq.search` provides simple full-text search stores for [Follow The Money](https://followthemoney.tech) entities. Entities are transformed into flat search documents (names, fingerprints, countries, dates and a text blob) that are indexed into a search backend for shallow retrieval by keyword, with optional [`Query`](./query.md) filtering by dataset, schema and country.

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

### Query filters

A search document holds three filterable fields: `datasets`, `schema` and `countries`. A [`Query`](./query.md) passed to `search()` is compiled into that subset by `ftmq.search.store.base.get_filters`, which drops filters on any other field (a property, an id) and keeps the rest as a flat list of ANDed terms.

Negation is honoured: `M(dataset__not="x")` (Aleph `exclude:dataset=x`) or `~M(dataset="x")` excludes the dataset instead of selecting it, and a same-field `OR` folds into a single term. A filter shape the index cannot express raises a `QueryError` rather than filtering on something else: a cross-field `OR`, a negated group of several conditions, or a comparator other than `eq` / `in` / `not` / `not_in` on one of the three fields.

Note that a negated filter on a multi-valued field means "holds none of these values" here (as `exclude:` does in the [Aleph param grammar](./query.md)), while the in-memory and SQL evaluators read `not` as "holds a value other than this one". For the single-valued `schema` field both readings agree.

## Settings

Environment variables use the `FTMQ_SEARCH_` prefix: `FTMQ_SEARCH_URI` (store uri, defaults to the nomenklatura sqlite database if configured, else `sqlite:///ftmqs.db`), `FTMQ_SEARCH_SQL_TABLE_NAME` (table name for the FTS5 backend, default `ftmqs`), `FTMQ_SEARCH_YAML_URI` / `FTMQ_SEARCH_JSON_URI` (load a store configuration document).
