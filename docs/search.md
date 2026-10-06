`ftmq.search` provides simple full-text search stores for [Follow The Money](https://followthemoney.tech) entities. Entities are flattened into search documents (names, fingerprints, countries, dates and a text blob) for keyword search, optionally filtered by a [`Query`](./query.md) on dataset, schema and country.

Backends: SQLite [FTS5](https://www.sqlite.org/fts5.html) (no extra dependencies) and [Tantivy](https://github.com/quickwit-oss/tantivy), persistent or in-memory. For a full Elasticsearch stack, see [openaleph-search](https://openaleph.org) or [yente](https://www.opensanctions.org/docs/yente/).

## Install

The tantivy backend needs the `search` extra (SQLite FTS5 works with a plain install):

```bash
pip install ftmq[search]
```

## Command line

The store uri comes from `--uri` or `FTMQ_SEARCH_URI`: `sqlite:///...` (FTS5), `tantivy://<path>` (persistent Tantivy) or `memory:///` (in-memory Tantivy).

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

Results are `EntitySearchResult` objects with the match score and a shallow `EntityModel` (id, caption, names, countries); `result.to_proxy()` converts back to an `EntityProxy`.

### Query filters

Only `datasets`, `schema` and `countries` are filterable. `ftmq.search.store.base.get_filters` compiles the `Query` into a flat list of ANDed terms on those fields and drops filters on any other field (a property, an id).

Negation works: `M(dataset__not="x")` or `~M(dataset="x")` (Aleph `exclude:dataset=x`) excludes that dataset's entities, and a same-field `OR` folds into one term. A shape the index cannot express raises a `QueryError`.

On a multi-valued field a negated filter means "holds none of these values" (as Aleph `exclude:` does), while the in-memory and SQL evaluators read `not` as "holds a value other than this one". For the single-valued `schema` both agree.

## Settings

Environment variables use the `FTMQ_SEARCH_` prefix: `FTMQ_SEARCH_URI` (store uri; defaults to the nomenklatura database if it is sqlite, else `sqlite:///ftmq_search.db`), `FTMQ_SEARCH_SQL_TABLE_NAME` (FTS5 table name, default `ftmq_search`), `FTMQ_SEARCH_YAML_URI` / `FTMQ_SEARCH_JSON_URI` (load a store configuration document).
