`ftmq` extends the statement stores of [`nomenklatura`](https://github.com/opensanctions/nomenklatura) with [querying](./query.md) and [aggregation](./aggregation.md).

## Initialize a store

::: ftmq.store.get_store

### Supported backends

- in memory: `get_store("memory://")`
- LevelDB: `get_store("leveldb://data")` (needs the `level` extra)
- Sql:
    - sqlite: `get_store("sqlite:///data.db")`
    - postgresql: `get_store("postgresql://user:password@host/db")`
    - duckdb: `get_store("duckdb://data.duckdb")` (needs the `duckdb` extra)
    - ...any other supported by [`sqlalchemy`](https://www.sqlalchemy.org/)
- Delta lake: `get_store("lake+s3://bucket/path")` (needs the `lake` extra)

duckdb paths follow the scheme directly: `duckdb://relative.duckdb`, `duckdb:///absolute/path.duckdb`; an empty path (or `duckdb://:memory:`) opens an in-memory database. The delta lake store (`lake+...`) is different: it queries parquet files through duckdb instead of owning a database file.

## Merged entities (resolver / linker)

A store resolves entity ids through a [`nomenklatura`](https://github.com/opensanctions/nomenklatura) `Linker`, the deduplication decisions that merge several source ids into one canonical entity. Without one every id is its own entity.

Both sources live in [`ftmq.store.base`][ftmq.store.base]:

- [`get_resolver`][ftmq.store.base.get_resolver]: the read/write `Resolver` over a `resolver` table in a sql database. This is the default; a store opened without a `linker` puts the table in its own database (a non-sql store gets an ephemeral in-memory one). Decisions are loaded into memory on construction and the object is cached, so to see another writer's decisions call `load_into_memory()`.
- [`get_linker`][ftmq.store.base.get_linker]: the read-only `Linker`. Its uri is a sql database as above, or an edge dump from `Resolver.dump()` / `nomenklatura dump-resolver` (json lines) on any [anystore](https://docs.investigraph.dev/lib/anystore) location.

```python
from ftmq.store import get_store
from ftmq.store.base import get_linker

# merge decisions from a json dump, entities from a sql store
store = get_store("sqlite:///followthemoney.store", linker=get_linker("s3://data/resolver.ijson"))
```

**A linker resolves ids, not data.** Stores read by the `canonical_id` column, so the merge has to be in the statements. The sql-family and lake writers stamp the canonical id on write; a store written before the decisions existed keeps the old ids, and filters, counts, aggregations and the search index still see the cluster members separately (`filter:id=<canonical>` matches nothing).

**Resolve the data before serving it**: dump the statements, apply the decisions with the nomenklatura cli, and load them back with `ftmq statements read` / `write` (see [the cli docs](./cli.md#statements-in-and-out-of-a-store)):

```bash
nomenklatura dump-resolver resolver.ijson
ftmq statements read -i sqlite:///followthemoney.store -o statements.csv
nomenklatura apply-statements -i statements.csv -o resolved.csv
ftmq statements write -i resolved.csv -o sqlite:///followthemoney.store
```

The reload upserts in place (the statement id does not cover `canonical_id`), and `write` keeps the canonical id each statement carries.

New data needs none of this; the writer applies the linker it was given:

```python
store = get_store("sqlite:///resolved.store", linker=get_linker("resolver.ijson"))
with store.writer() as bulk:
    for proxy in smart_read_proxies("entities.ftm.json"):
        bulk.add_entity(proxy)
```

A resolved store still needs the linker to look up a *referent* id: `get_entity("left-1")` finds nothing unless the id is mapped to its canonical first (as [`ftmq.api.store.get_entity`][ftmq.api.store.get_entity] does). Filters, counts and aggregations read the resolved ids from the data.

The in-memory store keeps the canonical id a statement already carries, so apply merges before writing to it. Dumping statements (`ftmq statements read`, [`Store.statements`][ftmq.store.base.Store.statements]) needs a SQL-family backend.

## Read and query entities

Iterate all entities via [`Store.iterate`][ftmq.store.base.Store.iterate]:

```python
from ftmq.store import get_store

store = get_store("sqlite:///followthemoney.store")
proxies = store.iterate()
```

Filter with a [`Query`](./query.md) on a [store view][ftmq.store.base.View]:

```python
from ftmq import Query, M

q = Query().where(M(dataset="my_dataset"), M(schema="Person"))
view = store.default_view()
proxies = view.query(q)
```

A view hides `external` statements (unaccepted enrichment candidates) in `query()`, `count()`, `stats()` and `aggregations()`, unless built with `store.view(scope, external=True)`.

### Command line

```bash
ftmq -i sqlite:///followthemoney.store -d my_dataset -q 'filter:schema=Person'
```

[cli reference](./reference/cli.md)

## Write entities to a store

Use the bulk writer:

```python
proxies = [...]

with store.writer() as bulk:
    for proxy in proxies:
        bulk.add_entity(proxy)
```

Or [`smart_write_proxies`][ftmq.io.smart_write_proxies], which uses the same writer:

```python
from ftmq.io import smart_write_proxies

smart_write_proxies("sqlite:///followthemoney.store", proxies)
```

The writer normalizes number and date values (`"324,687.00"` is stored as `"324687.00"`, the raw string as `original_value`), which the SQL backends rely on for numeric aggregation and sorting. `get_store(..., cast_types=False)` skips it; migrate existing data with [`ftmq statements cast-types`](./cli.md#statements).

### Command line

```bash
cat entities.ftm.json | ftmq -o sqlite:///followthemoney.store
```

Entities without a dataset are stored in the `default` dataset. Stamp a named one on with [`ftmq apply-dataset`](./cli.md) (`--replace-dataset` puts them in that dataset alone; a statement carries exactly one):

```bash
ftmq apply-dataset -d my_dataset --replace-dataset -i s3://data/entities.ftm.json -o sqlite:///followthemoney.store
```

[cli reference](./reference/cli.md)
