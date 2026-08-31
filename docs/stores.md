`ftmq` extends the statement based store implementation of [`nomenklatura`](https://github.com/opensanctions/nomenklatura) with more granular [querying](./query.md) and [aggregation](./aggregation.md) possibilities.

## Initialize a store

::: ftmq.store.get_store

### Supported backends

- in memory: `get_store("memory://")`
- Redis (or kvrocks): `get_store("redis://localhost")`
- LevelDB: `get_store("leveldb://data")`
- Sql:
    - sqlite: `get_store("sqlite:///data.db")`
    - postgresql: `get_store("postgresql://user:password@host/db")`
    - duckdb: `get_store("duckdb://data.duckdb")` (needs the `duckdb` extra)
    - ...any other supported by [`sqlalchemy`](https://www.sqlalchemy.org/)
- Clickhouse via [`ftm-clickhouse`](https://github.com/investigativedata/ftm-columnstore/): `get_store("clickhouse://localhost")`

The duckdb backend is the sql store against a [duckdb](https://duckdb.org) database file. Its path is spelled directly after the scheme: `duckdb://relative.duckdb`, `duckdb:///absolute/path.duckdb`, and an empty path (or `duckdb://:memory:`) opens an in-memory database. Don't confuse it with the delta lake store (`lake+...`), which queries parquet files through duckdb instead of owning a database file.

## Merged entities (resolver / linker)

A store resolves entity ids through a [`nomenklatura`](https://github.com/opensanctions/nomenklatura) `Linker`: the deduplication decisions that make several source ids one canonical entity. Without one every id is its own entity, which is why a store opened without a `linker` still works.

Two sources are supported, both via [`ftmq.store.base`][ftmq.store.base]:

- [`get_resolver`][ftmq.store.base.get_resolver] returns the read/write `Resolver` backed by a `resolver` table in a sql database. This is the default: a store opened without a `linker` puts the table in its own database (a non-sql store gets an ephemeral in-memory one). The decisions are loaded into memory when the resolver is built - it is a cached object, so a process that has to see another writer's decisions calls `load_into_memory()` itself.
- [`get_linker`][ftmq.store.base.get_linker] returns the read-only `Linker`: the merges without the judgement history. Its uri is either such a sql database, or an edge dump as written by `Resolver.dump()` / `nomenklatura dump-resolver` (json lines), which needs no database at all and can live anywhere [anystore](https://docs.investigraph.dev/lib/anystore) reads from.

```python
from ftmq.store import get_store
from ftmq.store.base import get_linker

# merge decisions from a json dump, entities from a sql store
store = get_store("sqlite:///followthemoney.store", linker=get_linker("s3://data/resolver.ijson"))
```

**A linker resolves ids, not data.** A statement store answers by the `canonical_id` column, so the merge has to be *in the statements*: the sql-family and lake writers stamp the canonical id onto everything they write, and a store written before the decisions existed keeps the old ids. Handing such a store a linker afterwards is not enough - a filter, a count, an aggregation and the search index all still see the cluster members as separate entities, and `filter:id=<canonical>` matches nothing.

**So resolve the data before serving it.** Either write the entities through a store that has the linker, or apply the decisions to a dump with the nomenklatura cli, which is what [`Linker.apply_statement`](https://github.com/opensanctions/nomenklatura) does per statement:

```bash
# export the decisions, then stamp them onto a statement dump
nomenklatura dump-resolver resolver.ijson
nomenklatura apply-statements -i statements.json -o resolved.json -f json

# or, for a stream of entities rather than statements
nomenklatura apply entities.ftm.json -o resolved.ftm.json
```

```python
# the same thing through a store: the writer applies the linker it was given
from ftmq.io import smart_read_proxies

store = get_store("sqlite:///resolved.store", linker=get_linker("resolver.ijson"))
with store.writer() as bulk:
    for proxy in smart_read_proxies("entities.ftm.json"):
        bulk.add_entity(proxy)
```

An already resolved store still needs the linker on the read side, for one thing: a lookup by a *referent* id. The statements carry the canonical id, so `get_entity("left-1")` finds nothing - the reader maps the id through the linker first (the api does this in [`ftmq.api.store.View.get_entity`][ftmq.api.store.View.get_entity]). Everything else - filters, counts, aggregations - reads the resolved ids straight out of the data.

The in-memory store is the exception to the write side: it keeps whatever canonical id a statement already carries, so merges have to be applied before writing to it.

## Read and query entities

Iterate through all the entities via [`Store.iterate`][ftmq.store.base.Store.iterate]:

```python
from ftmq.store import get_store

store = get_store("sqlite:///followthemoney.store")
proxies = store.iterate()
```

Filter entities with a [`Query`](./query.md) object using a [store view][ftmq.store.base.View]:

```python
from ftmq import Query, M

q = Query().where(M(dataset="my_dataset"), M(schema="Person"))
view = store.default_view()
proxies = store.query(q)
```

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

Or the [`smart_write_proxies`][ftmq.io.smart_write_proxies] shorthand, which uses the same bulk writer under the hood:

```python
from ftmq.io import smart_write_proxies

smart_write_proxies("sqlite:///followthemoney.store", proxies)
```

The writer normalizes number and date values on the way in (`"324,687.00"` is stored as `"324687.00"`, the raw string as the statement's `original_value`); the SQL backends rely on this format when aggregating or sorting numerically. Pass `cast_types=False` to `get_store` to skip it, and migrate existing data with [`ftmq statements cast-types`](./cli.md#statements).

### Command line

```bash
cat entities.ftm.json | ftmq -o sqlite:///followthemoney.store
```

Input entities that don't carry a `dataset` property are stored in the `default` dataset. To put them into a named one, stamp it on with [`ftmq apply-dataset`](./cli.md) (with `--replace-dataset`, so the entities end up in that dataset alone - a statement carries exactly one):

```bash
ftmq apply-dataset -d my_dataset --replace-dataset -i s3://data/entities.ftm.json -o sqlite:///followthemoney.store
```

[cli reference](./reference/cli.md)
