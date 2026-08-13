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
ftmq -i sqlite:///followthemoney.store --dataset=my_dataset --schema=Person
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
