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
    - ...any other supported by [`sqlalchemy`](https://www.sqlalchemy.org/)
- Clickhouse via [`ftm-clickhouse`](https://github.com/investigativedata/ftm-columnstore/): `get_store("clickhouse://localhost")`

## Read and query entities

Iterate through all the entities via [`Store.iterate`][ftmq.store.base.Store.iterate]:

```python
from ftmq.store import get_store

store = get_store("sqlite:///followthemoney.store")
proxies = store.iterate()
```

Filter entities with a [`Query`](./query.md) object using a [store view][ftmq.store.base.View]:

```python
from ftmq import Query

q = Query().where(dataset="my_dataset", schema="Person")
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

### Command line

```bash
cat entities.ftm.json | ftmq -o sqlite:///followthemoney.store
```

If the input entities don't have a `dataset` property, ensure a default dataset with the `--store-dataset` parameter.

```bash
ftmq -i s3://data/entities.ftm.json -o sqlite:///followthemoney.store --store-dataset=my_dataset
```

[cli reference](./reference/cli.md)
