One of the main features of `ftmq` is a high-level query interface for [Follow The Money](https://followthemoney.tech) data that is stored in a file or a statement-based store powered by [nomenklatura](https://github.com/opensanctions/nomenklatura).

To get familiar with the _Follow The Money_ ecosystem, you can have a look at [this pad here](https://pad.investigativedata.org/s/0qKuBEcsM#).

## Working with `Query`

The `Query` instance can be used to filter a stream of entities or to lookup entities from a store. The object itself acts independently and can be used in other applications as well.

```python
from ftmq import Query

# a basic query object to filter for a schema
q = Query(schema="Person")
```

`Query` objects can be chained:

```python
q = Query()
q = q.where(dataset="my_dataset").where(schema="Person")
```

### Filter lookups

The [`Query.where`][ftmq.Query.where] function can take filters for _datasets_, _schema_, _properties (values)_ or _entity ids_.

#### Dataset

```python
q = Query().where(dataset="my_dataset")
```

#### Schema

```python
q = Query().where(schema="Person")
```

#### Property

Any [valid property](https://followthemoney.tech/explorer/) from the model can be queried.

```python
q = Query().where(name="Jane")
```

#### Entity ID

```python
q = Query().where(id="id-jane")
```

#### Combining

These filters can be combined in a single `where` call:

```python
q = Query().where(dataset="my_dataset", schema="Person", name="Jane")
```

Or chained:

```python
q = Query().where(dataset="my_dataset").where(schema="Person").where(name="Jane")
```

### Value comparators

Lookups not only filter for _equal_ value lookup, but as well provide these comparators that can be appended with `__<comp>` to the property lookup.

- `eq` / `=` - equals
- `not` - not equals
- `gt` - greater than
- `lt` - lower than
- `gte` - greater or equal
- `lte` - lower or equal
- `like` - SQLish `LIKE` (use `%` placeholders)
- `ilike` - SQLish `ILIKE`, case-insensitive (use `%` placeholders)
- `in` - test if the value is in the given array of filter values
- `not_in` - test if the value is not in the given array of filter values
- `null` - testing for `NULL` values

```python
# Payments with a value >= 1000 €
q = Query().where(schema="Payment", amountEur__gte=1000)

# Events before october 2022
q = Query().where(schema="Event", date__lt="2022-10")

# Persons starting with "J"
q = Query().where(schema="Person", name__startswith="J")
# the same could be accomplished:
q = Query().where(schema="Person", name__ilike="j%")

# All Janes and Joes
q = Query().where(firstName__in=["Jane", "Joe"])

# ID prefixing
q = Query().where(id__startswith="de-")

# Exclude a specific legal form
q = Query().where(legalForm__not="gGmbH")

# Filter for null (empty) properties
q = Query().where(startDate__null=True)
```

### Sorting

A `Query` result can be sorted by properties in ascending or descending order. Subsequent calls of [`Query.order_by`][ftmq.Query.order_by] override a previous `order_by` definition.

```python
# sort by name
q = Query().order_by("name")

# sort by last name in descending order
q = q.order_by("lastName", ascending=False)
```

### Slicing

Slicing can be used to get the top first results:

```python
q = Query()[:100]

# get ten results starting from the 5th
q = q[5:15]

# Get only the 2nd result
q = q[1] # 0-index
```

## Putting it all together

Get the 10 highest `Payments` of a specific dataset within october 2024:

```python
q = Query().where(dataset="my_dataset") \
    .where(schema="Payment") \
    .where(date__gte="2024-10", date__lt="2024-11") \
    .order_by("amountEur", ascending=False)
q = q[:10]
```

## Using a `Query` instance

The query object can be passed to [`smart_read_proxies`][ftmq.io.smart_read_proxies]:

```python
from ftmq.io import smart_read_proxies
from ftmq import Query

q = Query().where(dataset="my_dataset", schema="Event")

for proxy in smart_read_proxies("s3://data/entities.ftm.json", query=q):
    assert proxy.schema.name == "Event"
```

Use for a _store view_:

```python
from ftmq.store import get_store
from ftmq import Query

q = Query().where(dataset="my_dataset", schema="Event")
store = get_store("sqlite:///followthemoney.store")
view = store.default_view()

for proxy in view.query(q):
    assert proxy.schema.name == "Event"
```

[See stores documentation](./stores.md)

## Reference

[Full reference][ftmq.Query]
