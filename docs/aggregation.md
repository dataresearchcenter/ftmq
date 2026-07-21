An aggregation computes a metric (`min`, `max`, `sum`, `avg` or `count`) over the entities a [`Query`](./query.md) matches, optionally grouped by a property or field. Aggregations are a *projection*, not a filter: they do not compose with the `& | ~` boolean tree, so an `A` node is not passed to `where()` but to `aggregate()`, alongside `order_by()` and slicing.

## The `A` node

`A` mirrors the `M` / `P` / `G` / `C` keyword style: each keyword is an aggregation function and its value is the property (or properties) to aggregate. `by=` groups the result by one or more properties / fields.

```python
from ftmq import Query, M, A

A(sum="amountEur")                       # sum of amountEur
A(sum="amountEur", by="beneficiary")     # ... grouped by beneficiary
A(count="id")                            # number of (distinct) entities
A(sum=["amountEur", "amount"])           # several properties
A(min="date", max="date")                # several functions in one node
```

The functions are `min`, `max`, `sum`, `avg` and `count` (`count` is over *distinct* values). You can aggregate any followthemoney property, plus the special fields `id`, `dataset`, `schema` and `year` (`year` is derived from any date-typed value).

## Adding aggregations to a query

`Query.aggregate()` is variadic and additive: pass several `A` nodes in one call, or chain calls.

```python
q = (
    Query()
    .where(M(schema="Payment"))
    .aggregate(
        A(sum="amountEur", by="beneficiary"),
        A(avg="amountEur"),
    )
)
q = q.aggregate(A(count="id"))     # chaining accumulates
```

## Running an aggregation

Aggregations run on any backend and return the same result. On a [store view](./stores.md):

```python
from ftmq.store import get_store

view = get_store("sqlite:///followthemoney.store").default_view()
result = view.aggregations(q)
```

In memory, the query collects its aggregations as a side effect of iterating; read the result off the query afterwards:

```python
_ = list(q.apply_iter(entities))
result = q.aggregator.result
```

The result is a nested mapping of `function -> property -> value`, with a `groups` sub-mapping for any grouped aggregation:

```python
{
    "sum": {"amountEur": 40589689.15},
    "groups": {
        "beneficiary": {"sum": {"amountEur": {"<entity-id>": 3368136.15, ...}}}
    },
}
```

## Serialization

Aggregations round-trip through [`Query.to_dict`][ftmq.Query.to_dict] / [`from_dict`][ftmq.Query.from_dict] as `{function: {properties}, "groups": {group: {function: {properties}}}}`.

[RQL](./query.md#rql) carries them losslessly in a single string via its native metric operators (`sum`, `min`, `max`, `mean`, `count`) and the `aggregate(groups..., funcs...)` grouping operator, side by side with the filter. Unlike the openaleph params below, RQL preserves per-node grouping exactly.

```python
q = Query().where(M(schema="Payment")).aggregate(A(count="id", by="beneficiary"))
q.to_rql()   # "and(eq(schema,Payment),aggregate(beneficiary,count(id)))"
```

Aggregations also map onto openaleph's metric-aggregation URL params, so an aggregating query survives [`to_params`][ftmq.Query.to_params] / [`to_string`][ftmq.Query.to_string] (and comes back via `from_params` / `from_string`): each spec becomes a `metric:<function>=<property>` param, and each grouped field a `facet=<field>` param.

```python
q = Query().aggregate(A(sum="amountEur", by="beneficiary"))
q.to_string()   # "facet=beneficiary&metric:sum=amountEur"
```

openaleph computes every metric inside every facet bucket, so `facet` groups apply across all metrics: a query whose metrics carry *different* groups collapses to their union on the way out, and every metric is grouped by every facet on the way back in.

## Reference

See the [aggregations reference][ftmq.query.aggregations].
