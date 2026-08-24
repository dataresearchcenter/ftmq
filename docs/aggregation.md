An aggregation computes a metric (`min`, `max`, `sum`, `avg` or `count`) over the entities a [`Query`](./query.md) matches, optionally grouped by another field. Aggregations are a *projection*, not a filter: they do not compose with the `& | ~` boolean tree, so an `A` node is not passed to `where()` but to `aggregate()`, alongside `order_by()` and slicing.

## Field references

An aggregation addresses a field with the same `M` / `P` / `G` / `C` markers the [filter families](./query.md) use, called with a bare field name instead of `field=value`. The result is a *reference*: the same field, no condition.

```python
from ftmq import M, P, G, C, Year

P("amountEur")      # a followthemoney property
G("countries")      # a property-type group
M("dataset")        # a meta field: id, entity_id, canonical_id, dataset, schema
C("origin")         # a context / storage column
Year()              # the year of any date-typed value (derived from `dates`)
```

Bare strings are not accepted - a field is always addressed through its family marker (`P("topics")` is the property, `G("topics")` the group).

## The `A` node

`A` mirrors the keyword style of the filter families: each keyword is an aggregation function and its value is the reference (or references) to aggregate. `by=` groups the result by one or more references.

```python
from ftmq import Query, M, P, G, A, Year

A(sum=P("amountEur"))                        # sum of amountEur
A(sum=P("amountEur"), by=P("beneficiary"))   # ... grouped by beneficiary
A(count=M("id"))                             # number of (distinct) entities
A(sum=[P("amountEur"), P("amount")])         # several fields
A(min=P("date"), max=P("date"))              # several functions in one node
A(count=M("id"), by=[G("countries"), Year()])
```

The functions are `min`, `max`, `sum`, `avg` and `count` (`count` is over *distinct* values).

!!! note "Numbers"

    `min` / `max` / `sum` / `avg` over a numeric property return numbers, not strings. In memory the values are read through followthemoney's number parser (values that do not parse are skipped); the SQL backends `CAST` the stored value, which must be in the canonical number format - store writers normalize values on write, and an existing store is migrated with [`ftmq statements cast-types`](./cli.md#statements). A store holding a display-formatted (`"324,687.00"`) or unparsable amount reads wrong on sqlite and raises on postgres / duckdb. `count` counts distinct raw values.

## Adding aggregations to a query

`Query.aggregate()` is variadic and additive: pass several `A` nodes in one call, or chain calls.

```python
q = (
    Query()
    .where(M(schema="Payment"))
    .aggregate(
        A(sum=P("amountEur"), by=P("beneficiary")),
        A(avg=P("amountEur")),
    )
)
q = q.aggregate(A(count=M("id")))     # chaining accumulates
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

The result is a nested mapping of `function -> field -> value`, with a `groups` sub-mapping for any grouped aggregation, fields keyed by their wire spelling:

```python
{
    "sum": {"properties.amountEur": 40589689.15},
    "groups": {
        "properties.beneficiary": {
            "sum": {"properties.amountEur": {"<entity-id>": 3368136.15, ...}}
        }
    },
}
```

## Serialization

Aggregations round-trip through [`Query.to_dict`][ftmq.Query.to_dict] / [`from_dict`][ftmq.Query.from_dict] as a flat list of specs - `[{"func": "sum", "field": "properties.amountEur", "by": ["year"]}, ...]` - fields spelled as on the wire, `by` omitted when ungrouped.

A reference uses the wire spelling shared with the filter grammar: `properties.<name>` for a property, `group.<name>` for a property-type group, `context.<name>` for a context column; meta fields and `year` are bare. `filter:group.countries=de` and `facet=group.countries` address the same dimension.

[RQL](./query.md#rql) carries them losslessly in a single string via its metric operators (`sum`, `min`, `max`, `mean`, `count`) and the `aggregate(groups..., funcs...)` grouping operator, side by side with the filter; per-node grouping is preserved exactly.

```python
q = Query().where(M(schema="Payment")).aggregate(A(count=M("id"), by=P("beneficiary")))
q.to_rql()   # "and(eq(schema,Payment),aggregate(properties.beneficiary,count(id)))"
```

In URL params ([`to_params`][ftmq.Query.to_params] / [`to_string`][ftmq.Query.to_string], back via `from_params` / `from_string`), each spec becomes a `metric:<function>=<field>` param and each grouped field a `facet=<field>` param.

```python
q = Query().aggregate(A(sum=P("amountEur"), by=P("beneficiary")))
q.to_string()   # "facet=properties.beneficiary&metric:sum=properties.amountEur"
```

`facet` groups apply across all metrics: a query whose metrics carry *different* groups collapses to their union on the way out, and every metric is grouped by every facet on the way back in. A `facet` without any `metric:` groups an entity count: `?facet=group.countries` parses as `A(count=M("id"), by=G("countries"))`.

## Reference

See the [aggregations reference][ftmq.query.aggregations] and the [field references][ftmq.query.refs].
