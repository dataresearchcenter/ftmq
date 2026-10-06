An aggregation computes `min`, `max`, `sum`, `avg` or `count` over the entities a [`Query`](./query.md) matches, optionally grouped by another field. It is a projection, not a filter: an `A` node does not compose with `& | ~` and is passed to `aggregate()`, not to `where()`.

## Field references

A field is addressed with the same `M` / `P` / `G` / `C` markers the [filter families](./query.md) use, called with a bare field name instead of `field=value`:

```python
from ftmq import M, P, G, C, Year

P("amountEur")      # a followthemoney property
G("countries")      # a property-type group
M("dataset")        # a meta field: id, entity_id, canonical_id, dataset, schema
C("origin")         # a context / storage column
Year()              # the year of any date-typed value (derived from `dates`)
```

Bare strings are rejected (`P("topics")` is the property, `G("topics")` the group).

## The `A` node

Each keyword is an aggregation function, its value the reference (or references) to aggregate; `by=` groups by one or more references.

```python
from ftmq import Query, M, P, G, A, Year

A(sum=P("amountEur"))                        # sum of amountEur
A(sum=P("amountEur"), by=P("beneficiary"))   # ... grouped by beneficiary
A(count=M("id"))                             # number of (distinct) entities
A(sum=[P("amountEur"), P("amount")])         # several fields
A(min=P("date"), max=P("date"))              # several functions in one node
A(count=M("id"), by=[G("countries"), Year()])
```

`count` counts distinct values.

!!! note "Numbers"

    `min` / `max` / `sum` / `avg` over a numeric property return numbers. In memory values are parsed with followthemoney's number parser; the SQL backends cast the stored value, and a value that isn't in the canonical number format (e.g. `"324,687.00"`) reads as `NULL` and drops out. Store writers normalize values on write; migrate an existing store with [`ftmq statements cast-types`](./cli.md#statements).

## Adding aggregations to a query

`Query.aggregate()` takes several `A` nodes, and chained calls accumulate:

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

Every backend returns the same result. On a [store view](./stores.md):

```python
from ftmq.store import get_store

view = get_store("sqlite:///followthemoney.store").default_view()
result = view.aggregations(q)
```

In memory, aggregations are collected while iterating; read them off the query afterwards:

```python
_ = list(q.apply_iter(entities))
result = q.aggregator.result
```

The result maps `function -> field -> value`, with a `groups` sub-mapping for grouped aggregations; fields are keyed by their wire spelling:

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

In [`Query.to_dict`][ftmq.Query.to_dict] / [`from_dict`][ftmq.Query.from_dict] aggregations are a flat spec list: `[{"func": "sum", "field": "properties.amountEur", "by": ["year"]}, ...]` (`by` omitted when ungrouped).

Fields use the filter wire spelling: `properties.<name>`, `group.<name>`, `context.<name>`; meta fields and `year` are bare. `filter:group.countries=de` and `facet=group.countries` address the same dimension.

[RQL](./query.md#rql) carries them losslessly via `sum`, `min`, `max`, `mean`, `count` and the `aggregate(groups..., funcs...)` grouping operator, next to the filter:

```python
q = Query().where(M(schema="Payment")).aggregate(A(count=M("id"), by=P("beneficiary")))
q.to_rql()   # "and(eq(schema,Payment),aggregate(properties.beneficiary,count(id)))"
```

In URL params ([`to_params`][ftmq.Query.to_params] / [`to_string`][ftmq.Query.to_string], back via `from_params` / `from_string`) each spec becomes `metric:<function>=<field>` and each grouped field `facet=<field>`:

```python
q = Query().aggregate(A(sum=P("amountEur"), by=P("beneficiary")))
q.to_string()   # "facet=properties.beneficiary&metric:sum=properties.amountEur"
```

Facets apply to all metrics: metrics with different groups collapse to their union on the way out, and every metric is grouped by every facet on the way in. A `facet` without a `metric:` groups an entity count: `?facet=group.countries` parses as `A(count=M("id"), by=G("countries"))`.

### Ranking facet buckets

Buckets are ranked by entity count. [`order_facets`][ftmq.Query.order_facets] ranks them by one of the query's grouped metrics instead (descending unless `ascending=True`); wire spelling `facet_sort=<func>:<field>[:asc]` in URL params and `to_dict`.

Each facet keeps its top 20 buckets, ties broken by value, on every backend. [`facet_size`][ftmq.Query.facet_size] sets another size per facet: `facet_size:<field>=N` in URL params, `{"facet_size": {field: N}}` in `to_dict`. Neither has an RQL operator.

```python
q = Query().aggregate(A(sum=P("amountEur"), by=P("beneficiary")))
q = q.order_facets(sum=P("amountEur")).facet_size(P("beneficiary"), 5)
q.to_string()   # "facet=properties.beneficiary&facet_size:properties.beneficiary=5&facet_sort=sum%3Aproperties.amountEur&metric:sum=properties.amountEur"
```

The metric must be a grouped aggregation, and the facet size a facet, of the query; otherwise a [`QueryError`][ftmq.QueryError] is raised.

## Reference

See the [aggregations reference][ftmq.query.aggregations] and the [field references][ftmq.query.refs].
