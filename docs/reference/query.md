# `ftmq.Query`

See the [query guide](../query.md).

::: ftmq.Query

## Query nodes

Filter nodes `M` (meta), `P` (property), `G` (property-type group) and `C` (context), plus the [aggregation](#aggregations) node `A`.

::: ftmq.M

::: ftmq.P

::: ftmq.G

::: ftmq.C

::: ftmq.A

## Expression tree

::: ftmq.query.Expr

::: ftmq.query.combine

## Field references

A field without a value, as used by [aggregations](../aggregation.md), sorting and `select`: a family constructor called with a bare field name (`P("amountEur")`).

::: ftmq.query.refs

## Leaves

::: ftmq.query.leaves

## Aleph bridge

The filter half of the [Aleph URL-param grammar](../query.md#openaleph), wrapped by `Query.to_params` / `from_params` and `to_string` / `from_string`.

::: ftmq.query.aleph

## RQL bridge

[RQL](https://github.com/pjwerneck/pyrql) support for nested filter trees, used by [`Query.from_rql`][ftmq.Query.from_rql].

::: ftmq.query.rql

## Aggregations

The [`A`][ftmq.A] node, the `Agg` spec and the in-memory `Aggregator`. See the [aggregation guide](../aggregation.md).

::: ftmq.query.aggregations

## SQL

A store passes its [`SqlSource`][ftmq.query.sql.SqlSource] to [`Query.compile`][ftmq.Query.compile] (or builds `Sql(query, source)` directly). Partition pruning is `prune={column: function}`, one rule per partition column.

::: ftmq.query.sql.SqlSource

::: ftmq.query.sql.prune_by_schema

::: ftmq.query.sql.Sql

## Errors

::: ftmq.QueryError
