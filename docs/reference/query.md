# `ftmq.Query`

See the [query guide](../query.md) for a narrative introduction.

::: ftmq.Query

## Query nodes

The composable filter-node constructors: `M` (meta), `P` (property), `G` (property-type group) and `C` (context). `A` is the aggregation-projection node (see [Aggregations](#aggregations) below).

::: ftmq.M

::: ftmq.P

::: ftmq.G

::: ftmq.C

::: ftmq.A

## Expression tree

::: ftmq.query.Expr

::: ftmq.query.combine

## Leaves

::: ftmq.query.leaves

## Aleph bridge

The filter half of the [Aleph URL-param grammar](../query.md#openaleph). `Query.to_params` / `from_params` and `to_string` / `from_string` wrap these.

::: ftmq.query.aleph

## RQL bridge

[RQL](https://github.com/pjwerneck/pyrql) support for **nested** filter trees, used by [`Query.from_rql`][ftmq.Query.from_rql].

::: ftmq.query.rql

## Aggregations

The [`A`][ftmq.A] projection node, the immutable `Agg` spec and the in-memory `Aggregator`. See the [aggregation guide](../aggregation.md).

::: ftmq.query.aggregations

## SQL

The SQL translation. A store passes its [`SqlSource`][ftmq.query.sql.SqlSource] to [`Query.compile`][ftmq.Query.compile] (or builds `Sql(query, source)` directly).

::: ftmq.query.sql.SqlSource

::: ftmq.query.sql.Sql

## Errors

::: ftmq.QueryError
