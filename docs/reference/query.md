# `ftmq.Query`

See the [query guide](../query.md) for a narrative introduction.

::: ftmq.Query

## Query nodes

The composable node constructors: `M` (meta), `P` (property), `G` (property-type group) and `C` (context).

::: ftmq.M

::: ftmq.P

::: ftmq.G

::: ftmq.C

## Expression tree

::: ftmq.query.Expr

::: ftmq.query.combine

## Leaves

::: ftmq.query.leaves

## Aleph bridge

The filter half of the [Aleph URL-param grammar](../query.md#serialization-and-the-aleph-bridge). `Query.to_params` / `from_params` and `to_string` / `from_string` wrap these.

::: ftmq.query.aleph

## SQL

The SQL translation. A store passes its [`SqlSource`][ftmq.query.sql.SqlSource] to [`Query.compile`][ftmq.Query.compile] (or builds `Sql(query, source)` directly).

::: ftmq.query.sql.SqlSource

::: ftmq.query.sql.Sql

## Errors

::: ftmq.QueryError
