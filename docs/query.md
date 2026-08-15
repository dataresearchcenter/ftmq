One of the main features of `ftmq` is a high-level query interface for [Follow The Money](https://followthemoney.tech) data stored in a file, a stream, or a statement-based store powered by [nomenklatura](https://github.com/opensanctions/nomenklatura).

A `Query` is a composable, backend-agnostic filter over FtM entities. It is also the *canonical query representation* used across `ftmq`: the same object can be evaluated in memory, translated to SQL, or converted to and from the [Aleph / OpenAleph](https://openaleph.org) URL-param grammar.

## Where `Query` sits in the toolchain

`Query` is the hub. It is built from `M` / `P` / `G` / `C` nodes (or deserialized from a dict or from Aleph params), and from there it is either run against entities or projected back out to another representation.

```mermaid
flowchart TD
    subgraph build [Build]
        NODES["M / P / G / C nodes<br/>combined with &amp; | ~"]
        DICT["nested dict"]
        RQL["RQL string<br/>(nested)"]
        PARAMS["Aleph params /<br/>URL query string"]
    end

    Q(["<b>ftmq.Query</b><br/>canonical query IR"])

    NODES -->|where| Q
    DICT -->|from_dict| Q
    Q -->|to_dict| DICT
    RQL -->|from_rql| Q
    Q -->|to_rql| RQL
    PARAMS -->|from_params / from_string| Q
    Q -->|to_params / to_string| PARAMS

    subgraph run [Run against entities]
        MEM["in-memory<br/>memory · level · redis stores<br/>smart_read_proxies (files, streams)"]
        SQLB["SQL · Lake stores<br/>(statement tables)"]
    end

    Q -->|apply / apply_iter| MEM
    Q -->|.sql → SQLAlchemy| SQLB

    PARAMS <-->|same filter grammar| ALEPH["OpenAleph HTTP API<br/>openaleph-search<br/>SearchQueryParser"]
```

The param grammar works in both directions: an Aleph-style request can drive an `ftmq` store, and an `ftmq` `Query` can drive the OpenAleph API.

## Building a query

A query is built from four node constructors, split by the statement-table column they target:

| Node | Targets | Use it for |
|---|---|---|
| **`M`** (meta) | `dataset`, `schema` / `schemata`, entity ids | `M(schema="Person")`, `M(dataset__in=["d1", "d2"])` |
| **`P`** (property) | a specific FtM property | `P(name="Jane")`, `P(amountEur__gte=1000)` |
| **`G`** (group) | a property-*type* group (`prop_type`) | `G(countries="de")`, `G(dates__gte="2020")` |
| **`C`** (context) | a context / storage column | `C(origin="crawl")`, `C(first_seen__gte="2024-01")` |

```python
from ftmq import Query, M, P, G, C

q = Query().where(M(schema="Person"), P(name="Jane"))
```

`M` covers the metadata fields: `dataset`, `schema` / `schemata` (see below), and `id` / `entity_id` / `canonical_id`.

`P` matches a single, named property (example: [Person](https://followthemoney.tech/explorer/schemata/Person/)). `G` matches *any* property of a followthemoney [property type](https://followthemoney.tech/explorer/types), keyed by its group name (`names`, `dates`, `countries`, `emails`, `entities`, ...). For example `P(country="de")` matches the literal `country` property, while `G(countries="de")` matches any country-typed property (`nationality`, `jurisdiction`, `country`, ...).

`C` matches a context / storage column: `origin`, plus whatever extra columns a backend carries (`fragment`, `first_seen`, `bucket`, ...). Any key is accepted at build time; in memory it reads `entity.context[key]`, in SQL it maps to the same-named statement-table column and an unknown column raises [`QueryError`][ftmq.QueryError] at compile time. On the wire a context field spells `context.<name>` (`filter:context.origin=crawl`).

### `schema` vs `schemata`

`M(schema=...)` is an **exact** match, while `M(schemata=...)` matches an entity that **is-a** the given schema (the schema itself or any of its descendants):

```python
Query().where(M(schema="LegalEntity"))     # only entities whose schema is exactly LegalEntity
Query().where(M(schemata="LegalEntity"))   # LegalEntity, Company, Organization, Person, PublicBody, ...
Query().where(M(schema__in=["Person", "Company"]))   # exactly those two
```

### Combining conditions

Nodes compose into arbitrary boolean trees with `&` (and), `|` (or) and `~` (not):

```python
~M(schema="Organization")                              # NOT
P(name="Jane") | P(name__ilike="j%")                   # OR
M(schema="Person") & (G(countries="de") | G(countries="at"))   # nested
```

[`Query.where`][ftmq.Query.where] takes any number of nodes and combines them with **and**. Chained `.where()` calls also combine with **and**:

```python
q = Query().where(M(schema="Payment"), P(date__gte="2024-10"))
q = q.where(G(countries="de") | G(countries="at"))
```

Structurally equivalent queries (built in a different order) serialize and hash identically.

Nodes are canonicalized as they combine: a sub-group of the same connector merges into its parent, and a condition that appears twice (passed twice, or re-applied in a chained `.where()`) is held once. `Query(P(name="x"), P(name="x"))` is `Query(P(name="x"))` on every surface - serialization, hash and the compiled SQL alike.

### Value comparators

Any lookup can carry a comparator with the `__<comparator>` suffix (default is equals):

- `eq` (default) / `not` - (not) equals
- `gt` / `gte` / `lt` / `lte` - greater / lower (or equal)
- `in` / `not_in` - value (not) in a list
- `like` / `ilike` - (case-insensitive) substring match; `notlike` / `notilike` negate it. `%` and `_` in the value match literally
- `startswith` / `endswith`
- `null` - test for presence: `P(deathDate__null=True)` matches entities *without* a `deathDate`

```python
# Payments >= 1000 EUR, in October 2024
Query().where(M(schema="Payment"), P(amountEur__gte=1000), P(date__gte="2024-10"), P(date__lt="2024-11"))

# All Janes and Joes
Query().where(P(firstName__in=["Jane", "Joe"]))

# Exclude a legal form
Query().where(~P(legalForm="gGmbH"))
```

### Reverse lookups (edges)

A reverse lookup is a filter on an entity-typed value:

```python
G(entities="entity-id")     # any entity-typed property pointing at this id (any edge)
P(director="entity-id")     # a specific edge property pointing at this id
```

### Sorting and slicing

Sorting takes a single field:

```python
q = Query().order_by("name")                 # ascending
q = Query().order_by("date", ascending=False)
q = Query().order_by("-date")                # leading `-` = descending

q = Query()[:100]     # first 100
q = q[10:20]          # next 10
q = q[1]              # the 2nd result (0-indexed)
```

Aggregations are documented on the [aggregation](./aggregation.md) page.

## Running a query

Filter a stream of entities with [`apply`][ftmq.Query.apply] / [`apply_iter`][ftmq.Query.apply_iter], or pass the query to [`smart_read_proxies`][ftmq.io.smart_read_proxies]:

```python
from ftmq import Query, M
from ftmq.io import smart_read_proxies

q = Query().where(M(dataset="my_dataset"), M(schema="Event"))

for proxy in smart_read_proxies("s3://data/entities.ftm.json", query=q):
    assert proxy.schema.name == "Event"
```

Or use a [store view](./stores.md):

```python
from ftmq.store import get_store

store = get_store("sqlite:///followthemoney.store")
view = store.default_view()

for proxy in view.query(q):
    ...
```

!!! note "SQL / Lake stores: how the boolean tree compiles"
    The SQL / Lake translation ([`query.sql`][ftmq.Query.sql]) compiles arbitrary `& | ~` trees and follows the in-memory evaluator. A plain conjunction of distinct fields becomes flat `WHERE` predicates; property and group conditions, and any tree involving `OR`, negation or repeated fields, lift each condition to an entity-level `canonical_id IN (...)` sub-select. Chained same-field filters AND (as in memory); spell alternatives as `P(name__in=[...])` or `P(name="a") | P(name="b")`.

    Caveats: `like` on SQLite is case-insensitive for ASCII (SQLite's `LIKE` collation), unlike the in-memory and DuckDB substring test. A query of *only* meta / context fields (`M(dataset=...), M(schema=...)`) filters statement rows directly, so a canonical entity merged across datasets can match in memory but not in SQL; with any property / group condition (or a store view's scope) involved, the whole canonical entity comes back.

    Numeric aggregations (`sum` / `min` / `max` / `avg`) read the text `value` column as a number with an error-tolerant cast, so a stored value that isn't a number reads as `NULL` and drops out of the aggregate instead of failing the query - the same value is skipped in memory, where `registry.number.to_number` returns `None`. A store holding display-formatted amounts (`"324,687.00"`) therefore aggregates them as `NULL` rather than as the number they display; migrate it with `ftmq statements cast-types` so the SQL backends see the canonical format.

## Serialization

`Query` serializes to a lossless nested dict (round-trips any query), plus two URL-friendly string grammars: RQL (nested) and the flat OpenAleph params (interop).

Lossless nested tree (any query), for caching and storage:

```python
data = q.to_dict()
assert Query.from_dict(data).to_dict() == data
```

### RQL

[RQL](https://github.com/pjwerneck/pyrql) (Resource Query Language) is a compact, URL-friendly string of nestable operators - `and()` / `or()` / `not()` around comparisons - carrying an **arbitrarily nested** `& | ~` tree in a single string. [`from_rql`][ftmq.Query.from_rql] parses it and [`to_rql`][ftmq.Query.to_rql] emits it (via the `pyrql` dependency):

```python
q = Query().where(M(schema="Person") & (P(name="jane") | G(countries="de")))
q.to_rql()   # "and(eq(schema,Person),or(eq(properties.name,jane),eq(group.countries,de)))"
Query.from_rql(q.to_rql()).to_dict() == q.to_dict()   # True
```

Field names use the wire spelling shared by every string surface (params, RQL, [aggregations](./aggregation.md)): `properties.<name>` for a property, `group.<name>` for a property-type group, `context.<name>` for a context column; meta fields (`schema`, `dataset`, `id`, ...) and `year` are bare. A bare name that is none of those is read as a property. Comparison operators map to ftmq comparators (`eq`, `ne` → `not`, `lt` / `le` / `gt` / `ge`, `in`, `out` → `not_in`, `like` / `ilike`).

RQL also carries [aggregations](./aggregation.md) in the same string: its native `sum` / `min` / `max` / `mean` / `count` and `aggregate(...)` operators map onto `A` nodes, side by side with the filter under the top-level `and` (e.g. `and(eq(schema,Payment),aggregate(properties.beneficiary,sum(properties.amountEur)))`).

`to_rql` raises [`QueryError`][ftmq.QueryError] for a comparator with no RQL equivalent (`null`, `startswith`, `endswith`, `notlike`, `notilike`).

### OpenAleph

URL params (as [OpenAleph](https://openaleph.org) uses them), as a `MultiDict` or as a URL query string:

```python
q = Query().where(M(schema="Person"), G(countries="de"))
q.to_string()   # "filter:group.countries=de&filter:schema=Person"
Query.from_string("filter:schema=Person&filter:group.countries=de")
Query.from_params({"filter:schema": ["Person"], "filter:group.countries": ["de"]})
```

The bridge maps `ftmq` nodes onto the Aleph `filter:` / `exclude:` / `empty:` convention:

| Aleph param | ftmq node |
|---|---|
| `filter:schema=Person` / `filter:schemata=LegalEntity` | `M(schema=...)` / `M(schemata=...)` |
| `filter:dataset=d` / `filter:collection_id=d` | `M(dataset="d")` |
| `filter:id=x` / `filter:_id=x` | `M(id="x")` |
| `filter:properties.firstName=Jane` | `P(firstName="Jane")` |
| `filter:group.countries=de` (any group) | `G(countries="de")` |
| `filter:gte:properties.date=2018` | `P(date__gte=2018)` |
| `exclude:properties.country=ru` | `P(country__not="ru")` |
| `empty:properties.birthDate` | `P(birthDate__null=True)` |

The param grammar is flat, so [`to_params`][ftmq.Query.to_params] / [`to_string`][ftmq.Query.to_string] raise [`QueryError`][ftmq.QueryError] for a query that cannot be expressed as flat Aleph params (a cross-field `OR` or a negated group). [`from_params`][ftmq.Query.from_params] / [`from_string`][ftmq.Query.from_string] are total.

!!! note "Result fidelity"
    The query *language* round-trips in all directions. Exact *result-set* equivalence between the Elasticsearch backend and an `ftmq` statement store is best-effort for analyzed fields (e.g. `ilike` uses SQL `%` wildcards vs ES analyzers; the `names` group is name-normalized in ES). Aleph free-text search (`q` / `prefix`) has no `ftmq` equivalent.

## Reference

[Full reference][ftmq.Query]
