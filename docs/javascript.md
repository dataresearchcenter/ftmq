# Javascript

`@dataresearchcenter/ftmq` is a TypeScript client for the [ftmq api](./api.md). Its `Query` mirrors the Python [`ftmq.Query`](./query.md) with the same semantics and serialization surfaces, so a client app can build queries, parse them back out of a url, and send them to the api.

The followthemoney model is not reimplemented: the client returns raw entity data (`IEntityDatum`) compatible with [`@opensanctions/followthemoney`](https://github.com/opensanctions/followthemoney).

## Install

```bash
npm install @dataresearchcenter/ftmq @opensanctions/followthemoney
```

The package ships ES modules. `@opensanctions/followthemoney` is only needed to hydrate results into `Entity` proxies (see [Working with results](#working-with-results)).

## Quick start

```ts
import { Api, Query, M, P } from "@dataresearchcenter/ftmq";

const api = new Api("https://api.example.org");

const query = new Query()
  .where(M({ schema: "Person" }), P({ name__ilike: "jane" }))
  .orderBy(P("name"), { ascending: false })
  .slice(0, 25);

const result = await api.getEntities(query);
console.log(result.total, result.results.length);
```

The optional second constructor argument is an api key (server-side only), which lifts the public pagination cap:

```ts
const api = new Api("https://api.example.org", process.env.FTMQ_API_KEY);
```

## The Query

Four node constructors, as in Python, each taking an object of `field[__comparator]: value` lookups:

| Node | Targets | Example |
|---|---|---|
| `M` | meta: `dataset`, `schema`, `schemata`, `id`, `entity_id`, `canonical_id` | `M({ schema: "Person" })` |
| `P` | a specific followthemoney property | `P({ name__ilike: "jane" })` |
| `G` | a property-type group (`countries`, `dates`, `entities`, ...) | `G({ countries: "de" })` |
| `C` | a context field (`origin`, ...) | `C({ origin: "crawl" })` |

Nodes compose with the free functions `and`, `or`, `not` (or the `.and()` / `.or()` / `.not()` methods):

```ts
import { Query, M, P, G, or, not } from "@dataresearchcenter/ftmq";

const query = new Query()
  .where(M({ schema: "Person" }))
  .where(or(G({ countries: "de" }), G({ countries: "at" }))) // nested OR
  .where(not(P({ status__ilike: "%dissolved%" })))
  .orderBy(P("incorporationDate"), { ascending: false })
  .slice(0, 25); // offset, offset + limit
```

`.where()` AND-combines its nodes (chained calls also AND); `.slice(start, stop)` sets offset / limit; `.orderBy(P(name), { ascending })` sorts by a single property (sent as `sort=properties.<name>`; the server rejects any other field).

### Comparators

Any lookup key takes a `__<comparator>` suffix (default equals): `gt` / `gte` / `lt` / `lte`, `like` / `ilike`, `startswith` / `endswith`, `in` / `not_in`, `not`, and `null` (presence):

```ts
P({ amountEur__gte: 1000 });
M({ dataset__in: ["leaks", "sanctions"] });
G({ dates__gte: "2020" });
G({ entities: "some-entity-id" }); // reverse lookup (any edge pointing here)
P({ deathDate__null: true }); // entities without a deathDate
```

Schema / property / group names are not validated client-side; the api answers an invalid query with a 400.

### Aggregations

Add `A(...)` nodes and read the response `metrics` (ungrouped) and `facets` (grouped); `.slice(0, 0)` returns only the aggregations. Fields are references (`M` / `P` / `G` / `C` called with a bare field name, plus `Year()`), keyed in the response by their wire spelling.

```ts
import { Query, M, P, A, Year } from "@dataresearchcenter/ftmq";

const query = new Query()
  .where(M({ schema: "Payment" }))
  .aggregate(A({ count: M("id"), by: Year() }), A({ sum: P("amountEur") }));

// alongside a page of entities
const page = await api.getEntities(query.slice(0, 25));
page.metrics; // ungrouped: { "properties.amountEur": { sum: ... } }
page.facets; // grouped: { year: { values: [{ value, label, count, metrics }], total } }

// rank buckets by a metric instead of entity count, and set the bucket count (default 20)
const ranked = query.orderFacets({ count: M("id"), ascending: true }).facetSize(Year(), 5);

// aggregations only: slice to limit 0 (no entities)
const { facets, metrics } = await api.getEntities(ranked.slice(0, 0));
```

## Parsing urls into a Query

Every surface round-trips, so an app can rebuild a `Query` from a url:

```ts
// e.g. from a browser location or a link
const query = Query.fromString(location.search);
// mutate and re-issue
const next = query.slice(0, 50);
await api.getEntities(next);
```

`Query.fromParams(new URLSearchParams(...))`, `Query.fromRql(rqlString)` and `Query.fromDict(json)` parse the other surfaces.

## Serialization surfaces

The same four surfaces as the Python `ftmq.Query`:

```ts
query.toDict(); // lossless nested tree (round-trips any query)
query.toParams(); // Aleph filter params (URLSearchParams-ready); throws on a nested tree
query.toString(); // Aleph url query string
query.toRql(); // RQL string (carries an arbitrarily nested tree + aggregations)
```

Queries parse across languages. `toParams` / `toString` are byte-identical to Python; `toDict` / `toRql` parse back to the same query but may order children differently. The client sends a flat query as Aleph params and a nested one as `rql=`.

## Client methods

| Method | Endpoint |
|---|---|
| `getCatalog()` | `/catalog` |
| `getDataset(name)` | `/catalog/{name}` |
| `getEntity(id, retrieve?)` | `/entities/{id}` |
| `getEntities(query?, retrieve?)` | `/entities` |
| `getEntitiesAll(query?, retrieve?)` | `/entities` (paginated) |
| `autocomplete(q)` | `/autocomplete` |

`retrieve` holds the non-query params: the flags `{ nested, featured, dehydrate, dehydrate_nested, stats }` and an optional full-text search term `q`. Without an api key, `limit` is capped to the public maximum.

The response is the OpenAleph api v2 envelope: `results`, `total`, `total_type`, `page`, `pages`, `limit`, `offset`, `next`, `previous`, `facets`, `metrics`, `filters`, `query_q`, plus the ftmq extensions `query` (the canonical `toDict`) and `stats`.

```ts
const page = await api.getEntities(query, { stats: true });
page.results; // IEntityDatum[]
page.total; // number of matches
page.page; // 1-based page number, page.pages total pages
page.next; // string | null (next-page url)
page.stats; // IDatasetStats | null

const all = await api.getEntitiesAll(new Query().where(M({ schema: "Company" })));

// full-text search: pass `q` alongside the query
const hits = await api.getEntities(new Query().where(G({ countries: "de" })), { q: "jane doe" });
const { candidates } = await api.autocomplete("jan");
```

## Working with results

Hydrate the plain `IEntityDatum` objects into entity proxies with the upstream model:

```ts
import { Model, defaultModel } from "@opensanctions/followthemoney";

const model = new Model(defaultModel);

const { results } = await api.getEntities(new Query().where(M({ schema: "Person" })));
for (const datum of results) {
  const entity = model.getEntity(datum);
  entity.getCaption();
  entity.getFirst("name");
  entity.getTypeValues("country");
}
```
