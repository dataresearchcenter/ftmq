# Javascript

`@dataresearchcenter/ftmq` is a TypeScript client for the [ftmq api](./api.md). It provides a composable `Query` that mirrors the Python [`ftmq.Query`](./query.md) with the same semantics and the same serialization surfaces, so a client app can build queries, parse them back out of a url, and talk to the api, all with one query model shared across both languages.

The followthemoney data model itself (entities, schemata, property types) is not reimplemented here: it comes from [`@opensanctions/followthemoney`](https://github.com/opensanctions/followthemoney), which the client returns raw entity data (`IEntityDatum`) compatible with.

## Install

```bash
npm install @dataresearchcenter/ftmq @opensanctions/followthemoney
```

The package ships ES modules; `@opensanctions/followthemoney` is only needed to hydrate returned entity data into rich `Entity` proxies (see [Working with results](#working-with-results)).

## Quick start

```ts
import { Api, Query, M, P } from "@dataresearchcenter/ftmq";

const api = new Api("https://api.example.org");

const query = new Query()
  .where(M({ schema: "Person" }), P({ name__ilike: "jane" }))
  .orderBy("-name")
  .slice(0, 25);

const result = await api.getEntities(query);
console.log(result.total, result.entities.length);
```

The constructor takes the api base url and an optional api key. The key is sent server-side only and lifts the public pagination cap:

```ts
const api = new Api("https://api.example.org", process.env.FTMQ_API_KEY);
```

## The Query

`Query` is built from four node constructors, mirroring the Python grammar. Each takes an object of `field[__comparator]=value` lookups:

| Node | Targets | Example |
|---|---|---|
| `M` | meta: `dataset`, `schema`, `schemata`, `id`, `entity_id`, `canonical_id` | `M({ schema: "Person" })` |
| `P` | a specific followthemoney property | `P({ name__ilike: "jane" })` |
| `G` | a property-type group (`countries`, `dates`, `entities`, ...) | `G({ countries: "de" })` |
| `C` | a context field (`origin`, ...) | `C({ origin: "crawl" })` |

Nodes compose into arbitrary boolean trees with the free functions `and`, `or`, `not` (or the equivalent `.and()` / `.or()` / `.not()` methods):

```ts
import { Query, M, P, G, or, not } from "@dataresearchcenter/ftmq";

const query = new Query()
  .where(M({ schema: "Person" }))
  .where(or(G({ countries: "de" }), G({ countries: "at" }))) // nested OR
  .where(not(P({ status__ilike: "%dissolved%" })))
  .orderBy("-incorporationDate") // leading `-` = descending
  .slice(0, 25); // offset, offset + limit
```

`.where()` AND-combines its nodes (chained `.where()` also ANDs); `.slice(start, stop)` sets offset / limit; `.orderBy(...fields)` sorts.

### Comparators

Any lookup key may carry a `__<comparator>` suffix (the default is equals): `gt` / `gte` / `lt` / `lte` (ranges), `like` / `ilike` (substring), `startswith` / `endswith`, `in` / `not_in` (lists), `not`, and `null` (presence):

```ts
P({ amountEur__gte: 1000 });
M({ dataset__in: ["leaks", "sanctions"] });
G({ dates__gte: "2020" });
G({ entities: "some-entity-id" }); // reverse lookup (any edge pointing here)
P({ deathDate__null: true }); // entities without a deathDate
```

Validity of schema / property / group names is not checked client-side; the api rejects an invalid query with a 400.

### Aggregations

Aggregations ride on the entities query (as in the Aleph api): add `A(...)` nodes and read the response `metrics` (ungrouped) and `facets` (grouped). Slice to `limit=0` (via `.slice(0, 0)`) to fetch only the aggregations, no entities.

```ts
import { Query, M, A } from "@dataresearchcenter/ftmq";

const query = new Query()
  .where(M({ schema: "Payment" }))
  .aggregate(A({ count: "id", by: "year" }), A({ sum: "amountEur" }));

// alongside a page of entities
const page = await api.getEntities(query.slice(0, 25));
page.metrics; // ungrouped: { amountEur: { sum: ... } }
page.facets; // grouped: { year: { values: [{ value, label, count }], total } }

// aggregations only: slice to limit 0 (no entities)
const { facets, metrics } = await api.getEntities(query.slice(0, 0));
```

## Parsing urls into a Query

Every serialization surface round-trips, so a client app can reconstruct a `Query` from an incoming url (the point of sharing the query model with the server):

```ts
// e.g. from a browser location or a link
const query = Query.fromString(location.search);
// mutate and re-issue
const next = query.slice(0, 50);
await api.getEntities(next);
```

`Query.fromParams(new URLSearchParams(...))`, `Query.fromRql(rqlString)` and `Query.fromDict(json)` parse the other surfaces.

## Serialization surfaces

A `Query` serializes to the same four surfaces as the Python `ftmq.Query`:

```ts
query.toDict(); // lossless nested tree (round-trips any query)
query.toParams(); // Aleph filter params (URLSearchParams-ready); throws on a nested tree
query.toString(); // Aleph url query string
query.toRql(); // RQL string (carries an arbitrarily nested tree + aggregations)
```

A query produced in Python parses in the TypeScript client and vice versa. The sorted-key surfaces (`toParams` / `toString`) are byte-for-byte identical across languages; `toDict` / `toRql` are order-independent (they parse back to the same query, but child ordering is not guaranteed to match). The client picks the wire format itself: a flat query is sent as Aleph params, a nested one as `rql=`.

## Client methods

| Method | Endpoint |
|---|---|
| `getCatalog()` | `/catalog` |
| `getDataset(name)` | `/catalog/{name}` |
| `getEntity(id, retrieve?)` | `/entities/{id}` |
| `getEntities(query?, retrieve?)` | `/entities` |
| `getEntitiesAll(query?, retrieve?)` | `/entities` (paginated) |
| `search(q, query?, retrieve?)` | `/entities?q=` |
| `autocomplete(q)` | `/autocomplete` |

`retrieve` shapes the response: `{ nested, featured, dehydrate, dehydrate_nested, stats }`. An unauthenticated `limit` is capped to the public maximum; pass an api key to exceed it.

The response matches the OpenAleph api v2 envelope: `results`, `total`, `total_type`, `page`, `pages`, `limit`, `offset`, `next`, `previous`, `facets`, `metrics`, `filters`, `query_q`, plus the ftmq extensions `query` (the canonical `toDict`) and `stats`.

```ts
const page = await api.getEntities(query, { stats: true });
page.results; // IEntityDatum[]
page.total; // number of matches
page.page; // 1-based page number, page.pages total pages
page.next; // string | null (next-page url)
page.stats; // IDatasetStats | null

const all = await api.getEntitiesAll(new Query().where(M({ schema: "Company" })));

const hits = await api.search("jane doe", new Query().where(G({ countries: "de" })));
const { candidates } = await api.autocomplete("jan");
```

## Working with results

The client returns plain `IEntityDatum` objects. To get a rich entity proxy (caption, typed property access, ...) hydrate them with the upstream model:

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
