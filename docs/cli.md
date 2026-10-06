`ftmq` reads (and writes) [Follow The Money entities](https://followthemoney.tech/docs/) from a line-based input stream, a file uri or a store uri.

```bash
cat entities.ftm.json | ftmq <filter expression> > output.ftm.json
```

Input `-i` and output `-o` accept any [anystore](https://github.com/investigativedata/anystore) uri:

```bash
ftmq <filter expression> -i ~/Data/entities.ftm.json
ftmq <filter expression> -i https://example.org/data.json.gz
ftmq <filter expression> -i s3://data-bucket/entities.ftm.json
ftmq <filter expression> -i webhdfs://host:port/path/file
cat data.json | ftmq <filter expression> -o s3://data-bucket/output.json
```

## Filter expressions

A query is passed as a whole query string in one of the [`Query`](./query.md) string surfaces: `-q` / `--query` for the [Aleph](https://openaleph.org) filter dialect ([`Query.from_string`][ftmq.Query.from_string]) or `--rql` for [RQL](https://github.com/pjwerneck/pyrql) ([`Query.from_rql`][ftmq.Query.from_rql]). Both are repeatable; several strings AND together.

The only filter shortcut is `-d` / `--dataset` (repeatable; several datasets are alternatives):

```bash
cat entities.ftm.json | ftmq -d ec_meetings
```

### Aleph filter string

`filter:` is a match, `exclude:` a negation, `empty:` an unset field; a comparator goes in the middle, `filter:<comparator>:<field>=<value>`:

```bash
cat entities.ftm.json | ftmq -q 'filter:schema=Person&filter:group.countries=de'
cat entities.ftm.json | ftmq -q 'filter:properties.name=Jane&exclude:properties.country=ru'
cat entities.ftm.json | ftmq -q 'filter:gte:properties.date=2020&empty:properties.deathDate'
```

Field spelling, the same on every string surface:

- bare - a meta field: `dataset`, `schema` (exact), `schemata` (is-a, i.e. the schema and its descendants), `id`, `entity_id`, `canonical_id`
- `properties.<name>` - a specific [property](https://followthemoney.tech/explorer/)
- `group.<name>` - a property-type group (`names`, `dates`, `countries`, `entities`, ...)
- `context.<name>` - a context / provenance field (`origin`, ...)

```bash
# companies based in Germany (the literal `country` property)
cat entities.ftm.json | ftmq -q 'filter:schema=Company&filter:properties.country=de'

# any country-typed property equal to `de` (the `countries` group)
cat entities.ftm.json | ftmq -q 'filter:group.countries=de'

# a schema and all its descendants (the is-a `schemata` field)
cat entities.ftm.json | ftmq -q 'filter:schemata=LegalEntity'

# by origin (context) and entity id prefix
cat entities.ftm.json | ftmq -q 'filter:context.origin=crawl&filter:startswith:id=de-'

# reverse lookup: entities pointing at an id (the `entities` group)
cat entities.ftm.json | ftmq -q 'filter:group.entities=some-entity-id'
```

Comparators:

- `gt` / `lt` / `gte` / `lte` - greater / lower (than or equal)
- `like` / `ilike` - substring / case-insensitive substring
- `startswith` / `endswith` - prefix / suffix
- a repeated key is an `in` list (under `exclude:`, its negation)

`exclude:properties.country=ru` also keeps the entities without a `country`.

Sorting and slicing go in the same string (`sort=properties.<name>[:asc|:desc]`, `limit=`, `offset=`):

```bash
cat entities.ftm.json | ftmq -q 'filter:schema=Company&sort=properties.name:desc&limit=10'
```

### RQL

For a nested filter tree (the Aleph string has no cross-field `OR`), pass an RQL string:

```bash
# schema=Person AND (countries=de OR countries=at)
cat entities.ftm.json | ftmq --rql 'and(eq(schema,Person),or(eq(group.countries,de),eq(group.countries,at)))'
# NOT Organization, with a name in a list
cat entities.ftm.json | ftmq --rql 'and(not(eq(schema,Organization)),in(name,(jane,joe)))'
```

RQL comparators are `eq` / `ne` / `lt` / `le` / `gt` / `ge` / `in` / `out` / `like` / `ilike`. RQL carries filters, aggregations and `select`, but no sorting or slicing (use `-q` for those).

## Aggregations

A query string carrying aggregations writes their result to `-o` (stdout by default) instead of the entities. Aleph dialect: `metric:<func>=<field>`, grouped by `facet=<field>`; RQL: `sum(...)` / `mean(...)` / `min(...)` / `max(...)` / `count(...)`, grouped by wrapping them in `aggregate(<field>, ...)`:

```bash
cat entities.ftm.json | ftmq -q 'filter:schema=Payment&metric:sum=properties.amountEur&facet=year'

cat entities.ftm.json | ftmq --rql 'and(eq(schema,Payment),aggregate(year,sum(properties.amountEur)))'
```

Against a [store](./stores.md) the backend computes the aggregation:

```bash
ftmq -i sqlite:///followthemoney.store -q 'filter:schema=Payment&metric:sum=properties.amountEur'
```

A `limit` slices what the in-memory aggregation of a file stream sees, while a store aggregation covers the whole matching set.

## Statistics

`--stats` writes coverage statistics of the result (schemata, countries, date range, entity count) instead of the entities. A store computes them itself:

```bash
cat entities.ftm.json | ftmq -q 'filter:schema=Payment' --stats
ftmq -i sqlite:///followthemoney.store -d my_dataset --stats
```

## Statements

`ftmq statements` works on raw statement streams (`csv`, `json` or `pack`, via `--input-format` / `--output-format`).

`cast-types` normalizes values into their property type's canonical format: numbers lose thousands separators and units (`"324,687.00"` -> `"324687.00"`, `"5 kg"` -> `"5"`), dates become ISO (partial dates are kept). The raw string moves to `original_value` and the statement id is regenerated:

```bash
cat statements.csv | ftmq statements cast-types > statements.typed.csv
```

The SQL backends read numbers in this format when aggregating or sorting. Statement stores cast on write; use `cast-types` to migrate data written outside ftmq.

Values that do not parse pass through unchanged (and are logged); `--drop-invalid` drops them. Restrict casting with `-t` / `--type` (`number`, `date`):

```bash
cat statements.csv | ftmq statements cast-types -t number --drop-invalid -o s3://data/statements.csv
```

### Statements in and out of a store

`read` dumps a store's statements, `write` loads a statement stream into a store. This is how a store is [resolved](./stores.md#merged-entities-resolver-linker) with the nomenklatura resolver tooling:

```bash
nomenklatura dump-resolver resolver.ijson
ftmq statements read -i duckdb://followthemoney.duckdb -o statements.csv
nomenklatura apply-statements -i statements.csv -o resolved.csv
ftmq statements write -i resolved.csv -o duckdb://followthemoney.duckdb
```

`write` updates the store in place: the statement id does not cover `canonical_id`, so resolved rows upsert onto their originals. Nothing is duplicated or deleted, so it also adds a dump to a store holding other data. `write` keeps each statement's `canonical_id` and never re-derives it from a resolver, so an unresolved stream loads unresolved.

`read` yields the rows as stored (external statements included, unordered), so a dump loads back verbatim. It needs a SQL-family backend (sqlite, postgres, duckdb, lake). Restrict it to one dataset with `-d`:

```bash
ftmq statements read -i sqlite:///followthemoney.store -d my_dataset -o statements.csv
```

Use `csv` or `json` when a resolver is involved: `pack` has no `canonical_id` column and reads merged statements back as their referents (a warning is logged).

The [delta lake store](./stores.md) appends instead of upserting, so loading into it adds rows, and a dump does not carry its `fragment` column.
