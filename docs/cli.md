`ftmq` accepts either a line-based input stream an argument with a file uri or a store uri to read (or write) [Follow The Money Entities](https://followthemoney.tech/docs/).

Input stream:

```bash
cat entities.ftm.json | ftmq <filter expression> > output.ftm.json
```

Under the hood, `ftmq` uses [anystore](https://github.com/investigativedata/anystore) to be able to interpret arbitrary file uris as argument `-i`:

```bash
ftmq <filter expression> -i ~/Data/entities.ftm.json
ftmq <filter expression> -i https://example.org/data.json.gz
ftmq <filter expression> -i s3://data-bucket/entities.ftm.json
ftmq <filter expression> -i webhdfs://host:port/path/file
```

Of course, the same is possible for output `-o`:

    cat data.json | ftmq <filter expression> -o s3://data-bucket/output.json

## Filter expressions

A query is passed as a whole query string, in one of the two string surfaces of the [`Query` language](./query.md): `-q` / `--query` for the [Aleph](https://openaleph.org) filter dialect (parsed by [`Query.from_string`][ftmq.Query.from_string]), `--rql` for [RQL](https://github.com/pjwerneck/pyrql) (parsed by [`Query.from_rql`][ftmq.Query.from_rql]). Both are repeatable, and several strings AND together.

One filter shortcut remains, `-d` / `--dataset` (repeatable; several datasets are alternatives):

```bash
cat entities.ftm.json | ftmq -d ec_meetings
```

### Aleph filter string

`filter:` is a match, `exclude:` a negation, `empty:` an unset field; a comparator is infixed as `filter:<comparator>:<field>=<value>`:

```bash
cat entities.ftm.json | ftmq -q 'filter:schema=Person&filter:group.countries=de'
cat entities.ftm.json | ftmq -q 'filter:properties.name=Jane&exclude:properties.country=ru'
cat entities.ftm.json | ftmq -q 'filter:gte:properties.date=2020&empty:properties.deathDate'
```

Fields take the wire spelling, the same on every string surface:

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

Possible comparators:

- `gt` / `lt` / `gte` / `lte` - greater / lower (than or equal)
- `like` / `ilike` - substring / case-insensitive substring
- `startswith` / `endswith` - prefix / suffix
- a repeated key is an `in` list (repeated under `exclude:`, a `not_in` one)

Sorting and slicing are part of the string as well (`sort=<field>[:asc|:desc]`, `limit=`, `offset=`):

```bash
cat entities.ftm.json | ftmq -q 'filter:schema=Company&sort=name:desc&limit=10'
```

### RQL

The Aleph string is flat (no cross-field `OR`). For a **nested** filter tree, pass an RQL string:

```bash
# schema=Person AND (countries=de OR countries=at)
cat entities.ftm.json | ftmq --rql 'and(eq(schema,Person),or(eq(group.countries,de),eq(group.countries,at)))'
# NOT Organization, with a name in a list
cat entities.ftm.json | ftmq --rql 'and(not(eq(schema,Organization)),in(name,(jane,joe)))'
```

RQL spells its comparators `eq` / `ne` / `lt` / `le` / `gt` / `ge` / `in` / `out` / `like` / `ilike`, and carries filters and aggregations only - sorting and slicing need `-q`.

## Aggregations

Aggregations ride on the same query string. A query carrying one writes its result instead of the entities, so `-o` (stdout by default) takes the aggregation. In the Aleph dialect they are `metric:<func>=<field>` with `facet=<field>` as the grouper; in RQL they are `sum(...)` / `mean(...)` / `min(...)` / `max(...)` / `count(...)` calls, grouped by wrapping them in `aggregate(<field>, ...)`:

```bash
cat entities.ftm.json | ftmq -q 'filter:schema=Payment&metric:sum=properties.amountEur&facet=year'

cat entities.ftm.json | ftmq --rql 'and(eq(schema,Payment),aggregate(year,sum(properties.amountEur)))'
```

Against a [store](./stores.md) the aggregation is computed by the backend (the SQL backends compile it into the query) rather than streaming every entity through the CLI:

```bash
ftmq -i sqlite:///followthemoney.store -q 'filter:schema=Payment&metric:sum=properties.amountEur'
```

One difference between the two: a `limit` in the query slices what the in-memory aggregation of a file stream sees, while a backend aggregation describes the whole matching set (`limit` is a page size there, and metrics cover the result, not the page).

## Statistics

`--stats` writes the coverage statistics of the result - schemata, countries, date range, entity count - instead of the entities. Like an aggregation, a store computes them itself:

```bash
cat entities.ftm.json | ftmq -q 'filter:schema=Payment' --stats
ftmq -i sqlite:///followthemoney.store -d my_dataset --stats
```

## Statements

`ftmq statements` works on raw statement streams (`csv`, `json` or `pack`, via `--input-format` / `--output-format`) instead of entities.

`cast-types` normalizes statement values into the canonical format of their property type: numbers lose their thousands separators and unit (`"324,687.00"` -> `"324687.00"`, `"5 kg"` -> `"5"`), dates become ISO (partial dates are kept). The raw string moves into the `original_value` column and the statement id (a content hash over the value) is regenerated:

```bash
cat statements.csv | ftmq statements cast-types > statements.typed.csv
```

The SQL backends `CAST` the `value` column when aggregating or sorting by a number, so stored values must be in this format. Statement stores apply the same casting on write; use `cast-types` to migrate data written outside ftmq.

Values that do not parse are logged and passed through unchanged; `--drop-invalid` drops them instead. Restrict the casting with `-t` / `--type` (`number`, `date`):

```bash
cat statements.csv | ftmq statements cast-types -t number --drop-invalid -o s3://data/statements.csv
```

### Statements in and out of a store

`read` dumps the statements of a store, `write` loads a statement stream into one. Together they let the nomenklatura resolver tooling, which works on statement streams, operate on a store - this is how a store is [resolved](./stores.md#merged-entities-resolver--linker) so that merged entities read back as one:

```bash
nomenklatura dump-resolver resolver.ijson
ftmq statements read -i duckdb://followthemoney.duckdb -o statements.csv
nomenklatura apply-statements -i statements.csv -o resolved.csv
ftmq statements write -i resolved.csv -o duckdb://followthemoney.duckdb
```

The last step updates the store in place: a statement's id is a hash over its dataset, entity id, property, value and language - not over `canonical_id` - so the resolved rows upsert onto the rows they came from. Nothing is duplicated and nothing is deleted, so this also works to add a dump to a store that already holds other data.

`write` preserves the `canonical_id` each statement carries and never re-derives it from a resolver. That is what keeps the `apply-statements` pass meaningful; it also means a stream that was never resolved loads unresolved, whatever the target store knows.

`read` yields the rows as stored - external statements included, in no particular order - so a dump loads back verbatim. It needs a SQL-family backend (sqlite, postgres, duckdb, lake); other stores raise. Restrict it to one dataset with `-d`:

```bash
ftmq statements read -i sqlite:///followthemoney.store -d my_dataset -o statements.csv
```

Both commands take `--input-format` / `--output-format` for their file side. Use `csv` or `json` for anything involving a resolver: the `pack` format has no `canonical_id` column at all, so it silently reads merged statements back as their referents (a warning is logged).

Two things are specific to the [delta lake store](./stores.md): it appends rather than upserting, so loading into one adds rows instead of updating them, and its `fragment` column has no place in any of the statement formats, so a dump does not carry it.
