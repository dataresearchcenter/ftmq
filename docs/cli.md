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

The CLI mirrors the [`Query` language](./query.md): the same `M` / `P` / `G` / `C` families, expressed either as typed flags or as an Aleph filter string.

Filter for a dataset (a `-d` shortcut for `-m dataset=`):

```bash
cat entities.ftm.json | ftmq -d ec_meetings
```

Filter for a schema (`-s` shortcut for `-m schema=`):

```bash
cat entities.ftm.json | ftmq -s Person
```

Filter for a schema and all its descendants (the is-a `schemata` field):

```bash
cat entities.ftm.json | ftmq -s LegalEntity --schema-include-descendants
```

### Family flags

Each family has a repeatable flag taking a `field[__comparator]=value` argument:

- `-m` / `--meta` - meta fields: `dataset`, `schema`, `schemata`, `id`, `entity_id`, `canonical_id`
- `-p` / `--prop` - a specific [property](https://followthemoney.tech/explorer/)
- `-g` / `--group` - a property-type group (`names`, `dates`, `countries`, `entities`, ...)
- `-c` / `--context` - a context / provenance field (`origin`, ...)

```bash
# companies based in Germany (the literal `country` property)
cat entities.ftm.json | ftmq -s Company -p country=de

# any country-typed property equal to `de` (the `countries` group)
cat entities.ftm.json | ftmq -g countries=de

# by origin (context) and entity id prefix (meta)
cat entities.ftm.json | ftmq -c origin=crawl -m id__startswith=de-

# reverse lookup: entities pointing at an id (the `entities` group)
cat entities.ftm.json | ftmq -g entities=some-entity-id
```

### Comparison lookups

Any flag value may carry a `__<comparator>` suffix; `__in` / `__not_in` accept a comma-separated list:

```bash
cat entities.ftm.json | ftmq -s Company -p incorporationDate__gte=2020 -p address__ilike=berlin
cat entities.ftm.json | ftmq -p firstName__in=Jane,Joe
```

Possible lookups:

- `gt` / `lt` / `gte` / `lte` - greater / lower (than or equal)
- `like` / `ilike` - SQLish `LIKE` / case-insensitive `ILIKE` (use `%` placeholders)
- `in` / `not_in` - value (not) in a comma-separated list
- `not` - not equal
- `null` - test for presence (`__null=true` / `__null=false`)

### Aleph filter string

Alternatively pass a full [Aleph](https://openaleph.org) filter string via `-q` / `--query` (parsed by [`Query.from_string`][ftmq.Query.from_string]); repeatable and combinable with the family flags:

```bash
cat entities.ftm.json | ftmq -q 'filter:schema=Person&filter:countries=de'
cat entities.ftm.json | ftmq -q 'filter:properties.name=Jane&exclude:properties.country=ru'
cat entities.ftm.json | ftmq -q 'filter:gte:properties.date=2020&empty:properties.deathDate'
```

The Aleph string is flat (no cross-field `OR`). For a **nested** filter tree, pass an [RQL](https://github.com/pjwerneck/pyrql) string via `--rql` (parsed by [`Query.from_rql`][ftmq.Query.from_rql]):

```bash
# schema=Person AND (countries=de OR countries=at)
cat entities.ftm.json | ftmq --rql 'and(eq(schema,Person),or(eq(countries,de),eq(countries,at)))'
# NOT Organization, with a name in a list
cat entities.ftm.json | ftmq --rql 'and(not(eq(schema,Organization)),in(name,(jane,joe)))'
```
