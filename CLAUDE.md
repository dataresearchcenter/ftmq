# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ftmq is a Python library for querying and filtering [Follow The Money](https://followthemoney.tech) (FTM) entities. It provides:
- A composable `Query` language (`M` / `P` / `G` / `C` nodes composed with `&` / `|` / `~`, plus `A` aggregation projections over the same families used as bare field references) for filtering entities by meta fields, properties, property-type groups and context/storage columns
- Smart I/O helpers for reading/writing FTM entities from various sources (files, S3, databases)
- Multiple storage backends via nomenklatura stores (memory, LevelDB, Redis, SQL, Aleph, Delta Lake)
- CLI for piping and filtering FTM JSON streams

## Behaviour rules for code agents

1. Don’t assume. Don’t hide confusion. Surface tradeoffs.

2. Minimum code that solves the problem. Nothing speculative.

3. Touch only what you must. Clean up only your own mess.

4. Define success criteria. Loop until verified.

## Environment

Use the virtualenv at `.venv` for all commands (e.g. `.venv/bin/pytest`, `.venv/bin/python`).

## Common Commands

```bash
# Install with all extras
make install

# Run all tests with coverage
make test

# Run a single test file
.venv/bin/pytest tests/test_query.py -v

# Run a specific test
.venv/bin/pytest tests/test_query.py::test_apply_meta -v

# Lint
make lint

# Type checking
make typecheck

# Pre-commit hooks
make pre-commit
```

## Architecture

### Core Components

**Query (`ftmq/query/`)**: The central query language (see [Query language](#query-language-ftmqquery) below). Chainable `.where()`, `.order_by()`, `.aggregate()`; slicing `q[10:20]`; applied via `q.apply(entity)` / `q.apply_iter(entities)`, or compiled to SQL via `q.sql` / `q.compile(source)`.

**I/O (`ftmq/io.py`)**: `smart_read_proxies()` and `smart_write_proxies()` auto-detect source type (file, URL, store URI) and handle streaming.

### Query language (`ftmq/query/`)

`Query` is built from four composable node constructors, split by the statement-table column they target:

- **`M(**meta)`** - meta fields: `dataset`, `schema` (exact match), `schemata` (is-a: entity *is-a* X, i.e. `model[X] in entity.schema.schemata`), `id` / `entity_id` / `canonical_id`.
- **`P(**props)`** - a specific FtM property (the `prop` column), e.g. `P(name="Jane")`, `P(amountEur__gte=1000)`.
- **`G(**groups)`** - a followthemoney property-type group (the `prop_type` column, keyed by `registry.groups`: `names`, `dates`, `countries`, `entities`, ...). `G(entities=<id>)` is the reverse lookup; `P(<edgeProp>=<id>)` is the narrow form.
- **`C(**context)`** - a context / storage column: `origin` plus backend-specific columns (`fragment`, `first_seen`, `bucket`, ...). In-memory it reads `entity.context[key]`; in SQL it maps to the same-named statement-table column (an unknown column raises `QueryError` at compile time).

Nodes compose with `&`, `|`, `~` into arbitrary boolean trees. `Query.where(*nodes)` AND-combines positional nodes; chained `.where()` also ANDs. Lookups are `field__comparator=value` (the comparator set lives in `ftmq/query/leaves.py:COMPARATORS`; in-memory semantics in `Leaf.match`, SQL translation in `Sql.get_expression`). Sorting is single-field (`order_by("name")` / `order_by("-date")`). Invalid queries raise `QueryError` (subclass of `ValueError`).

Aggregations are a projection, not a filter: the **`A`** node (`A(sum=P("amountEur"), by=P("beneficiary"))`; functions `min` / `max` / `sum` / `avg` / `count`) does not compose with `& | ~` and is passed to `Query.aggregate()`, parallel to `where()`. A field is addressed by a **`Ref`** - the same `M` / `P` / `G` / `C` marker called with a bare field name instead of `field=value` (`P("amountEur")`, `G("countries")`, `M("dataset")`, `C("origin")`), plus `Year()` for the date-derived year dimension; bare strings are rejected (`topics` is both a property and a group, `id` both the entity id and a `prop` whose values are referent ids). `min` / `max` / `sum` / `avg` over a numeric property read the value as a number in both evaluators (in memory through `registry.number.to_number`, in SQL as an error-tolerant cast over the canonical format written by `ftmq/statements.py` - `numeric_value` / `NumericValue` in `ftmq/query/sql.py`, which renders `TRY_CAST` by default and guards the cast on postgres / sqlite, so a value that isn't a number reads as `NULL` and drops out of the aggregate instead of failing it). In-memory they collect during `apply_iter` into `q.aggregator.result` (store views: `view.aggregations(q)`), keyed by each ref's wire spelling; the SQL backend reads the same `Agg` specs and compiles grouped aggregations to one select per grouper (`Sql.grouped_aggregations`, top-N capped). In `to_dict` aggregations serialize as a flat spec list (`[{"func", "field", "by"?}]`). Do not confuse with `ftmq/aggregate.py`, the schema-downgrading entity merge behind the `ftmq aggregate` CLI subcommand.

```python
from ftmq import Query, M, P, G, A

q = Query().where(M(schema="Person"), P(name__ilike="jane%"))
q = q.where(G(countries="de") | G(countries="at"))
q = q.order_by("name")[:10]
q = q.aggregate(A(count=M("id"), by=M("dataset")))
```

Package layout: `refs.py` (`Ref` hierarchy - one class per family - + `Year` + the `wire` / `ref_from_wire` field codec), `nodes.py` (`Expr` tree + `M`/`P`/`G`/`C` + `combine`), `leaves.py` (leaf classes + factories + `LeafDict`), `aggregations.py` (`A` + `Agg` specs + in-memory `Aggregator`), `aleph.py` (Aleph URL-param bridge), `rql.py` (RQL string bridge), `sql.py` (`Sql` + `SqlSource`, see [SQL integration](#sql-integration-ftmqquerysqlpy)), `main.py` (`Query` + `Sort`), `exceptions.py` (`QueryError`). `__init__.py` only re-exports.

A ref is "a leaf without a value", shared by both halves of the grammar: a `Ref` owns its family's name validation and its in-memory value access (`ref.values(entity)`), the filter leaves delegate to it (`RefLeaf`), and one field codec spells every field the same way on every string surface - `properties.<name>` for a property, `group.<name>` for a property-type group, `context.<name>` for a context column, bare for a meta field / `year` (`Ref.wire`, `ref_from_wire`, plus the upstream key aliases in `aleph.ALEPH_META`). `filter:group.countries=de` and `facet=group.countries` address one dimension. The SQL side dispatches on ref *type* (`Sql.lookup`, a `singledispatchmethod` returning a `Lookup(value, where)`).

`Query` is the canonical query IR with four serialization surfaces:
- `to_dict()` / `from_dict()` - lossless nested tree (any tree, plus aggregations / sort / slice).
- `to_rql()` / `from_rql()` - [RQL](https://github.com/pjwerneck/pyrql) string (via `pyrql`): the only string surface carrying arbitrary `& | ~` nesting, plus aggregations via RQL's `sum` / `mean` / `count` / `aggregate(...)` operators; raises `QueryError` for comparators with no RQL equivalent (`null`, `startswith`, ...).
- `to_params()` / `from_params()` - Aleph `filter:`/`exclude:`/`empty:` MultiDict (the flat subset; raises `QueryError` for cross-field OR / negated groups); also carries aggregations (`metric:<func>` / `facet`) and `sort` / `limit` / `offset`.
- `to_string()` / `from_string()` - Aleph URL query string.

The param bridge maps `M`→`filter:schema|schemata|dataset|...`, `P`→`filter:properties.<name>`, `G`→`filter:group.<name>`, `C`→`filter:context.<name>`, `~`→`exclude:`, `__null`→`empty:`, aggregations→`metric:<func>=<field>` + `facet=<field>` (fields spelled exactly as the filter keys are). The grammar is bidirectional: params can query ftmq stores, and a `Query` can drive an OpenAleph-style API.

This query IR and all four surfaces are mirrored in TypeScript in `js/query/`; a change here MUST be mirrored there (see the parity requirement under [JavaScript client](#javascript-client-js)).

### SQL compilation semantics

The SQL translation compiles arbitrary `& | ~` trees, matching the in-memory evaluator: a plain conjunction with at most one leaf per field uses the flat collectors (row predicates for meta / context columns, one entity-level clause per property / group field); any `OR`, negation or repeated field switches to `Sql._expr_clause`, which lifts every leaf to an entity-level `canonical_id IN (...)` predicate and composes them (chained same-field `.where()` calls AND, as in memory - alternatives are spelled `__in`). Whenever row and entity clauses mix (or a slice applies), the statement select routes through the `canonical_ids` sub-select, so whole canonical entities come back; only a purely row-level query (meta / context fields, no scope) filters statement rows directly. View scoping is compiled by `Sql` itself (an entity-level dataset-membership conjunct, nomenklatura view semantics: scope selects entities, assembly stays store-wide). Caveats are documented in the note in `docs/query.md`: SQLite `LIKE` collation and the row-level semantics of meta-only queries.

Typing status: `ftmq/query/` is `mypy --strict` clean except `sql.py`; `make typecheck` (strict over the whole package) still fails on the CLI, stores, model and util modules.

### Store Backends (`ftmq/store/`)

All stores inherit from `ftmq/store/base.py:Store` which extends nomenklatura's store interface:

- **memory**: In-memory store for testing
- **level**: LevelDB backend (requires `plyvel`)
- **redis**: Redis/Kvrocks backend (requires `redis`)
- **sql**: SQLAlchemy-based (SQLite, PostgreSQL) with SQL query optimization
- **duckdb**: the sql store against a duckdb database file (requires `duckdb-engine`); only the bulk upsert is duckdb-specific, everything else is `store/sql.py`
- **aleph**: Aleph API backend (requires `alephclient`)
- **lake**: Delta Lake parquet-based store using DuckDB for queries (requires `[lake]` extras)
- **fragments**: Entity fragment store for incremental processing

Backend selection is automatic via URI scheme in `get_store()`:
```python
get_store("memory://")
get_store("leveldb:///path")
get_store("redis://localhost")
get_store("sqlite:///data.db")
get_store("duckdb://data.duckdb")
get_store("lake+s3://bucket/path")
```

### Statement value types (`ftmq/statements.py`)

followthemoney's `number` and `date` types don't normalize on write - a statement keeps the string it was given (`"324,687.00"`, `"1.1.2021"`) and parses it on read. The in-memory evaluator does the same, but the SQL backends `CAST` the `value` column, so display-formatted values would read wrong (sqlite) or raise (postgres, duckdb). Instead of porting the parsers into every dialect, values are normalized once on write: `cast_types` puts the parsed value into `value` and the raw string into `original_value`, regenerating the statement id (a content hash over the value). Numbers go through `registry.number.parse` (the string form of what `to_number` reads, so a big value doesn't take a float round trip); dates through `registry.date.clean_text`. Values that don't parse are logged and passed through unchanged (`drop_invalid=True` drops them instead).

Every statement store applies this on write: `Store.writer()` reblesses the backend writer into a subclass hooking `add_statement` (`Store.casting_writer`, which a store building its own writer - the lake store - has to call itself); `get_store(..., cast_types=False)` opts out. Existing dumps are migrated with `ftmq statements cast-types`.

### SQL Integration (`ftmq/query/sql.py`)

`Sql` translates a `Query` into SQLAlchemy clauses; `SqlSource` describes what it compiles against (table, id column, optional partition pruning, optional row-level `base_filter`). Access via `query.sql` (default nomenklatura statement table), `query.compile(source)`, or `Sql(query, source, scope=...)` for a dataset-scoped view (what store views do). The SQL and Lake stores own their `SqlSource` (`SQLStore.source`; the Lake store folds `bucket` partition pruning - positive flat schema conjuncts only - and its view filter into every compiled query). Arbitrary boolean trees compile - see [SQL compilation semantics](#sql-compilation-semantics).

### Search (`ftmq/search/`)

Full-text "shallow search" stores for FtM entities. `logic.transform` flattens entities into `EntityDocument` search docs (names, rigour-based fingerprints via `ftmq.util.entity_fingerprints`, countries, dates, text blob); stores index those docs and answer `search(q, query=None)` / `autocomplete(q)`, where the optional `Query` filters by `dataset_names` / `schemata_names` / `countries`. Backends via `ftmq.search.store.get_store(uri=...)`: `sqlite://` (FTS5, no extra deps) and `tantivy://` / `memory://` (require the `search` extra, i.e. `tantivy`; lazily imported in the factory). Settings use the `FTMQ_SEARCH_` env prefix. The CLI lives in `ftmq/search/cli.py` as a typer sub-app (`ftmq search ...`); a bare query routes to its `search` command via `SearchDefaultGroup`.

### API (`ftmq/api/`)

Read-only FastAPI over a statement store + the search index (requires the `api` extra: fastapi, furl, granian). Request flow: `app.py` (routes) -> `views.py` (controllers, anycache keyed by request url) -> `query.py` (params) + `store.py` (catalog/store/View access, all `functools.cache`d) -> `serialize.py` (yente-style response models). The http filter dialect IS the Aleph grammar: `query.build_query` feeds the request params straight into the upstream `Query.from_params`, plus an optional `rql=` param (parsed via `Query.from_rql`) that overrides the flat filter tree for nested boolean queries; it then clamps `limit` to `settings.default_limit` unless `?api_key=` matches (`auth = higher limit, not access control`) and validates datasets against the catalog. The catalog is *reconciled with the store* at boot (`store.get_catalog`): datasets present in the store but absent from `settings.catalog` are appended as bare entries, so the store - not the catalog file - decides what is queryable. `/entities` returns the OpenAleph api v2 **envelope** (`serialize.EntitiesResponse`): `status`/`results`/`total`/`total_type`/`page`/`pages`/`limit`/`offset`/`next`/`previous`/`facets`/`metrics`/`filters`/`query_q`, plus additive ftmq extras `query` (canonical `to_dict`) and `stats`. `/entities` is the single query surface (mirroring Aleph): aggregations ride on the same query (no `/aggregate`) - grouped -> `facets` (Aleph value/count buckets), ungrouped -> `metrics`, `limit=0` returns them with empty `results`; a `q=<term>` param routes to full-text search via `ftmq.search` (no `/search` endpoint). Only `/autocomplete` remains separate. The JS api types (`js/api/types.ts` `IEntitiesResult`) mirror this envelope by hand (no automated cross-check, unlike the query parity fixtures). Boot-time quirk: `store.py` fetches the catalog at import and freezes `Datasets = Literal[tuple(catalog.names)]` for path-param validation; catalog or data changes need a process restart. Settings env prefix `FTMQ_API_`; search store via `FTMQ_SEARCH_URI`. The base package must stay importable without fastapi: nothing outside `ftmq/api/` imports it, and `ftmq/api/__init__.py` has no imports. Run via `granian --interface asgi ftmq.api.app:app` (no CLI runner).

### JavaScript client (`js/`)

The npm package `@dataresearchcenter/ftmq` (built from `js/`, entry `js/index.ts` -> `dist/`). `js/api/` is the api client (`Api` class + ftmq-specific dataset/catalog/stats types in `js/api/model.ts`). `js/query/` is a full TypeScript port of the `ftmq.query` serialization surfaces: `Query`, `M`/`P`/`G`/`C` (as conditions *and*, called with a bare field name, as `Ref`s), `A`, `Year`, `and`/`or`/`not`, with lossless round-trips across all four surfaces (`toDict`/`toParams`/`toString`/`toRql` and their `from*`), so a client app can parse a url back into a `Query`. It is **serialization-only**: no entity evaluation, no SQL, and no model/name validation (the server validates; invalid -> 400). The followthemoney model itself is not reimplemented - it comes from `@opensanctions/followthemoney`. RQL uses a self-contained pyrql-compatible codec (`js/query/rql.ts`). Build/test: `npm run build` / `npm test` (`tsc` then `node --test`).

**Parity requirement (Python <-> JS): `js/query/` is a strict mirror of `ftmq/query/` and MUST stay in lockstep.** Any change to the query grammar or a serialization surface - nodes, leaves, comparators, aggregations, the aleph bridge, or rql - has to be made in BOTH languages in the same change. Enforcement: `scripts/gen_query_fixtures.py` dumps every query surface from the Python `Query` to `js/tests/fixtures/query_cases.json`, and `js/tests/query.test.ts` rebuilds each fixture in TS and fails if any surface diverges. After any query-layer change (either language), regenerate fixtures (`.venv/bin/python scripts/gen_query_fixtures.py` or `npm run gen-fixtures`) and run `npm test`; extend the `CASES` in the generator when adding grammar. `toString`/`toParams` are asserted byte-identical cross-language (sorted keys); `toDict`/`toRql` are asserted parse-equal (order-independent, since Python orders children by `hash_data` and TS by JSON). Never land a query-layer change that leaves the two out of sync or the fixtures stale.

### CLI (`ftmq/cli/`)

Built on typer (same conventions as anystore: module-level `Settings()`, `@cli.callback` with `--version`, `with ErrorHandler():` command bodies, `Annotated[..., typer.Option(...)]` params). `DefaultCmdTyperGroup` (a `TyperGroup` subclass) routes unknown or absent subcommands to `q`; `typer_cli = get_group(cli)` at the end of `main.py` feeds mkdocs-click. typer vendors click, so ftmq has no direct click dependency.

One module per command group: `main.py` (the root app, its callback, the top-level `q` / `apply-dataset` / `aggregate` commands, and the `add_typer` wiring), `dataset.py` (`dataset` + `catalog`), `store.py`, `fragments.py`, `statements.py`; the `search` group lives with its package in `ftmq/search/cli.py`. Each module exposes its own `typer.Typer` app and does *not* import the root app, so wiring stays one-directional. `__init__.py` only re-exports `cli` / `typer_cli` (the console-script and mkdocs-click entry points).

Entry point is `ftmq`. Default command is `q` for filtering:
```bash
cat entities.ftm.json | ftmq -s Company -p country=de -o output.json
```

Filter flags mirror the query families as repeatable `field[__op]=value` arguments: `-m/--meta`, `-p/--prop`, `-g/--group`, `-c/--context`; `-d` (dataset) and `-s` (schema) are shortcuts, with `--schema-include-descendants` / `--schema-include-matchable` switching `-s` to the is-a `schemata` field. Whole query strings: `-q` (Aleph filter params) and `--rql`. Aggregations: `--sum` / `--min` / `--max` / `--avg` / `--count` plus `--groups`, written to `--aggregation-uri`; their fields take the wire spelling (`--sum properties.amountEur --groups group.countries`), resolved through `ref_from_wire`.

Subcommands: `dataset`, `catalog`, `store`, `fragments`, `search`, `statements`, `aggregate`, `apply-dataset`. The default-command plumbing (`DefaultCmdTyperGroup`) lives in `ftmq/cli_util.py` (not in the `ftmq.cli` package) so the search sub-app can subclass it without an import cycle.

## Key Dependencies

- `followthemoney`: FTM schema and entity types
- `nomenklatura`: Statement-based entity storage
- `anystore`: Cloud-agnostic file I/O (S3, GCS, local)
- `pydantic`: Data validation for models in `ftmq/model/`
- `pyrql`: RQL parsing for `Query.from_rql()` / `to_rql()`
- `typer`: CLI framework (vendors click; no direct click dependency)

## Testing

Tests use fixtures in `tests/fixtures/` (`eu_authorities.ftm.json`, `donations.ijson`). A local HTTP server is spawned for URL-based tests. Environment variables for tests are configured in `pyproject.toml` under `[tool.pytest_env]`.

## Conventions

- Never use em-dashes (`—`) in prose (docstrings, comments, docs, commit messages, PR text). Use a normal hyphen (`-`) or restructure the sentence.
- In `docs/` markdown prose, keep each paragraph on a single line (do not hard-wrap; one line per paragraph, separated by blank lines). Code docstrings and comments wrap normally.
- Import at module top level. Use a function-local (inline) import only to break a genuine circular dependency; a type-only import belongs under `if TYPE_CHECKING:` instead.
