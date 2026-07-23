# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ftmq is a Python library for querying and filtering [Follow The Money](https://followthemoney.tech) (FTM) entities. It provides:
- A composable `Query` language (`M` / `P` / `G` / `C` nodes composed with `&` / `|` / `~`, plus `A` aggregation projections) for filtering entities by meta fields, properties, property-type groups and context/storage columns
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
- **`G(**groups)`** - a followthemoney property-type group (the `prop_type` column, keyed by `registry.groups`: `names`, `dates`, `countries`, `entities`, ...). `G(entities=<id>)` is the reverse lookup (replaces the old `reverse`); `P(<edgeProp>=<id>)` is the narrow form.
- **`C(**context)`** - a context / storage column: `origin` plus backend-specific columns (`fragment`, `first_seen`, `bucket`, ...). In-memory it reads `entity.context[key]`; in SQL it maps to the same-named statement-table column (an unknown column raises `QueryError` at compile time).

Nodes compose with `&`, `|`, `~` into arbitrary boolean trees. `Query.where(*nodes)` AND-combines positional nodes; chained `.where()` also ANDs. Lookups are `field__comparator=value` (comparators defined in `ftmq/enums.py`). Invalid queries raise `QueryError` (subclass of `ValueError`).

Aggregations are a projection, not a filter: the **`A`** node (`A(sum="amountEur", by="beneficiary")`; functions `min` / `max` / `sum` / `avg` / `count` over any property plus the fields `id` / `dataset` / `schema` / `year`) does not compose with `& | ~` and is passed to `Query.aggregate()`, parallel to `where()`. In-memory they collect during `apply_iter` into `q.aggregator.result` (store views: `view.aggregations(q)`); the SQL backend reads the same `Agg` specs. Do not confuse with `ftmq/aggregate.py`, the schema-downgrading entity merge behind the `ftmq aggregate` CLI subcommand.

```python
from ftmq import Query, M, P, G, A

q = Query().where(M(schema="Person"), P(name__ilike="jane%"))
q = q.where(G(countries="de") | G(countries="at"))
q = q.order_by("name")[:10]
q = q.aggregate(A(count="id", by="dataset"))
```

Package layout: `nodes.py` (`Expr` tree + `M`/`P`/`G`/`C` + `combine`), `leaves.py` (leaf classes + factories + `LeafDict`), `aggregations.py` (`A` + `Agg` specs + in-memory `Aggregator`), `aleph.py` (Aleph URL-param bridge), `rql.py` (RQL string bridge), `sql.py` (`Sql` + `SqlSource`, see [SQL integration](#sql-integration-ftmqquerysqlpy)), `main.py` (`Query` + `Sort`), `exceptions.py` (`QueryError`). `__init__.py` only re-exports.

`Query` is the canonical query IR with four serialization surfaces:
- `to_dict()` / `from_dict()` - lossless nested tree (any tree, plus aggregations / sort / slice).
- `to_rql()` / `from_rql()` - [RQL](https://github.com/pjwerneck/pyrql) string (via `pyrql`): the only string surface carrying arbitrary `& | ~` nesting, plus aggregations via RQL's `sum` / `mean` / `count` / `aggregate(...)` operators; raises `QueryError` for comparators with no RQL equivalent (`null`, `startswith`, ...).
- `to_params()` / `from_params()` - Aleph `filter:`/`exclude:`/`empty:` MultiDict (the flat subset; raises `QueryError` for cross-field OR / negated groups); also carries aggregations (`metric:<func>` / `facet`) and `sort` / `limit` / `offset`.
- `to_string()` / `from_string()` - Aleph URL query string.

The Aleph bridge maps `M`→`filter:schema|schemata|dataset|...`, `P`→`filter:properties.<name>`, `G`→`filter:<group>`, `C`→`filter:origin`, `~`→`exclude:`, `__null`→`empty:`, aggregations→`metric:<func>=<prop>` + `facet=<field>`. Goal: bidirectional interop with openaleph-search's `SearchQueryParser` (Aleph params can query ftmq stores and vice versa).

### Query language refactor status

The rewrite from the legacy hand-made `.where(**kwargs)` DSL to the `M`/`P`/`G`/`C` grammar is complete (**no backward compatibility**, major-version break): the grammar + `Expr` tree, the in-memory evaluator, all four serialization surfaces, the `A` aggregation rewrite, the `Sql`/`SqlSource` adapter, and all consumers (CLI, SQL/Lake stores, docs, tests) are migrated; the legacy `filters.py` leaf layer is removed. The full test suite passes.

Known gaps:
- The SQL translation compiles only flat conjunctions: `Sql` reads the query's flat leaf collectors (OR within a field, AND across fields), so cross-field `OR` and negated groups evaluate in-memory only (see the warning in `docs/query.md`).
- `ftmq/query/` is `mypy --strict` clean except the moved `sql.py`; `make typecheck` (strict over the whole package) still fails on the legacy modules (CLI, stores, model, util).
- The `smart_read_proxies` docstring in `ftmq/io.py` still shows the removed kwargs API.

### Store Backends (`ftmq/store/`)

All stores inherit from `ftmq/store/base.py:Store` which extends nomenklatura's store interface:

- **memory**: In-memory store for testing
- **level**: LevelDB backend (requires `plyvel`)
- **redis**: Redis/Kvrocks backend (requires `redis`)
- **sql**: SQLAlchemy-based (SQLite, PostgreSQL) with SQL query optimization
- **aleph**: Aleph API backend (requires `alephclient`)
- **lake**: Delta Lake parquet-based store using DuckDB for queries (requires `[lake]` extras)
- **fragments**: Entity fragment store for incremental processing

Backend selection is automatic via URI scheme in `get_store()`:
```python
get_store("memory://")
get_store("leveldb:///path")
get_store("redis://localhost")
get_store("sqlite:///data.db")
get_store("lake+s3://bucket/path")
```

### SQL Integration (`ftmq/query/sql.py`)

`Sql` translates a `Query` into SQLAlchemy clauses; `SqlSource` describes what it compiles against (table, id column, optional partition pruning) and replaces the old `query.table` mutation. Access via `query.sql` (default nomenklatura statement table) or `query.compile(source)`; the SQL and Lake stores own their `SqlSource` (`SQLStore.source`; the Lake store folds `bucket` partition pruning into every compiled query). Flat conjunctions only - see [refactor status](#query-language-refactor-status).

### Search (`ftmq/search/`)

Full-text "shallow search" stores for FtM entities (formerly the standalone `ftmq-search` package). `logic.transform` flattens entities into `EntityDocument` search docs (names, rigour-based fingerprints via `ftmq.util.entity_fingerprints`, countries, dates, text blob); stores index those docs and answer `search(q, query=None)` / `autocomplete(q)`, where the optional `Query` filters by `dataset_names` / `schemata_names` / `countries`. Backends via `ftmq.search.store.get_store(uri=...)`: `sqlite://` (FTS5, no extra deps) and `tantivy://` / `memory://` (require the `search` extra, i.e. `tantivy`; lazily imported in the factory). Settings use the `FTMQ_SEARCH_` env prefix. The CLI lives in `ftmq/search/cli.py` as a typer sub-app (`ftmq search ...`); a bare query routes to its `search` command via `SearchDefaultGroup`.

### API (`ftmq/api/`)

Read-only FastAPI over a statement store + the search index (formerly the standalone `ftmq-api` package; requires the `api` extra: fastapi, furl, granian). Request flow: `app.py` (routes) -> `views.py` (controllers, anycache keyed by request url) -> `query.py` (params) + `store.py` (catalog/store/View access, all `functools.cache`d) -> `serialize.py` (yente-style response models). The http filter dialect IS the Aleph grammar: `query.build_query` feeds the request params straight into the upstream `Query.from_params`, then clamps `limit` to `settings.default_limit` unless `?api_key=` matches (`auth = higher limit, not access control`) and validates datasets against the catalog. Boot-time quirk: `store.py` fetches the catalog at import and freezes `Datasets = Literal[tuple(catalog.names)]` for path-param validation; catalog or data changes need a process restart. Settings env prefix `FTMQ_API_`; search store via `FTMQ_SEARCH_URI`. The base package must stay importable without fastapi: nothing outside `ftmq/api/` imports it, and `ftmq/api/__init__.py` has no imports. Run via `granian --interface asgi ftmq.api.app:app` (no CLI runner).

### CLI (`ftmq/cli.py`)

Built on typer (same conventions as anystore: module-level `Settings()`, `@cli.callback` with `--version`, `with ErrorHandler():` command bodies, `Annotated[..., typer.Option(...)]` params). `DefaultCmdTyperGroup` (a `TyperGroup` subclass) routes unknown or absent subcommands to `q`, replacing the former click-default-group dependency; `typer_cli = get_group(cli)` at module end feeds mkdocs-click. typer vendors click, so ftmq has no direct click dependency.

Entry point is `ftmq`. Default command is `q` for filtering:
```bash
cat entities.ftm.json | ftmq -s Company -p country=de -o output.json
```

Filter flags mirror the query families as repeatable `field[__op]=value` arguments: `-m/--meta`, `-p/--prop`, `-g/--group`, `-c/--context`; `-d` (dataset) and `-s` (schema) are shortcuts, with `--schema-include-descendants` / `--schema-include-matchable` switching `-s` to the is-a `schemata` field. Whole query strings: `-q` (Aleph filter params) and `--rql`. Aggregations: `--sum` / `--min` / `--max` / `--avg` / `--count` plus `--groups`, written to `--aggregation-uri`.

Subcommands: `dataset`, `catalog`, `store`, `fragments`, `search`, `aggregate`, `apply-dataset`. The default-command plumbing (`DefaultCmdTyperGroup`) lives in `ftmq/cli_util.py` so the search sub-app can subclass it without an import cycle.

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
