# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ftmq is a Python library for querying and filtering [Follow The Money](https://followthemoney.tech) (FTM) entities. It provides:
- A composable `Query` language (`M` / `P` / `G` nodes composed with `&` / `|` / `~`) for filtering entities by meta fields, properties, and property-type groups
- Smart I/O helpers for reading/writing FTM entities from various sources (files, S3, databases)
- Multiple storage backends via nomenklatura stores (memory, LevelDB, Redis, SQL, Aleph, Delta Lake)
- CLI for piping and filtering FTM JSON streams

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

**Query (`ftmq/query/`)**: The central query language (see [Query language](#query-language-ftmqquery) below). Chainable `.where()`, `.order_by()`, `.aggregate()`; slicing `q[10:20]`; applied via `q.apply(entity)` / `q.apply_iter(entities)`.

**I/O (`ftmq/io.py`)**: `smart_read_proxies()` and `smart_write_proxies()` auto-detect source type (file, URL, store URI) and handle streaming.

### Query language (`ftmq/query/`)

`Query` is built from three composable node constructors, split by the statement-table column they target:

- **`M(**meta)`** - meta fields: `dataset`, `schema` (exact match), `schemata` (is-a: entity *is-a* X, i.e. `model[X] in entity.schema.schemata`), `origin`, `id` / `entity_id` / `canonical_id`.
- **`P(**props)`** - a specific FtM property (the `prop` column), e.g. `P(name="Jane")`, `P(amountEur__gte=1000)`.
- **`G(**groups)`** - a followthemoney property-type group (the `prop_type` column, keyed by `registry.groups`: `names`, `dates`, `countries`, `entities`, ...). `G(entities=<id>)` is the reverse lookup (replaces the old `reverse`); `P(<edgeProp>=<id>)` is the narrow form.

Nodes compose with `&`, `|`, `~` into arbitrary boolean trees. `Query.where(*nodes)` AND-combines positional nodes; chained `.where()` also ANDs. Lookups are `field__comparator=value` (comparators defined in `ftmq/enums.py`). Invalid queries raise `QueryError` (subclass of `ValueError`).

```python
from ftmq import Query, M, P, G

q = Query().where(M(schema="Person"), P(name__ilike="jane%"))
q = q.where(G(countries="de") | G(countries="at"))
q = q.order_by("name")[:10]
```

Package layout: `nodes.py` (`Expr` tree + `M`/`P`/`G` + `combine`), `leaves.py` (leaf classes + factories + `LeafDict`), `aleph.py` (Aleph URL-param bridge), `main.py` (`Query` + `Sort`), `exceptions.py` (`QueryError`), `filters.py` (legacy leaf layer, reused by import). `__init__.py` only re-exports.

`Query` is the canonical query IR with three serialization surfaces:
- `to_dict()` / `from_dict()` - lossless nested tree (any tree).
- `to_params()` / `from_params()` - Aleph `filter:`/`exclude:`/`empty:` MultiDict (the flat subset; raises `QueryError` for cross-field OR / negated groups).
- `to_string()` / `from_string()` - Aleph URL query string.

The Aleph bridge maps `M`→`filter:schema|schemata|dataset|...`, `P`→`filter:properties.<name>`, `G`→`filter:<group>`, `~`→`exclude:`, `__null`→`empty:`. Goal: bidirectional interop with openaleph-search's `SearchQueryParser` (Aleph params can query ftmq stores and vice versa).

### Query language refactor status

The query language is mid-refactor from the legacy hand-made `.where(**kwargs)` DSL to the `M`/`P`/`G` grammar. **No backward compatibility** (major-version break).

- **Done (phase 1)**: the grammar + `Expr` tree, the in-memory evaluator (`apply`/`apply_iter`, correct AND/OR/NOT, is-a `schemata`, correct `null`), all three serialization surfaces, and the `ftmq/query/` package. Covered by `tests/test_query.py`; the new modules are `mypy --strict` clean (the moved legacy `filters.py` is not).
- **Pending (phase 2)**: `ftmq/sql.py` still consumes the query's tree-walking collectors and is only correct for flat `is_and_only()` queries (no OR/NOT). Consumers still on the removed kwargs API and failing until migrated: `ftmq/cli.py`, the `ftmq/io.py` docstring, the SQL/Lake stores, and `tests/test_proxy.py` / `tests/test_sql.py` / `tests/test_store.py` / `tests/test_cli.py`. Aggregations (`ftmq/aggregations.py`, `Query.aggregate`) are unchanged.

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

### SQL Integration (`ftmq/sql.py`)

The `Sql` class translates Query filters into SQLAlchemy clauses for SQL and Lake stores. Accessed via `query.sql`. **Not yet updated for the `M`/`P`/`G` refactor** (phase 2): it reads the query's tree-walking collectors and is only correct for flat `is_and_only()` queries. See [refactor status](#query-language-refactor-status).

### CLI (`ftmq/cli.py`)

Entry point is `ftmq`. Default command is `q` for filtering:
```bash
cat entities.ftm.json | ftmq -s Company --country=de -o output.json
```

Subcommands: `dataset`, `catalog`, `store`, `fragments`, `aggregate`, `apply-dataset`

## Key Dependencies

- `followthemoney`: FTM schema and entity types
- `nomenklatura`: Statement-based entity storage
- `anystore`: Cloud-agnostic file I/O (S3, GCS, local)
- `pydantic`: Data validation for models in `ftmq/model/`

## Testing

Tests use fixtures in `tests/fixtures/` (`eu_authorities.ftm.json`, `donations.ijson`). A local HTTP server is spawned for URL-based tests. Environment variables for tests are configured in `pyproject.toml` under `[tool.pytest_env]`.

## Conventions

- Never use em-dashes (`—`) in prose (docstrings, comments, docs, commit messages, PR text). Use a normal hyphen (`-`) or restructure the sentence.
