# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ftmq is a Python library for querying and filtering [Follow The Money](https://followthemoney.tech) (FTM) entities. It provides:
- A `Query` DSL for filtering entities by schema, dataset, properties, and comparators
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
.venv/bin/pytest tests/test_query.py::test_query_lookups -v

# Lint
make lint

# Type checking
make typecheck

# Pre-commit hooks
make pre-commit
```

## Architecture

### Core Components

**Query (`ftmq/query.py`)**: The central DSL class. Chainable methods `.where()`, `.order_by()`, `.aggregate()` build filter pipelines. Supports slicing syntax `q[10:20]` for pagination. Filters are applied via `q.apply(entity)` or `q.apply_iter(entities)`.

**Filters (`ftmq/filters.py`)**: Filter classes for each lookup type:
- `DatasetFilter`, `SchemaFilter`, `PropertyFilter`, `ReverseFilter`, `IdFilter`, `OriginFilter`
- Comparators: `eq`, `in`, `not`, `not_in`, `gt`, `gte`, `lt`, `lte`, `like`, `ilike`, `startswith`, `endswith`, `null`

**I/O (`ftmq/io.py`)**: `smart_read_proxies()` and `smart_write_proxies()` auto-detect source type (file, URL, store URI) and handle streaming.

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

The `Sql` class translates Query filters into SQLAlchemy clauses for SQL and Lake stores. Accessed via `query.sql`.

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
