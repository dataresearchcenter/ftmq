[![Docs](https://img.shields.io/badge/docs-live-brightgreen)](https://docs.investigraph.dev/lib/ftmq/)
[![ftmq on pypi](https://img.shields.io/pypi/v/ftmq)](https://pypi.org/project/ftmq/)
[![PyPI Downloads](https://static.pepy.tech/badge/ftmq/month)](https://pepy.tech/projects/ftmq)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/ftmq)](https://pypi.org/project/ftmq/)
[![Python test and package](https://github.com/dataresearchcenter/ftmq/actions/workflows/python.yml/badge.svg)](https://github.com/dataresearchcenter/ftmq/actions/workflows/python.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](https://coveralls.io/repos/github/dataresearchcenter/ftmq/badge.svg?branch=main)](https://coveralls.io/github/dataresearchcenter/ftmq?branch=main)
[![AGPLv3+ License](https://img.shields.io/pypi/l/ftmq)](./LICENSE)
[![Pydantic v2](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pydantic/pydantic/main/docs/badge/v2.json)](https://pydantic.dev)

# ftmq

`ftmq` queries and filters [Follow The Money](https://followthemoney.tech) entities from json files / streams or from a statement-based [nomenklatura](https://github.com/opensanctions/nomenklatura) store.

Its `ftmq.Query` is the backend-agnostic query object of the [OpenAleph](https://openaleph.org) ecosystem: it filters FtM streams and stores, compiles to SQL, and converts to and from OpenAleph API params.

New to Follow The Money? See [this introduction](https://pad.investigativedata.org/s/0qKuBEcsM#).

## Installation

Minimum Python version: 3.11

    pip install ftmq

## Usage

### Command line

```bash
cat entities.ftm.json | ftmq -q 'filter:schema=Company&filter:properties.country=de&filter:gte:properties.incorporationDate=2023' -o s3://data/entities-filtered.ftm.json
```

### Python Library

```python
from ftmq import Query, M, P, G, smart_read_proxies

# legal entities in `companies` based in Germany, or in Austria and incorporated
# since 2020, not dissolved: the 5 most recently incorporated
q = Query().where(
    M(dataset="companies"),
    M(schemata="LegalEntity"),
    G(countries="de") | (G(countries="at") & P(incorporationDate__gte=2020)),
    ~P(status__ilike="%dissolved%"),
).order_by(P("incorporationDate"), ascending=False)[:5]

for proxy in smart_read_proxies("s3://data/entities.ftm.json", query=q):
    print(proxy.caption)
```

### Full-text search

`ftmq.search` indexes entities into full-text search stores backed by SQLite FTS5 or [Tantivy](https://github.com/quickwit-oss/tantivy) (`pip install ftmq[search]`):

```bash
cat entities.ftm.json | ftmq search transform | ftmq search --uri sqlite:///ftmqs.db index
ftmq search --uri sqlite:///ftmqs.db "jane doe"
```

### HTTP API

`ftmq.api` serves a store and its search index as a read-only FastAPI application speaking the Aleph filter grammar (`pip install ftmq[api]`):

```bash
granian --interface asgi ftmq.api.app:app
curl "localhost:8000/entities?filter:schema=Payment&filter:gte:properties.date=2023"
```

## Documentation

https://docs.investigraph.dev/lib/ftmq

## Support

This project is part of [investigraph](https://investigraph.dev)

In 2023, development of `ftmq` was supported by [Media Tech Lab Bayern batch #3](https://github.com/media-tech-lab)

<a href="https://www.media-lab.de/en/programs/media-tech-lab">
    <img src="https://raw.githubusercontent.com/media-tech-lab/.github/main/assets/mtl-powered-by.png" width="240" title="Media Tech Lab powered by logo">
</a>

## License and Copyright

`ftmq`, (C) 2023 Simon Wörpel
`ftmq`, (C) 2024-2025 investigativedata.io
`ftmq`, (C) 2025 [Data and Research Center – DARC](https://dataresearchcenter.org)

`ftmq` is licensed under the AGPLv3 or later license.

Prior to version 0.8.0, `ftmq` was released under the MIT license.

see [NOTICE](./NOTICE) and [LICENSE](./LICENSE)
