[![ftmq on pypi](https://img.shields.io/pypi/v/ftmq)](https://pypi.org/project/ftmq/)
[![Python test and package](https://github.com/investigativedata/ftmq/actions/workflows/python.yml/badge.svg)](https://github.com/investigativedata/ftmq/actions/workflows/python.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](https://coveralls.io/repos/github/investigativedata/ftmq/badge.svg?branch=main)](https://coveralls.io/github/investigativedata/ftmq?branch=main)
[![AGPLv3+ License](https://img.shields.io/pypi/l/ftmq)](./LICENSE)

# ftmq

This library provides methods to query and filter entities formatted as [Follow The Money](https://followthemoney.tech) data, either from a json file/stream or using a statement-based store backend from [nomenklatura](https://github.com/opensanctions/nomenklatura).

It also provides a `Query` class that can be used in other libraries to work with SQL store queries or api queries.

`ftmq` is the base layer for [investigativedata.io's](https://investigativedata.io) libraries and applications dealing with [Follow The Money](https://followthemoney.tech) data.

## Installation

Minimum Python version: 3.11

    pip install ftmq

## Usage

### Command line

```bash
cat entities.ftm.json | ftmq -s Company --country=de --incorporationDate__gte=2023 -o s3://data/entities-filtered.ftm.json
```

### Python Library

```python
from ftmq import Query, smart_read_proxies

q = Query() \
    .where(dataset="ec_meetings", date__lte=2020) \
    .where(schema="Event") \
    .order_by("date", ascending=False)

for proxy in smart_read_proxies("s3://data/entities.ftm.json"):
    if q.apply(proxy):
        yield proxy
```

## Support

This project is part of [investigraph](https://investigraph.dev)

In 2023, development of `ftmq` was supported by [Media Tech Lab Bayern batch #3](https://github.com/media-tech-lab)

<a href="https://www.media-lab.de/en/programs/media-tech-lab">
    <img src="https://raw.githubusercontent.com/media-tech-lab/.github/main/assets/mtl-powered-by.png" width="240" title="Media Tech Lab powered by logo">
</a>


## License and Copyright

`ftmq`, (C) 2023 Simon Wörpel
`ftmq`, (C) 2024-2025 investigativedata.io

`ftmq` is licensed under the AGPLv3 or later license.

Prior to version 0.8.0, `ftmq` was released under the MIT license.

see [NOTICE](./NOTICE) and [LICENSE](./LICENSE)
