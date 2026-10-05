FROM python:3.14-slim AS ftmq

RUN apt-get -qq update && apt-get -qq -y upgrade
RUN apt-get install -qq -y pkg-config libicu-dev build-essential
RUN apt-get -qq -y autoremove && apt-get clean

RUN pip install --no-cache-dir -U pip setuptools
RUN pip install --no-cache-dir -q --no-binary=:pyicu: pyicu

COPY ftmq /src/ftmq
COPY setup.py /src/setup.py
COPY README.md /src/README.md
COPY pyproject.toml /src/pyproject.toml
COPY VERSION /src/VERSION

WORKDIR /src

RUN pip install --no-cache-dir .

ENTRYPOINT ["ftmq"]

# docker build --target full: all extras (stores, search, api)
FROM ftmq AS full

RUN apt-get -qq update && apt-get install -qq -y libleveldb-dev libpq5 && apt-get clean
RUN pip install --no-cache-dir ".[level,sql,postgres,duckdb,lake,aleph,search,api]" redis

# the default target: plain ftmq
FROM ftmq
