# wheels for everything, built with the compilers the runtime images drop
FROM python:3.14-slim AS build

RUN apt-get -qq update \
    && apt-get install -qq -y --no-install-recommends \
        pkg-config libicu-dev libleveldb-dev build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY ftmq /src/ftmq
COPY setup.py README.md pyproject.toml VERSION /src/

WORKDIR /src

RUN pip wheel --no-cache-dir -q --wheel-dir /wheels --no-binary=:pyicu: pyicu .

FROM build AS build-full

RUN pip wheel --no-cache-dir -q --wheel-dir /wheels \
    ".[level,sql,postgres,duckdb,lake,search,api]" redis

FROM python:3.14-slim AS ftmq

RUN apt-get -qq update \
    && apt-get -qq -y upgrade \
    && apt-get install -qq -y --no-install-recommends '^libicu[0-9]+$' \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=bind,from=build,source=/wheels,target=/wheels \
    pip install --no-cache-dir -q --no-index --find-links /wheels pyicu ftmq

ENTRYPOINT ["ftmq"]

# docker build --target full: all extras (stores, search, api)
FROM ftmq AS full

RUN apt-get -qq update \
    && apt-get install -qq -y --no-install-recommends libleveldb1d libpq5 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=bind,from=build-full,source=/wheels,target=/wheels \
    pip install --no-cache-dir -q --no-index --find-links /wheels \
        "ftmq[level,sql,postgres,duckdb,lake,search,api]" redis

# the default target: plain ftmq
FROM ftmq
