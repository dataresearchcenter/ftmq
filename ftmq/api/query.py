from collections import defaultdict

from fastapi import HTTPException, Request
from pydantic import BaseModel

from ftmq.api.settings import Settings
from ftmq.api.store import get_catalog
from ftmq.query import Query

settings = Settings()


class RetrieveParams(BaseModel):
    nested: bool
    featured: bool
    dehydrate: bool
    dehydrate_nested: bool
    stats: bool


def params_from_request(request: Request) -> dict[str, list[str]]:
    """Collect the request query params as a dict of lists (starlette's
    `QueryParams.items()` drops repeated keys)."""
    params: dict[str, list[str]] = defaultdict(list)
    for key, value in request.query_params.multi_items():
        params[key].append(value)
    return dict(params)


def build_query(request: Request, authenticated: bool | None = False) -> Query:
    """Build a [`Query`][ftmq.Query] from a request's query params.

    The flat filter grammar is the Aleph one (`filter:` / `exclude:` /
    `empty:`, `sort`, `limit` / `offset`, `metric:<func>` / `facet`) parsed via
    [`Query.from_params`][ftmq.Query.from_params]. An optional `rql=` param
    carries a full nested [RQL][ftmq.Query.from_rql] filter tree (and, if
    present, its aggregations); it overrides the flat filter grammar while
    `sort` / `limit` / `offset` keep coming from the plain params.

    Non-query params (`q`, `api_key`, retrieve flags) are ignored by the
    parser. The limit is capped to `settings.default_limit` unless the request
    is authenticated; datasets are validated against the catalog.

    Raises:
        HTTPException: 422 for a dataset not in the catalog.
        QueryError: For invalid filter fields, values or RQL (handled as 400
            upstream).
    """
    params = params_from_request(request)
    q = Query.from_params(params)
    rql = params.get("rql")
    if rql:
        rql_q = Query.from_rql(rql[0])
        q = q._chain(q=rql_q.q, aggregations=rql_q.aggregations or q.aggregations)
    limit = q.limit if q.limit is not None else settings.default_limit
    if not authenticated:
        limit = min(limit, settings.default_limit)
    offset = q.offset or 0
    q = q[offset : offset + limit]
    invalid = q.dataset_names - get_catalog().names
    if invalid:
        raise HTTPException(422, detail=[f"Invalid dataset: `{', '.join(invalid)}`"])
    return q
