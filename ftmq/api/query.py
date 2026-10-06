from collections import defaultdict
from dataclasses import dataclass
from typing import Annotated

from fastapi import HTTPException
from fastapi import Query as QueryField
from fastapi import Request

from ftmq.api.settings import settings
from ftmq.api.store import get_catalog
from ftmq.query import Query


@dataclass(frozen=True)
class RetrieveParams:
    """Response-shaping query params (a FastAPI `Depends()` dependency)."""

    nested: Annotated[
        bool, QueryField(description="Inline adjacent entities instead of their ids")
    ] = False
    featured: Annotated[
        bool, QueryField(description="Only include featured properties and caption")
    ] = False
    dehydrate: Annotated[
        bool, QueryField(description="Only include id, schema and caption")
    ] = False
    dehydrate_nested: Annotated[
        bool, QueryField(description="Dehydrate nested entities")
    ] = True
    stats: Annotated[bool, QueryField(description="Include statistics in response")] = (
        False
    )


def params_from_request(request: Request) -> dict[str, list[str]]:
    """The request query params as a dict of lists, keeping repeated keys."""
    params: dict[str, list[str]] = defaultdict(list)
    for key, value in request.query_params.multi_items():
        params[key].append(value)
    return dict(params)


def build_query(request: Request, authenticated: bool | None = False) -> Query:
    """Build a [`Query`][ftmq.Query] from a request's query params.

    The params are parsed via [`Query.from_params`][ftmq.Query.from_params]; an
    optional `rql=` param ([`Query.from_rql`][ftmq.Query.from_rql]) replaces the
    filter tree (and the aggregations / projection, if it carries any). Unless
    authenticated, the limit is capped to `settings.default_limit` and each facet
    size to `settings.max_facet_size`.

    Raises:
        HTTPException: 422 for a dataset not in the catalog.
        QueryError: For an invalid field, value or RQL string.
    """
    params = params_from_request(request)
    q = Query.from_params(params)
    rql = params.get("rql")
    if rql:
        rql_q = Query.from_rql(rql[0])
        q = q._chain(
            q=rql_q.q,
            aggregations=rql_q.aggregations or q.aggregations,
            selection=rql_q.selection or q.selection,
        )
    limit = q.limit if q.limit is not None else settings.default_limit
    if not authenticated:
        limit = min(limit, settings.default_limit)
        sizes = {r: q.get_facet_size(r) for r in q.facet_sizes}
        q = q._chain(
            facet_sizes={r: min(n, settings.max_facet_size) for r, n in sizes.items()}
        )
    offset = q.offset or 0
    q = q[offset : offset + limit]
    names = get_catalog().names
    invalid = q.dataset_names - names
    if invalid:
        raise HTTPException(
            422,
            detail=[
                f"Invalid dataset: `{', '.join(sorted(invalid))}` - "
                f"available: `{', '.join(sorted(names)) or 'none'}`"
            ],
        )
    return q
