"""
serialization data models as seen in
https://github.com/opensanctions/yente/
"""

import math
from collections import defaultdict
from collections.abc import Iterable
from typing import Any, Self, Union

from fastapi import Request
from furl import furl
from pydantic import BaseModel, ConfigDict, Field

from ftmq.model import DatasetStats, EntityModel
from ftmq.query import Query, QueryError
from ftmq.query.aggregations import AggregatorResult
from ftmq.search.model import AutocompleteResult
from ftmq.types import Entities, Entity

EntityProperties = dict[str, list[Union[str, "EntityResponse"]]]


class ErrorResponse(BaseModel):
    detail: str = Field(..., examples=["Detailed error message"])


class EntityResponse(EntityModel):
    model_config = ConfigDict(populate_by_name=True)

    # not part of the public wire format (inherited from `EntityModel`)
    dataset: str | None = Field(None, exclude=True)

    @classmethod
    def from_entity(cls, entity: Entity, adjacents: Entities | None = None) -> Self:
        return cls.from_proxy(entity, adjacents)


EntityResponse.model_rebuild()


def build_metrics(aggregations: AggregatorResult) -> dict[str, Any]:
    """Ungrouped aggregations as Aleph-style `metrics`: `{prop: {func: value}}`."""
    metrics: dict[str, Any] = defaultdict(dict)
    for func, props in aggregations.items():
        if func == "groups":
            continue
        for prop, value in props.items():
            metrics[prop][func] = value
    return dict(metrics)


def build_facets(aggregations: AggregatorResult) -> dict[str, Any]:
    """Grouped aggregations as Aleph-style `facets`:
    `{field: {"values": [{"value", "label", <func>: value}], "total": n}}`.

    A `count` grouping yields the idiomatic Aleph `{value, label, count}`; other
    functions ride under their function name in each value bucket.
    """
    facets: dict[str, Any] = {}
    for field, funcs in aggregations.get("groups", {}).items():
        buckets: dict[str, dict[str, Any]] = defaultdict(dict)
        for func, props in funcs.items():
            for _prop, gvals in props.items():
                for gval, value in gvals.items():
                    buckets[gval][func] = value
        values = [{"value": g, "label": g, **m} for g, m in buckets.items()]
        values.sort(key=lambda v: (-(v.get("count") or 0), v["value"]))
        facets[field] = {"values": values, "total": len(values)}
    return facets


def build_filters(query: Query) -> dict[str, list[str]]:
    """The applied positive filters as `{field: [values]}` (empty for nested queries)."""
    try:
        params = query.to_params()
    except QueryError:
        return {}
    return {
        key[len("filter:") :]: list(values)
        for key, values in params.items()
        if key.startswith("filter:")
    }


class EntitiesResponse(BaseModel):
    """The list / search response, matching the OpenAleph api v2 envelope."""

    status: str = "ok"
    results: list[EntityResponse] = []
    total: int = 0
    total_type: str = "eq"
    page: int = 1
    pages: int = 0
    limit: int = 0
    offset: int = 0
    next: str | None = None
    previous: str | None = None
    facets: dict[str, Any] = {}
    metrics: dict[str, Any] = {}
    filters: dict[str, list[str]] = {}
    query_q: str | None = None
    # ftmq extensions (additive; Aleph clients ignore extra keys)
    query: dict[str, Any] = {}
    stats: DatasetStats | None = None
    links: dict[str, str] = {}

    @classmethod
    def from_view(
        cls,
        request: Request,
        entities: Entities,
        query: Query,
        stats: DatasetStats | None = None,
        adjacents: Iterable[Entity] | None = None,
        count: int = 0,
        aggregations: AggregatorResult | None = None,
        query_q: str | None = None,
    ) -> Self:
        url = furl(str(request.url))
        results = [EntityResponse.from_entity(e, adjacents) for e in entities]
        total = stats.entity_count if stats else count
        limit, offset = query.limit or 0, query.offset or 0
        response = cls(
            results=results,
            total=total,
            limit=limit,
            offset=offset,
            page=(offset // limit + 1) if limit else 1,
            pages=math.ceil(total / limit) if limit else 0,
            query=query.to_dict(),
            query_q=query_q,
            stats=stats,
            filters=build_filters(query),
            facets=build_facets(aggregations) if aggregations else {},
            metrics=build_metrics(aggregations) if aggregations else {},
        )
        if limit:
            if offset > 0:
                url.args["offset"] = max(0, offset - limit)
                url.args["limit"] = limit
                response.previous = str(url)
            if offset + limit < total:
                url.args["offset"] = offset + limit
                url.args["limit"] = limit
                response.next = str(url)
        return response


class AutocompleteResponse(BaseModel):
    candidates: list[AutocompleteResult]
