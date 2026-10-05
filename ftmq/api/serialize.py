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
from ftmq.query.aggregations import AggregatorResult, make_agg
from ftmq.query.refs import IdRef
from ftmq.search.model import AutocompleteResult
from ftmq.types import Entities, Entity

EntityProperties = dict[str, list[Union[str, "EntityResponse"]]]


class ErrorResponse(BaseModel):
    detail: str = Field(..., examples=["Detailed error message"])


class EntityResponse(EntityModel):
    model_config = ConfigDict(populate_by_name=True)

    # not part of the public wire format (inherited from `EntityModel`)
    dataset: str | None = Field(None, exclude=True)
    # nested adjacents serialize as responses too (no `dataset`)
    properties: EntityProperties = Field(default_factory=dict)

    @classmethod
    def from_entity(cls, entity: Entity, adjacents: Entities | None = None) -> Self:
        return cls.from_proxy(entity, adjacents)


EntityResponse.model_rebuild()


def with_bucket_counts(query: Query) -> Query:
    """Add an entity count per facet, so every bucket has its `count`.

    Args:
        query: The request query.

    Returns:
        The query to compute the aggregations with.
    """
    groups = {g for agg in query.aggregations for g in agg.groups}
    if not groups:
        return query
    return query._chain(
        aggregations={*query.aggregations, make_agg("count", IdRef(), groups)}
    )


def build_metrics(aggregations: AggregatorResult, query: Query) -> dict[str, Any]:
    """The query's ungrouped aggregations as `{field: {func: value}}`.

    Args:
        aggregations: The computed aggregations.
        query: The request query; only its aggregations are reported.

    Returns:
        The Aleph-style `metrics`.
    """
    metrics: dict[str, Any] = defaultdict(dict)
    for agg in query.aggregations:
        value = aggregations.get(agg.func, {}).get(agg.key)
        if value is not None:
            metrics[agg.key][agg.func] = value
    return dict(metrics)


def _bucket(value: str, count: int) -> dict[str, Any]:
    return {"value": value, "label": value, "count": count, "metrics": {}}


def build_facets(aggregations: AggregatorResult, query: Query) -> dict[str, Any]:
    """Grouped aggregations as Aleph `facets` with buckets of
    `{value, label, count, metrics: {field: {func: value}}}`, ranked by the
    query's `facet_sort` metric, else by entity count.

    Args:
        aggregations: The computed aggregations, with bucket counts.
        query: The request query.

    Returns:
        The Aleph-style `facets`.
    """
    facets: dict[str, Any] = {}
    order = query.facet_sort
    for field, results in aggregations.get("groups", {}).items():
        counts = results.get("count", {}).get("id", {})
        buckets = {gval: _bucket(gval, count) for gval, count in counts.items()}
        ranked = False
        for agg in query.aggregations:
            if field not in {g.wire for g in agg.groups}:
                continue
            ranked = ranked or bool(order and order.orders(agg))
            for gval, value in results.get(agg.func, {}).get(agg.key, {}).items():
                bucket = buckets.setdefault(gval, _bucket(gval, 0))
                bucket["metrics"].setdefault(agg.key, {})[agg.func] = value
        values = sorted(buckets.values(), key=lambda v: (-v["count"], v["value"]))
        if order is not None and ranked:
            keyed = [
                (v["metrics"].get(order.ref.wire, {}).get(order.func), v)
                for v in values
            ]
            # stable: ties keep the count order
            present = [(m, v) for m, v in keyed if m is not None]
            present.sort(key=lambda x: x[0], reverse=not order.ascending)
            values = [v for _, v in present] + [v for m, v in keyed if m is None]
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
            facets=build_facets(aggregations, query) if aggregations else {},
            metrics=build_metrics(aggregations, query) if aggregations else {},
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
