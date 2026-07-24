"""
serialization data models as seen in
https://github.com/opensanctions/yente/
"""

from collections import defaultdict
from collections.abc import Iterable
from typing import Any, Self, Union

from fastapi import Request
from furl import furl
from pydantic import BaseModel, ConfigDict, Field

from ftmq.model import DatasetStats, EntityModel
from ftmq.query import Query
from ftmq.query.aggregations import AggregatorResult
from ftmq.search.model import AutocompleteResult
from ftmq.types import Entities, Entity

EntityProperties = dict[str, list[Union[str, "EntityResponse"]]]
Aggregations = dict[str, dict[str, Any]]


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


def pivot_aggregations(aggregations: AggregatorResult) -> Aggregations:
    """Pivot `{func: {prop: value}}` to `{prop: {func: value}}` (yente shape)."""
    agg_data: Aggregations = defaultdict(dict)
    for func, agg in aggregations.items():
        for field, value in agg.items():
            agg_data[field][func] = value
    return agg_data


class EntitiesResponse(BaseModel):
    total: int
    items: int
    stats: DatasetStats | None
    query: dict[str, Any]
    url: str
    next_url: str | None = None
    prev_url: str | None = None
    entities: list[EntityResponse]
    # populated when the query carries aggregations (openaleph-style: a query
    # with `limit=0` returns only these, no entities)
    aggregations: Aggregations | None = None

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
    ) -> Self:
        url = furl(str(request.url))
        entity_responses = [EntityResponse.from_entity(e, adjacents) for e in entities]
        count = stats.entity_count if stats else count
        response = cls(
            total=count,
            items=len(entity_responses),
            query=query.to_dict(),
            entities=entity_responses,
            stats=stats,
            url=str(url),
            aggregations=pivot_aggregations(aggregations) if aggregations else None,
        )
        limit, offset = query.limit or 0, query.offset or 0
        if limit:
            if offset > 0:
                url.args["offset"] = max(0, offset - limit)
                url.args["limit"] = limit
                response.prev_url = str(url)
            if offset + limit < count:
                url.args["offset"] = offset + limit
                url.args["limit"] = limit
                response.next_url = str(url)
        return response


class AutocompleteResponse(BaseModel):
    candidates: list[AutocompleteResult]
