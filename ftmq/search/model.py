from typing import Any, Iterable, Self

from followthemoney import EntityProxy
from followthemoney.types import registry
from pydantic import BaseModel, ConfigDict, Field

from ftmq.model import EntityModel
from ftmq.search.exceptions import IntegrityError
from ftmq.util import entity_fingerprints


class EntityDocument(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(..., examples=["NK-A7z...."])
    caption: str = Field(..., examples=["Jane Doe"])
    schema_: str = Field(..., examples=["LegalEntity"], alias="schema")
    datasets: list[str] = Field([], examples=[["us_ofac_sdn"]])
    countries: list[str] = Field([], examples=[["de"]])
    temporal_start: str | None = Field(default=None, examples=["2022-01"])
    temporal_end: str | None = Field(default=None, examples=["2022-01"])
    dates: list[str]
    names: list[str]
    fingerprints: list[str]
    linked_entities: list[str]
    text: str = ""

    @classmethod
    def from_entity(cls, entity: EntityProxy) -> Self:
        if entity.id is None:
            raise IntegrityError("Entity has no ID!")
        names = entity.get_type_values(registry.name)
        fps = sorted(entity_fingerprints(entity))
        text = " ".join(
            sorted([v for values in entity.properties.values() for v in values])
        )
        text = text or ""
        dates = entity.get_type_values(registry.date)

        return cls(
            id=entity.id,
            # `.datasets` is added by the StatementEntity / ValueEntity subclasses
            datasets=list(getattr(entity, "datasets", [])),
            schema=entity.schema.name,
            countries=entity.countries,
            caption=entity.caption,
            names=sorted(names),
            fingerprints=fps,
            linked_entities=entity.get_type_values(registry.entity),
            text=text,
            temporal_start=min(dates) if dates else None,
            temporal_end=max(dates) if dates else None,
            dates=dates,
        )


class EntitySearchResult(BaseModel):
    id: str = Field(..., examples=["NK-A7z...."])
    entity: EntityModel
    score: float = 1

    def __init__(self, /, **data: Any) -> None:
        if "entity" not in data:
            data["entity"] = self.make_entity(**data)
        super().__init__(**data)

    def to_proxy(self) -> EntityProxy:
        return self.entity.to_proxy()

    @staticmethod
    def make_entity(
        id: str,
        schema: str,
        datasets: Iterable[str],
        caption: str,
        names: Iterable[str],
        countries: Iterable[str] | None = None,
        **kwargs: Any,
    ) -> EntityModel:
        return EntityModel(
            id=id,
            schema=schema,
            datasets=list(datasets),
            caption=caption,
            properties={"name": list(names), "country": list(countries or [])},
            referents=[],
        )


class AutocompleteResult(BaseModel):
    id: str
    name: str
