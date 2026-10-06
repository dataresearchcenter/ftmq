from typing import Any, Iterable, Mapping, Self, Sequence, Type, TypeAlias

from followthemoney import E, model
from followthemoney.entity import ValueEntity
from followthemoney.types import registry
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ftmq.types import Entity
from ftmq.util import make_entity, must_str

Properties: TypeAlias = Mapping[str, Sequence["str | EntityModel"]]


def _extract_id(data: "str | dict | EntityModel") -> str:
    if isinstance(data, str):
        return data
    if isinstance(data, dict):
        return data["id"]
    return data.id


class EntityModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(..., examples=["NK-A7z...."])
    caption: str = Field(..., examples=["Jane Doe"])
    schema_: str = Field(..., examples=["LegalEntity"], alias="schema")
    properties: Properties = Field(
        default_factory=dict, examples=[{"name": ["Jane Doe"]}]
    )
    datasets: list[str] = Field([], examples=[["us_ofac_sdn"]])
    referents: list[str] = Field([], examples=[["ofac-1234"]])

    @classmethod
    def from_proxy(
        cls,
        entity: Entity,
        adjacents: Iterable[Entity] | Mapping[str, "EntityModel"] | None = None,
    ) -> Self:
        """Build from an entity, inlining `adjacents` (entities, or models by id)."""
        properties = dict(entity.properties)
        if adjacents:
            if not isinstance(adjacents, Mapping):
                adjacents = {must_str(e.id): cls.from_proxy(e) for e in adjacents}
            for prop in entity.iterprops():
                if prop.type == registry.entity:
                    properties[prop.name] = [
                        adjacents.get(i, i) for i in entity.get(prop)
                    ]
        return cls(
            id=must_str(entity.id),
            caption=entity.caption,
            schema=entity.schema.name,
            properties=properties,
            datasets=list(entity.datasets),
            referents=list(entity.referents),
        )

    def to_proxy(
        self, entity_type: Type[E] = ValueEntity, default_dataset: str | None = None
    ) -> E:
        """Turn the payload into an entity, un-nesting inlined entity properties."""
        schema = model[self.schema_]
        data = self.model_dump(by_alias=True)
        props = data.pop("properties", {})
        for prop in props:
            prop = schema.properties[prop]
            if prop.type == registry.entity:
                props[prop.name] = [_extract_id(v) for v in props[prop.name]]
        data["properties"] = props
        return make_entity(data, entity_type, default_dataset)

    @model_validator(mode="before")
    @classmethod
    def get_caption(cls, data: Any) -> Any:
        """Derive a missing `caption` from the entity, or the schema label."""
        if isinstance(data, dict):
            if data.get("caption") is None:
                entity = make_entity(data)
                data["caption"] = entity.caption
        return data
