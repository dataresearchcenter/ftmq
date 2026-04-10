from typing import Any, Iterable, Mapping, Self, Sequence, Type, TypeAlias

from followthemoney import E
from followthemoney.entity import ValueEntity
from followthemoney.types import registry
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ftmq.types import Entity
from ftmq.util import DEFAULT_DATASET, make_entity, must_str

Properties: TypeAlias = Mapping[str, Sequence["str | EntityModel"]]


class EntityModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(..., examples=["NK-A7z...."])
    dataset: str = DEFAULT_DATASET
    caption: str = Field(..., examples=["Jane Doe"])
    schema_: str = Field(..., examples=["LegalEntity"], alias="schema")
    properties: Properties = Field(..., examples=[{"name": ["Jane Doe"]}])
    datasets: list[str] = Field([], examples=[["us_ofac_sdn"]])
    referents: list[str] = Field([], examples=[["ofac-1234"]])

    @classmethod
    def from_proxy(
        cls, entity: Entity, adjacents: Iterable[Entity] | None = None
    ) -> Self:
        properties = dict(entity.properties)
        if adjacents:
            adjacents_: dict[str, EntityModel] = {
                must_str(e.id): cls.from_proxy(e) for e in adjacents
            }
            for prop in entity.iterprops():
                if prop.type == registry.entity:
                    properties[prop.name] = [
                        adjacents_.get(i, i) for i in entity.get(prop)
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
        return make_entity(self.model_dump(by_alias=True), entity_type, default_dataset)

    @model_validator(mode="before")
    @classmethod
    def get_caption(cls, data: Any) -> Any:
        """Derive ``caption`` from the entity proxy if not provided.

        Uses ``ValueEntity.caption`` (the proxy property) which walks
        the schema's full caption property list — e.g. ``fileName`` or
        ``title`` for documents, ``identifier`` for things — and falls
        back to the schema label if no caption property has a value.
        The result is therefore always a non-empty string.
        """
        if data.get("caption") is None:
            entity = make_entity(data)
            data["caption"] = entity.caption
        return data
