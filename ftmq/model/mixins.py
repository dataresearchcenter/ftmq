from anystore.mixins import BaseModel as _BaseModel
from pydantic import Field


class BaseModel(_BaseModel):
    from_uri: str | None = Field(default=None, exclude=True)

    def __init__(self, **data):
        from_uri = data.pop("from_uri", None)
        if from_uri is not None:
            data = self._from_uri(from_uri, **data).model_dump()
            data["uri"] = data.get("uri") or from_uri
        super().__init__(**data)
