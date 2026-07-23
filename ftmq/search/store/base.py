from typing import Iterable

from anystore.model import BaseModel

from ftmq.query import Query
from ftmq.search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq.search.settings import Settings

settings = Settings()


class BaseStore(BaseModel):
    uri: str = settings.uri

    def put(self, doc: EntityDocument) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError

    def search(
        self, q: str, query: Query | None = None
    ) -> Iterable[EntitySearchResult]:
        raise NotImplementedError

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        raise NotImplementedError
