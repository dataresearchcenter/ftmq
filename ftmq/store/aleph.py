from functools import cached_property
from typing import Any, Self

from alephclient.api import AlephAPI
from alephclient.settings import API_KEY, HOST, MAX_TRIES
from followthemoney import Dataset, EntityProxy, Statement, StatementEntity, ValueEntity
from followthemoney.namespace import Namespace
from furl import furl
from nomenklatura.resolver import Resolver
from nomenklatura.store import Writer

from ftmq.query import Query
from ftmq.store.base import Store, View
from ftmq.types import StatementEntities
from ftmq.util import apply_dataset, ensure_entity, make_entity

uns = Namespace()


def parse_uri(uri: str) -> tuple[str, str | None, str | None]:
    """
    http://host.org
    https://dataset@host.org
    https://dataset:api_key@host.org
    """
    u = furl(uri)
    api_key = str(u.password) if u.password else API_KEY
    dataset = str(u.username) if u.username else None
    return f"{u.scheme}://{u.host}", api_key, dataset


class AlephStore(Store):
    def __init__(
        self, host: str | None = None, api_key: str | None = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.host = host or HOST
        self.api_key = api_key or API_KEY

    @classmethod
    def from_uri(
        cls,
        uri: str,
        dataset: Dataset | str | None = None,
        linker: Resolver | None = None,
    ) -> Self:
        host, api_key, foreign_id = parse_uri(uri)
        if dataset is None and foreign_id is not None:
            dataset = foreign_id

        return cls(dataset=dataset, linker=linker, host=host, api_key=api_key)

    @cached_property
    def api(self):
        return AlephAPI(self.host, self.api_key, retries=MAX_TRIES)

    @cached_property
    def collection(self) -> dict[str, Any]:
        return self.api.load_collection_by_foreign_id(self.dataset.name)

    @cached_property
    def ns(self) -> Namespace:
        return Namespace(self.dataset.name)

    def view(
        self, scope: Dataset | None = None, external: bool = False
    ) -> "AlephQueryView":
        return AlephQueryView(self, scope, external=external)

    def writer(self) -> "AlephWriter":
        return AlephWriter(self)


class AlephQueryView(View):
    store: AlephStore

    def entities(self, query: Query | None = None) -> StatementEntities:
        for proxy in self.store.api.stream_entities(self.store.collection):
            proxy = make_entity(
                proxy, StatementEntity, default_dataset=self.store.dataset.name
            )
            yield uns.apply(proxy)

    def get_entity(self, id: str) -> StatementEntity | None:
        entity_id = self.store.ns.sign(id)
        if not entity_id:
            raise RuntimeError(f"Invalid id: `{id}`")
        proxy = self.store.api.get_entity(entity_id)
        if proxy is not None:
            proxy = make_entity(proxy, StatementEntity, self.store.dataset.name)
            return uns.apply(proxy)
        return None


class AlephWriter(Writer):
    BATCH = 1_000
    store: AlephStore

    def __init__(self, store: AlephStore):
        self.store = store
        self.batch: list[ValueEntity] = []

    def flush(self) -> None:
        if self.batch:
            self.store.api.write_entities(self.store.collection["id"], self.batch)
        self.batch = []

    def add_entity(self, entity: EntityProxy) -> None:
        e = ensure_entity(entity, ValueEntity, self.store.dataset)
        e = apply_dataset(e, dataset=self.store.dataset)
        e.id = self.store.linker.get_canonical(e.id)
        self.batch.append(e)
        if len(self.batch) >= self.BATCH:
            self.flush()

    def pop(self, entity_id: str) -> list[Statement]:
        # FIXME this actually doesn't delete anything
        self.flush()
        statements: list[Statement] = []
        view = self.store.default_view()
        entity = view.get_entity(entity_id)
        if entity is not None:
            statements = list(entity.statements)
        return statements
