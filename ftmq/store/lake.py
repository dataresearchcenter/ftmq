"""
https://openaleph.org/docs/lib/ftm-datalake/rfc/#basic-layout

A file-like "datalake" statement store.

Backend has to be local filesystem, s3 or anything else compatible with
`anystore.store.fs`

Layout:
    ```
    ./[dataset]/
        entities/
            statements/
                [origin]/
                    [entity_id]/
                        [uuid].csv
    ```
"""

import csv
from collections import defaultdict
from pathlib import Path
from typing import Generator, Optional

from anystore.store import get_store
from anystore.store.fs import Store as FSStore
from anystore.types import Uri
from anystore.util import make_data_checksum
from nomenklatura.dataset import DS
from nomenklatura.entity import CE
from nomenklatura.statement import Statement, StatementDict
from nomenklatura.store import View as NKView
from nomenklatura.store import Writer

from ftmq.model import Catalog
from ftmq.store.base import Store, View
from ftmq.types import CEGenerator

DEFAULT_ORIGIN = "default"
FIELDNAMES = Statement.__slots__


class LakeView(NKView):
    store: "LakeStore"

    def get_origins(
        self, dataset: str | None = None
    ) -> Generator[tuple[str, str], None, None]:
        datasets = self.dataset_names
        if dataset:
            datasets = [dataset]
        for dataset in datasets:
            prefix = self.store._backend.get_key(f"{dataset}/entities/statements")
            for child in self.store._backend._fs.ls(prefix):
                yield dataset, Path(child).name

    def has_entity(self, id: str) -> bool:
        for dataset in self.dataset_names:
            for origin in self.store.get_origins(dataset):
                prefix = f"{dataset}/entities/statements/{origin}/{id}"
                for _ in self.store._backend.iterate_keys(prefix=prefix):
                    return True
        return False

    def get_entity(self, id: str, dataset: str | None = None) -> Optional[CE]:
        statements: list[Statement] = []
        for ds, origin in self.get_origins(dataset):
            prefix = f"{ds}/entities/statements/{origin}/{id}"
            for key in self.store._backend.iterate_keys(prefix=prefix):
                with self.store._backend.open(key, mode="r") as h:
                    reader = csv.DictReader(h, fieldnames=FIELDNAMES)
                    for row in reader:
                        data = StatementDict(row)
                        statements.append(Statement.from_dict(data))
        return self.store.assemble(list(statements))

    def entities(self) -> CEGenerator:
        for dataset, origin in self.get_origins():
            prefix = self.store._backend.get_key(
                f"{dataset}/entities/statements/{origin}"
            )
            for path in self.store._backend._fs.ls(prefix):
                id = path.split("/")[-1]
                entity = self.get_entity(id, dataset=dataset)
                if entity is not None:
                    yield entity


class LakeQueryView(View, LakeView):
    pass


class LakeStore(Store):
    def __init__(self, uri: Uri, **kwargs):
        self._backend: FSStore = get_store(uri)
        assert isinstance(
            self._backend, FSStore
        ), f"Invalid store backend: `{self._backend.__class__}"
        super().__init__(**kwargs)

    def get_catalog(self) -> Catalog:
        names: set[str] = set()
        for child in self._backend._fs.ls(self._backend.uri):
            names.add(Path(child).name)
        return Catalog.from_names(names)

    def query(self, scope: DS | None = None, external: bool = False) -> LakeQueryView:
        scope = scope or self.dataset
        return LakeQueryView(self, scope, external=external)

    def writer(self) -> "Writer[DS, CE]":
        return LakeWriter(self)

    def view(self, scope: DS, external: bool = False) -> "NKView[DS, CE]":
        return LakeView(self, scope, external)


class LakeWriter(Writer[DS, CE]):
    store: LakeStore

    def add_statement(self, stmt: Statement, origin: str | None = None) -> None:
        origin = origin or DEFAULT_ORIGIN
        key = f"{stmt.dataset}/entities/statements/{origin}/{stmt.entity_id}/{stmt.id}.csv"
        with self.store._backend.open(key, "w") as h:
            writer = csv.DictWriter(h, fieldnames=FIELDNAMES)
            writer.writerow(stmt.to_csv_row())

    def add_entity(self, entity: CE, origin: str | None = None) -> None:
        datasets = defaultdict(list)
        for stmt in entity.statements:
            datasets[stmt.dataset].append(stmt)
        origin = origin or DEFAULT_ORIGIN
        for dataset, values in datasets.items():
            ids = "-".join(set(s.id for s in values))
            statements = [s.to_csv_row() for s in values]
            checksum = make_data_checksum(ids)
            key = f"{dataset}/entities/statements/{origin}/{entity.id}/{checksum}.csv"
            with self.store._backend.open(key, "w") as h:
                writer = csv.DictWriter(h, fieldnames=FIELDNAMES)
                writer.writerows(statements)
