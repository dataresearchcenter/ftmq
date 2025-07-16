from followthemoney.dataset.dataset import Dataset
from nomenklatura.store import level as nk

from ftmq.store.base import Store, View
from ftmq.util import get_scope_dataset


class LevelDBQueryView(View, nk.LevelDBView):
    pass


class LevelDBStore(Store, nk.LevelDBStore):
    def get_scope(self) -> Dataset:
        names: set[str] = set()
        with self.db.iterator(prefix=b"s:", include_value=False) as it:
            for k in it:
                dataset = k.decode().split(":")[3]
                names.add(dataset)
        return get_scope_dataset(*names)

    def view(
        self, scope: Dataset | None = None, external: bool = False
    ) -> LevelDBQueryView:
        scope = scope or self.dataset
        return LevelDBQueryView(self, scope, external=external)
