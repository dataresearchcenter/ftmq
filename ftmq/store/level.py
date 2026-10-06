from followthemoney.dataset.dataset import Dataset
from nomenklatura.store import level as nk

from ftmq.store.base import Store, View
from ftmq.util import get_scope_dataset


class LevelDBQueryView(View, nk.LevelDBView):
    pass


class LevelDBStore(Store, nk.LevelDBStore):
    view_class = LevelDBQueryView

    def get_scope(self) -> Dataset:
        names: set[str] = set()
        with self.db.iterator(prefix=b"s:", include_value=False) as it:
            for k in it:
                dataset = k.decode().split(":")[3]
                names.add(dataset)
        return get_scope_dataset(*names)
