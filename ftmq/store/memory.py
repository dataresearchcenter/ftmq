from followthemoney.dataset.dataset import Dataset
from nomenklatura.store import memory as nk

from ftmq.store.base import Store, View
from ftmq.util import get_scope_dataset


class MemoryQueryView(View, nk.MemoryView):
    pass


class MemoryStore(Store, nk.MemoryStore):
    view_class = MemoryQueryView

    def get_scope(self) -> Dataset:
        return get_scope_dataset(*self.entities.keys())
