from followthemoney.dataset.dataset import Dataset
from nomenklatura.store import redis_ as nk

from ftmq.store.base import Store, View
from ftmq.util import get_scope_dataset


class RedisQueryView(View, nk.RedisView):
    pass


class RedisStore(Store, nk.RedisStore):
    def get_scope(self) -> Dataset:
        # dataset membership is tracked under `ds:<dataset>` keys (see the
        # nomenklatura RedisWriter)
        names: set[str] = set()
        for key in self.db.scan_iter(match="ds:*"):
            names.add(key.decode().split(":", 1)[1])
        return get_scope_dataset(*names)

    def view(
        self, scope: Dataset | None = None, external: bool = False
    ) -> RedisQueryView:
        scope = scope or self.dataset
        return RedisQueryView(self, scope, external=external)
