from followthemoney.dataset.dataset import Dataset
from nomenklatura.store import redis_ as nk

from ftmq.store import Store, View


class RedisQueryView(View, nk.RedisView):
    pass


class RedisStore(Store, nk.RedisStore):
    def query(
        self, scope: Dataset | None = None, external: bool = False
    ) -> RedisQueryView:
        scope = scope or self.dataset
        return RedisQueryView(self, scope, external=external)
