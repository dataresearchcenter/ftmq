from followthemoney import ValueEntity

from ftmq import G, M, Query
from ftmq.model.entity import EntityModel as Entity
from ftmq.search.logic import index_entities
from ftmq.search.store import get_store
from ftmq.search.store.base import BaseStore


def _test_store(things, store: BaseStore):
    index_entities(things, store)
    res = [r for r in store.search("metall")]
    assert len(res) == 3
    assert res[0].id == "62ad0fe6f56dbbf6fee57ce3da76e88c437024d5"
    assert isinstance(res[0].entity, Entity)
    assert isinstance(res[0].to_proxy(), ValueEntity)

    res = [r for r in store.search("metall OR tchibo")]
    assert len(res) == 4
    res = [r for r in store.search("metall AND tchibo")]
    assert len(res) == 0
    res = [r for r in store.autocomplete("verband")]
    assert len(res) == 5

    # use filters
    q = Query().where(M(dataset="donations"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 3
    q = Query().where(M(dataset="foo"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 0

    q = Query().where(M(dataset="donations", schema="Organization"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 3
    q = Query().where(M(dataset="foo", schema="Organization"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 0
    q = Query().where(M(dataset="donations", schema="Person"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 0

    q = Query().where(G(countries__in=["de", "lu"]))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 3
    q = Query().where(G(countries="gb"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 0

    return True


def test_search_store_sqlite(things, tmp_path):
    store = get_store(uri="sqlite:///" + str(tmp_path / "ftmqs.db"))
    assert _test_store(things, store)


def test_search_store_tantivy(things, tmp_path):
    store = get_store(uri=f'tantivy://{tmp_path / "tantivy.db"}')
    assert _test_store(things, store)


def test_search_store_memory(things):
    store = get_store(uri="memory:///")
    assert _test_store(things, store)
