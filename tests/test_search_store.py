import pytest
from followthemoney import ValueEntity

from ftmq import G, M, P, Query
from ftmq.model.entity import EntityModel as Entity
from ftmq.query.exceptions import QueryError
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

    # a negated filter excludes its values - it doesn't filter *to* them
    q = Query().where(M(dataset__not="donations"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 0
    q = Query().where(~M(dataset="donations"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 0
    q = Query().where(M(dataset__not="foo"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 3
    q = Query().where(M(schema__not="Organization"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 0
    q = Query().where(G(countries__not="gb"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 3
    # ... and a double negation is positive again
    q = Query().where(~M(schema__not="Organization"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 3

    # an is-a filter expands to the concrete schemata the index holds
    q = Query().where(M(schemata="LegalEntity"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 3
    q = Query().where(M(schemata__not="LegalEntity"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 0

    # a same-field OR folds into one filter, and its negation excludes both
    q = Query().where(G(countries="de") | G(countries="lu"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 3
    q = Query().where(~(G(countries="de") | G(countries="lu")))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 0

    # a filter on a field the index doesn't hold is ignored
    q = Query().where(P(name__ilike="%metall%"))
    res = [r for r in store.search("metall", q)]
    assert len(res) == 3

    # a filter the index can't express raises instead of quietly mis-filtering
    for q in (
        Query().where(M(dataset="donations") | G(countries="de")),
        Query().where(G(countries__ilike="d%")),
        Query().where(~(M(dataset="donations") & G(countries="de"))),
    ):
        with pytest.raises(QueryError):
            list(store.search("metall", q))

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
