import pytest
from followthemoney import StatementEntity, model

from ftmq.io import make_entity
from ftmq.query import Query


def test_proxy_composite():
    data = {"id": "1", "schema": "Thing", "properties": {"name": "Test"}}
    proxy = make_entity(data, StatementEntity)
    assert proxy.id == "1"
    assert proxy.get("name") == ["Test"]
    assert proxy.datasets == {"default"}

    data = {
        "id": "1",
        "schema": "Thing",
        "properties": {"name": "Test"},
        "datasets": ["test_dataset"],
    }
    proxy = make_entity(data, StatementEntity)
    assert proxy.id == "1"
    assert proxy.get("name") == ["Test"]
    assert proxy.datasets == {"test_dataset"}

    data = {
        "id": "1",
        "schema": "Thing",
        "properties": {"name": "Test", "sourceUrl": "https://example.org"},
        "datasets": ["test_dataset", "ds2"],
    }
    proxy = make_entity(data, StatementEntity, "another_dataset")
    assert proxy.id == "1"
    assert proxy.get("name") == ["Test"]
    assert proxy.datasets == {"another_dataset"}


def test_proxy_filter_dataset(proxies):
    q = Query()
    result = list(filter(q.apply, proxies))
    assert len(result) == len(proxies)

    q = q.where(dataset="eu_authorities")
    result = list(filter(q.apply, proxies))
    assert len(result) == 151


def test_proxy_filter_schema(proxies):
    q = Query().where(schema="Payment")
    result = list(filter(q.apply, proxies))
    assert len(result) == 290

    q = Query().where(schema="Organization")
    result = list(filter(q.apply, proxies))
    assert len(result) == 17

    q = Query().where(schema__in=["Payment", "Organization"])
    result = list(filter(q.apply, proxies))
    assert len(result) == 290 + 17

    q = Query().where(schema="Organization", schema_include_matchable=True)
    result = list(filter(q.apply, proxies))
    assert len(result) == 224

    # FIXME
    # q = Query().where(schema__not="Organization", schema_include_matchable=True)
    # result = list(filter(q.apply, proxies))
    # assert len(result) == len(proxies) - 224

    q = Query().where(schema="LegalEntity")
    result = list(filter(q.apply, proxies))
    assert len(result) == 0

    q = Query().where(schema="LegalEntity", schema_include_matchable=True)
    result = list(filter(q.apply, proxies))
    assert len(result) == 246

    q = Query().where(schema="LegalEntity", schema_include_descendants=True)
    result = list(filter(q.apply, proxies))
    assert len(result) == 246

    q = Query().where(schema=model.get("Person"))
    result = list(filter(q.apply, proxies))
    assert len(result) == 22

    q = Query().where(schema__startswith="Pers")
    result = list(filter(q.apply, proxies))
    assert len(result) == 22

    # invalid
    with pytest.raises(ValueError):
        q = Query().where(schema="Invalid schema")


def test_proxy_filter_property(proxies):
    q = Query().where(prop="country", value="cy")
    result = list(filter(q.apply, proxies))
    assert len(result) == 2

    q = Query().where(prop="date", value="2010", comparator="gte")
    result = list(filter(q.apply, proxies))
    assert len(result) == 49

    q = Query().where(prop="date", value="2010", comparator="gt")
    result = list(filter(q.apply, proxies))
    assert len(result) == 49

    # chained same props as AND
    q = q.where(prop="date", value="2011", comparator="lt")
    result = list(filter(q.apply, proxies))
    assert len(result) == 28

    q = Query().where(prop="date", value=2011, comparator="gte")
    result = list(filter(q.apply, proxies))
    assert len(result) == 21

    q = Query().where(prop="date", value=True, comparator="null")
    result = list(filter(q.apply, proxies))
    assert len(result) == 290

    q = Query().where(prop="date", value=False, comparator="null")
    result = list(filter(q.apply, proxies))
    assert len(result) == 290

    q = Query().where(prop="full", value="Am ", comparator="startswith")
    result = list(filter(q.apply, proxies))
    assert len(result) == 2

    q = Query().where(prop="city", value="Hamburg", comparator="endswith")
    result = list(filter(q.apply, proxies))
    assert len(result) == 8

    q = Query().where(prop="country", value="de", comparator="not")
    result = list(filter(q.apply, proxies))
    assert len(result) == 7


def test_proxy_filters_combined(proxies):
    q = Query().where(prop="country", value="de")
    q = q.where(schema="Event")
    result = list(filter(q.apply, proxies))
    assert len(result) == 0


def test_proxy_sort(proxies):
    tested = False
    q = Query().where(schema="Person").order_by("name")
    for proxy in q.apply_iter(proxies):
        assert proxy.caption == "Dr.-Ing. E. h. Martin Herrenknecht"
        tested = True
        break
    assert tested
    q = Query().where(schema="Person").order_by("name", ascending=False)
    for proxy in q.apply_iter(proxies):
        assert proxy.caption == "Johanna Quandt"
        tested = True
        break
    assert tested

    # numeric sort
    tested = False
    q = Query().where(schema="Payment").order_by("amountEur")
    for proxy in q.apply_iter(proxies):
        assert proxy.get("amountEur") == ["50000"]
        tested = True
        break
    tested = False
    q = Query().where(schema="Payment").order_by("amountEur", ascending=False)
    for proxy in q.apply_iter(proxies):
        assert proxy.get("amountEur") == ["2334526"]
        tested = True
        break


def test_proxy_slice(proxies):
    q = Query()[:10]
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 10
    q = Query()[10:20]
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 10
    q = Query().where(schema="Person").order_by("name")[0]
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 1
    assert res[0].caption == "Dr.-Ing. E. h. Martin Herrenknecht"


def test_proxy_filter_reverse(proxies):
    # here: reverse payments
    entity_id = "783d918df9f9178400d6b3386439ab3b3679979c"
    q = Query().where(reverse=entity_id)
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 53
    tested = False
    for proxy in res:
        assert entity_id in proxy.get("beneficiary")
        tested = True
    assert tested

    q = Query().where(reverse=entity_id, schema="Payment")
    q = q.where(prop="date", value=2007, comparator="gte")
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 37
    q = Query().where(reverse=entity_id, schema="Person")
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 0


def test_proxy_filter_ids(eu_authorities):
    q = Query().where(entity_id="eu-authorities-chafea")
    res = [p for p in q.apply_iter(eu_authorities)]
    assert len(res) == 1
    assert res[0].id == "eu-authorities-chafea"
    q = q.where(dataset="gdho")
    res = [p for p in q.apply_iter(eu_authorities)]
    assert len(res) == 0
    q = Query().where(entity_id__startswith="eu-authorities")
    res = [p for p in q.apply_iter(eu_authorities)]
    assert len(res) == len(eu_authorities)
