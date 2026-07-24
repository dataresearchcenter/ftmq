import pytest

pytest.importorskip("fastapi")

ADDRESS_ID = "97149caa3aef14e2be5ae6b3974c6882e7536d88"
METALL_ID = "62ad0fe6f56dbbf6fee57ce3da76e88c437024d5"


def test_api_index(api_client):
    res = api_client.get("/")
    assert res.status_code == 200


def test_api_catalog(api_client):
    res = api_client.get("/catalog")
    assert res.status_code == 200
    data = res.json()
    assert len(data["datasets"]) == 2

    res = api_client.get("/catalog/eu_authorities")
    assert res.status_code == 200
    data = res.json()
    assert data["name"] == "eu_authorities"
    assert data["entity_count"] == 151

    # unknown dataset fails the Literal validation
    res = api_client.get("/catalog/not_existent")
    assert res.status_code == 422


def test_api_entities(api_client):
    res = api_client.get("/entities")
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 625
    assert data["items"] == 100

    res = api_client.get("/entities?filter:dataset=donations")
    assert res.json()["total"] == 474
    res = api_client.get("/entities?filter:dataset=eu_authorities")
    assert res.json()["total"] == 151
    res = api_client.get(
        "/entities?filter:dataset=donations&filter:dataset=eu_authorities"
    )
    assert res.json()["total"] == 625

    res = api_client.get("/entities?filter:dataset=eu_authorities&stats=1")
    data = res.json()
    assert data["stats"]["entity_count"] == 151

    # unknown dataset -> 422
    res = api_client.get("/entities?filter:dataset=not_existent")
    assert res.status_code == 422
    # invalid property -> 400
    res = api_client.get("/entities?filter:properties.foo=bar")
    assert res.status_code == 400


def test_api_entities_filtered(api_client):
    url = "/entities?filter:properties.jurisdiction=eu&sort=name:desc&dehydrate=true"
    res = api_client.get(url)
    data = res.json()
    assert data["total"] == 151
    entity = data["entities"][0]
    assert entity["id"].startswith("eu-authorities-")
    # dehydrated: no jurisdiction property
    assert "jurisdiction" not in entity["properties"]
    # wire format: no `dataset` key
    assert "dataset" not in entity

    # `exclude:` matches entities that carry the property with another value
    res = api_client.get(
        "/entities?filter:dataset=eu_authorities&exclude:properties.jurisdiction=eu"
    )
    assert res.json()["total"] == 0

    res = api_client.get(
        "/entities?filter:schema=Payment&filter:gte:properties.date=2010"
    )
    assert res.json()["total"] == 49

    res = api_client.get(
        "/entities?filter:ilike:properties.name=metall&filter:schema=Organization"
    )
    assert res.json()["total"] == 3

    res = api_client.get("/entities?filter:startswith:canonical_id=eu-authorities-")
    assert res.json()["total"] == 151

    res = api_client.get("/entities?filter:entity_id=eu-authorities-chafea")
    assert res.json()["total"] == 1


def test_api_entities_paging(api_client):
    res = api_client.get("/entities?filter:dataset=donations&limit=10&offset=0")
    data = res.json()
    assert data["items"] == 10
    assert "offset=10" in data["next_url"]
    assert data["prev_url"] is None

    res = api_client.get("/entities?filter:dataset=donations&limit=10&offset=10")
    data = res.json()
    assert "offset=0" in data["prev_url"]
    assert "offset=20" in data["next_url"]

    # limit is capped unless authenticated
    res = api_client.get("/entities?limit=500")
    assert res.json()["items"] == 100
    res = api_client.get("/entities?limit=500&api_key=secret-key-for-build")
    assert res.json()["items"] == 500


def test_api_entities_nested(api_client):
    res = api_client.get(
        "/entities?filter:schema=Payment&limit=1&nested=true&filter:dataset=donations"
    )
    data = res.json()
    entity = data["entities"][0]
    # adjacent entities are inlined
    nested = [
        v
        for values in entity["properties"].values()
        for v in values
        if isinstance(v, dict)
    ]
    assert len(nested) > 0
    assert "id" in nested[0] and "schema" in nested[0]


def test_api_entity_detail(api_client):
    res = api_client.get(f"/entities/{ADDRESS_ID}")
    assert res.status_code == 200
    data = res.json()
    assert data["id"] == ADDRESS_ID
    assert data["caption"] == "Schillerstraße 19, 76135 Karlsruhe"
    assert data["schema"] == "Address"
    assert data["datasets"] == ["donations"]
    assert "dataset" not in data

    res = api_client.get("/entities/not_existent")
    assert res.status_code == 404


def test_api_entities_reverse(api_client):
    res = api_client.get(f"/entities?filter:entities={ADDRESS_ID}")
    data = res.json()
    assert data["total"] == 1
    entity = data["entities"][0]
    assert ADDRESS_ID in entity["properties"]["addressEntity"]


def test_api_aggregation(api_client):
    # aggregations ride on /entities (Aleph-style); `limit=0` returns only them
    res = api_client.get(
        "/entities?filter:dataset=donations&filter:schema=Payment"
        "&metric:sum=amountEur&metric:min=date&metric:max=date&limit=0"
    )
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 290
    assert data["items"] == 0
    assert data["entities"] == []
    assert data["aggregations"] == {
        "amountEur": {"sum": 40589689.15},
        "date": {"min": "2002-07-04", "max": "2011-12-29"},
    }

    res = api_client.get(
        "/entities?filter:dataset=donations&filter:schema=Payment"
        "&metric:count=id&facet=year&limit=0"
    )
    data = res.json()
    groups = data["aggregations"]["year"]["groups"]
    assert "count" in groups
    assert groups["count"]["id"]["2011"] > 0

    # aggregations returned alongside entities when limit > 0
    res = api_client.get(
        "/entities?filter:dataset=donations&filter:schema=Payment"
        "&metric:sum=amountEur&limit=5"
    )
    data = res.json()
    assert data["items"] == 5
    assert data["aggregations"]["amountEur"]["sum"] == 40589689.15


def test_api_search(api_client):
    res = api_client.get("/search?q=metall")
    assert res.status_code == 200
    data = res.json()
    assert data["items"] == 3
    assert METALL_ID in {e["id"] for e in data["entities"]}

    res = api_client.get("/search?q=metall&filter:dataset=eu_authorities")
    assert res.json()["items"] == 0

    res = api_client.get("/search?q=metall&filter:countries=gb")
    assert res.json()["items"] == 0

    # too short
    res = api_client.get("/search?q=xx")
    assert res.status_code == 400
    # missing
    res = api_client.get("/search")
    assert res.status_code == 422


def test_api_rql(api_client):
    # nested OR across datasets, expressible only via rql
    res = api_client.get(
        "/entities?rql=or(eq(dataset,donations),eq(dataset,eu_authorities))"
    )
    assert res.status_code == 200
    assert res.json()["total"] == 625

    # an rql filter tree with flat pagination on top
    res = api_client.get("/entities?rql=eq(schema,Payment)&limit=5")
    data = res.json()
    assert data["total"] == 290
    assert data["items"] == 5

    # an unknown dataset inside rql is still validated -> 422
    res = api_client.get("/entities?rql=eq(dataset,not_existent)")
    assert res.status_code == 422


def test_api_autocomplete(api_client):
    res = api_client.get("/autocomplete?q=verband")
    assert res.status_code == 200
    assert len(res.json()["candidates"]) == 5

    res = api_client.get("/autocomplete?q=ab")
    assert res.status_code == 400
