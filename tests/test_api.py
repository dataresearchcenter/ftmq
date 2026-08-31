import pytest

from tests.conftest import FIXTURES_PATH

pytest.importorskip("fastapi")

ADDRESS_ID = "97149caa3aef14e2be5ae6b3974c6882e7536d88"
METALL_ID = "62ad0fe6f56dbbf6fee57ce3da76e88c437024d5"
A29WP_ID = "eu-authorities-a29wp"
ACER_ID = "eu-authorities-acer"


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
    assert data["status"] == "ok"
    assert data["total"] == 625
    assert data["total_type"] == "eq"
    assert len(data["results"]) == 100

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
    entity = data["results"][0]
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
    assert len(data["results"]) == 10
    assert data["limit"] == 10 and data["offset"] == 0
    assert data["page"] == 1 and data["pages"] == 48  # ceil(474 / 10)
    assert "offset=10" in data["next"]
    assert data["previous"] is None

    res = api_client.get("/entities?filter:dataset=donations&limit=10&offset=10")
    data = res.json()
    assert data["page"] == 2
    assert "offset=0" in data["previous"]
    assert "offset=20" in data["next"]

    # limit is capped unless authenticated
    res = api_client.get("/entities?limit=500")
    assert len(res.json()["results"]) == 100
    res = api_client.get("/entities?limit=500&api_key=secret-key-for-build")
    assert len(res.json()["results"]) == 500


def test_api_entities_nested(api_client):
    res = api_client.get(
        "/entities?filter:schema=Payment&limit=1&nested=true&filter:dataset=donations"
    )
    data = res.json()
    entity = data["results"][0]
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
    res = api_client.get(f"/entities?filter:group.entities={ADDRESS_ID}")
    data = res.json()
    assert data["total"] == 1
    entity = data["results"][0]
    assert ADDRESS_ID in entity["properties"]["addressEntity"]


def test_api_aggregation(api_client):
    # aggregations ride on /entities (Aleph-style); `limit=0` returns only them
    res = api_client.get(
        "/entities?filter:dataset=donations&filter:schema=Payment"
        "&metric:sum=properties.amountEur&metric:min=properties.date"
        "&metric:max=properties.date&limit=0"
    )
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 290
    assert data["results"] == []
    # ungrouped aggregations -> Aleph `metrics`
    # fields are spelled as in the filter grammar, so a property keeps its
    # `properties.` prefix here too
    assert data["metrics"] == {
        "properties.amountEur": {"sum": 40589689.15},
        "properties.date": {"min": "2002-07-04", "max": "2011-12-29"},
    }

    res = api_client.get(
        "/entities?filter:dataset=donations&filter:schema=Payment"
        "&metric:count=id&facet=year&limit=0"
    )
    data = res.json()
    # grouped aggregations -> Aleph `facets` (value/count buckets)
    year = data["facets"]["year"]
    assert year["total"] == len(year["values"])
    bucket_2011 = next(v for v in year["values"] if v["value"] == "2011")
    assert bucket_2011["count"] > 0

    # aggregations returned alongside entities when limit > 0
    res = api_client.get(
        "/entities?filter:dataset=donations&filter:schema=Payment"
        "&metric:sum=properties.amountEur&limit=5"
    )
    data = res.json()
    assert len(data["results"]) == 5
    assert data["metrics"]["properties.amountEur"]["sum"] == 40589689.15


def test_api_search(api_client):
    # a `q` term routes /entities to full-text search via ftmq.search
    res = api_client.get("/entities?q=metall")
    assert res.status_code == 200
    data = res.json()
    assert len(data["results"]) == 3
    assert data["query_q"] == "metall"
    assert METALL_ID in {e["id"] for e in data["results"]}

    res = api_client.get("/entities?q=metall&filter:dataset=eu_authorities")
    assert len(res.json()["results"]) == 0

    res = api_client.get("/entities?q=metall&filter:group.countries=gb")
    assert len(res.json()["results"]) == 0

    # `exclude:` excludes - it does not select what it names
    res = api_client.get("/entities?q=metall&exclude:dataset=donations")
    assert len(res.json()["results"]) == 0
    res = api_client.get("/entities?q=metall&exclude:dataset=eu_authorities")
    assert len(res.json()["results"]) == 3

    # a filter the search index cannot express -> 400, not silently wrong hits
    res = api_client.get(
        "/entities?q=metall&rql=or(eq(dataset,donations),eq(group.countries,de))"
    )
    assert res.status_code == 400

    # too short -> 400
    res = api_client.get("/entities?q=xx")
    assert res.status_code == 400
    # no `q` -> normal listing, not an error
    res = api_client.get("/entities")
    assert res.status_code == 200
    assert res.json()["query_q"] is None


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
    assert len(data["results"]) == 5

    # an unknown dataset inside rql is still validated -> 422
    res = api_client.get("/entities?rql=eq(dataset,not_existent)")
    assert res.status_code == 422


def test_api_autocomplete(api_client):
    res = api_client.get("/autocomplete?q=verband")
    assert res.status_code == 200
    assert len(res.json()["candidates"]) == 5

    res = api_client.get("/autocomplete?q=ab")
    assert res.status_code == 400


def test_api_resolver_uri(api_client, tmp_path, monkeypatch):
    """`resolver_uri` + a resolved store: a referent id serves the canonical.

    The api reads a store whose statements already carry the canonical id (see
    the note in `docs/api.md`), so this builds one: entities written through a
    store that has the linker get it stamped on by the writer.
    """
    from followthemoney import StatementEntity
    from nomenklatura.db import Session, get_engine
    from nomenklatura.judgement import Judgement
    from nomenklatura.resolver import Resolver

    from ftmq.api import store as api_store
    from ftmq.io import smart_read_proxies
    from ftmq.store import get_store
    from ftmq.store.base import get_linker

    dump = tmp_path / "resolver.ijson"
    with Session(get_engine(f"sqlite:///{tmp_path}/resolver.db")) as session:
        resolver = Resolver[StatementEntity](session, create=True)
        canonical = str(
            resolver.decide(A29WP_ID, ACER_ID, Judgement.POSITIVE, user="test")
        )
        resolver.dump(dump)
    linker = get_linker(dump)

    store_uri = f"sqlite:///{tmp_path}/resolved.db"
    proxies = [
        p
        for p in smart_read_proxies(
            FIXTURES_PATH / "eu_authorities.ftm.json", entity_type=StatementEntity
        )
        if p.id in (A29WP_ID, ACER_ID)
    ]
    assert len(proxies) == 2
    store = get_store(store_uri, dataset="eu_authorities", linker=linker)
    with store.writer() as bulk:
        for proxy in proxies:
            bulk.add_entity(proxy)

    monkeypatch.setattr(api_store.settings, "store_uri", store_uri)
    monkeypatch.setattr(api_store.settings, "resolver_uri", str(dump))
    for cached in (api_store.get_store, api_store.get_view, api_store.get_catalog):
        cached.cache_clear()
    try:
        assert api_store.get_store().linker.get_canonical(A29WP_ID) == canonical

        # either referent, or the canonical itself, serves the merged entity
        for entity_id in (A29WP_ID, ACER_ID, canonical):
            res = api_client.get(f"/entities/{entity_id}")
            assert res.status_code == 200, entity_id
            data = res.json()
            assert data["id"] == canonical, entity_id
            assert set(data["referents"]) == {A29WP_ID, ACER_ID}, entity_id
            assert set(data["properties"]["name"]) == {
                "Article 29 Working Party",
                "Agency for the Cooperation of Energy Regulators",
            }, entity_id

        # and a query sees one entity, not one per referent
        res = api_client.get("/entities?filter:schema=PublicBody")
        data = res.json()
        assert data["total"] == 1
        assert data["results"][0]["id"] == canonical
    finally:
        monkeypatch.undo()
        for cached in (api_store.get_store, api_store.get_view, api_store.get_catalog):
            cached.cache_clear()
