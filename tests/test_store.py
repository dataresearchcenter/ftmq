from followthemoney import EntityProxy, StatementEntity

from ftmq.query import A, C, G, M, P, Query, Year
from ftmq.store import MemoryStore, Store, get_store
from ftmq.store.aleph import AlephStore, parse_uri
from ftmq.store.base import get_resolver
from ftmq.store.fragments import get_fragments
from ftmq.store.lake import LakeStore
from ftmq.store.level import LevelDBStore
from ftmq.store.sql import SQLStore
from ftmq.util import get_scope_dataset, make_dataset, make_entity


def _run_store_test_implicit(cls: type[Store], proxies, **kwargs):
    # implicit catalog from store content
    store = cls(linker=get_resolver(), **kwargs)
    assert store._implicit_scope is True

    datasets_seen = set()
    with store.writer() as bulk:
        for proxy in proxies:
            if proxy.datasets - datasets_seen:
                bulk.add_entity(proxy)
                datasets_seen.update(proxy.datasets)

    assert store.get_scope().leaf_names == {"donations", "eu_authorities"}

    # regression: an unscoped store implicitly spans every dataset present in
    # the backend. nomenklatura scopes a view to `dataset.leaf_names`, so a
    # store opened without a dataset used to surface only entities literally
    # tagged dataset="default" (the `__init__` scope guard silently stopped
    # firing once followthemoney made a plain dataset its own leaf). The writer
    # scope stays "default", but the read scope and `default_view()` must span
    # all datasets.
    assert store.dataset.leaf_names == {"default"}
    assert store.scope.leaf_names == {"donations", "eu_authorities"}
    entities = list(store.default_view().entities())
    assert entities
    assert {ds for e in entities for ds in e.datasets} == {
        "donations",
        "eu_authorities",
    }
    return True


def _run_store_test(cls: type[Store], proxies, test_pop: bool | None = True, **kwargs):
    store = cls(
        dataset=get_scope_dataset("eu_authorities", "donations"),
        linker=get_resolver(),
        **kwargs,
    )

    assert store.default_view().get_entity("foo") is None

    with store.writer() as bulk:
        for proxy in proxies:
            bulk.add_entity(proxy)
    view = store.default_view()
    properties = view.get_entity("eu-authorities-satcen").to_dict()["properties"]
    assert properties == {
        "legalForm": ["security_agency"],
        "keywords": ["security_agency"],
        "website": ["https://www.satcen.europa.eu/"],
        "description": [
            "The European Union Satellite Centre (SatCen) supports EU decision-making and\naction in the context of Europe’s Common Foreign and Security Policy. This\nmeans providing products and services based on exploiting space assets and\ncollateral data, including satellite imagery and aerial imagery, and related\nservices."  # noqa
        ],
        "name": ["European Union Satellite Centre"],
        "weakAlias": ["SatCen"],
        "jurisdiction": ["eu"],
        "sourceUrl": ["https://www.asktheeu.org/en/body/satcen"],
    }
    assert store.dataset.leaf_names == {"donations", "eu_authorities"}
    tested = False
    for proxy in view.entities():
        assert isinstance(proxy, StatementEntity)
        tested = True
        break
    assert tested

    # iterate
    entities = [e for e in store.iterate()]
    assert len(entities) == 474 + 151
    entities = [e for e in store.iterate(dataset="eu_authorities")]
    assert len(entities) == 151

    view = store.default_view()
    assert len([e for e in view.entities()]) == 474 + 151
    ds = make_dataset("eu_authorities")
    view = store.view(ds)
    assert len([e for e in view.entities()]) == 151

    view = store.default_view()
    q = Query().where(M(dataset="eu_authorities"))
    res = [e for e in view.query(q)]
    assert len(res) == 151
    assert "eu_authorities" in res[0].datasets
    q = Query().where(M(schema="Payment"), P(date__gte=2011))
    res = [e for e in view.query(q)]
    assert all(r.schema.name == "Payment" for r in res)
    assert len(res) == 21

    # schemata (is-a) filters
    q = Query().where(M(schemata="Organization"))
    res = [e for e in view.query(q)]
    assert len(res) == 224
    q = Query().where(M(schema="LegalEntity"))
    res = [e for e in view.query(q)]
    assert len(res) == 0
    q = Query().where(M(schemata="LegalEntity"))
    res = [e for e in view.query(q)]
    assert len(res) == 246

    # stats
    q = Query().where(M(dataset="eu_authorities"))
    stats = view.stats(q)
    assert [c.model_dump() for c in stats.things.countries] == [
        {"code": "eu", "label": "European Union", "count": 151}
    ]
    assert stats.entity_count == 151
    assert [s.model_dump() for s in stats.things.schemata] == [
        {
            "name": "PublicBody",
            "label": "Public body",
            "plural": "Public bodies",
            "count": 151,
        }
    ]
    assert view.count(q) == 151

    # ordering
    q = Query().where(M(schema="Payment"), P(date__gte=2011))
    q = q.order_by("amountEur")
    res = [e for e in view.query(q)]
    assert len(res) == 21
    assert res[0].get("amountEur") == ["50001"]
    q = q.order_by("amountEur", ascending=False)
    res = [e for e in view.query(q)]
    assert len(res) == 21
    assert res[0].get("amountEur") == ["320000"]

    # slice
    q = Query().where(M(schema="Payment"), P(date__gte=2011))
    q = q.order_by("amountEur")
    q = q[:10]
    res = [e for e in view.query(q)]
    assert len(res) == 10
    assert res[0].get("payer") == ["efccc434cdf141c7ba6f6e539bb6b42ecd97c368"]

    q = Query().where(M(schema="Person")).order_by("name")[0]
    res = [e for e in view.query(q)]
    assert len(res) == 1
    assert res[0].caption == "Dr.-Ing. E. h. Martin Herrenknecht"

    # aggregation
    q = Query().aggregate(A(max=P("date")), A(min=P("date")))
    res = view.aggregations(q)
    assert res == {
        "max": {"properties.date": "2011-12-29"},
        "min": {"properties.date": "2002-07-04"},
    }

    q = Query().aggregate(A(count=M("id"), by=P("beneficiary")))
    res = view.aggregations(q)
    assert (
        res["groups"]["properties.beneficiary"]["count"]["id"][
            "6d03aec76fdeec8f9697d8b19954ab6fc2568bc8"
        ]
        == 10
    )
    assert len(proxies) == res["count"]["id"]

    q = (
        Query()
        .where(M(dataset="donations"))
        .aggregate(A(sum=P("amountEur"), by=P("beneficiary")))
    )
    res = view.aggregations(q)
    assert res == {
        "groups": {
            "properties.beneficiary": {
                "sum": {
                    "properties.amountEur": {
                        "6d03aec76fdeec8f9697d8b19954ab6fc2568bc8": 3368136.15,
                        "783d918df9f9178400d6b3386439ab3b3679979c": 6039987,
                        "6d8377d3938b85fa1bfd1985486f0f913c42e224": 6394282,
                        "d10764ddf47ca220527d385fc8fbaa62114408e4": 660008,
                        "7202347006660188aab5c1e264c4bee948478fd6": 4125977,
                        "c326dd8021ee75fe9608f31ecb4e2e7388144102": 17231420,
                        "542c6435219bd84c061ea407a6ab1e29b4d146d0": 1030898,
                        "9fbaa5733790781e56eec4998aeacf5093dccbf5": 290725,
                        "9e292c150c617eec85e5479c5f039f8441569441": 175000,
                        "49d46f7e70e19bc497a17734af53ea1a00c831d6": 1221256,
                        "4b308dc2b128377e63a4bf2e4c1b9fcd59614eee": 52000,  # pytest: MAX_SQL_AGG_GROUPS=11
                    }
                }
            }
        },
        "sum": {"properties.amountEur": 40589689.15},
    }
    q = (
        Query()
        .where(M(dataset="donations"))
        .aggregate(A(sum=P("amountEur"), by=Year()))
    )
    res = view.aggregations(q)
    assert res == {
        "groups": {
            "year": {
                "sum": {
                    "properties.amountEur": {
                        "2011": 1953402.15,
                        "2010": 3899002,
                        "2009": 6451130,
                        "2008": 6002766,
                        "2007": 3266005,
                        "2006": 4515084,
                        "2005": 7278646,
                        "2004": 2156628,
                        "2003": 2337982,
                        "2002": 2729044,
                    }
                }
            }
        },
        "sum": {"properties.amountEur": 40589689.15},
    }

    q = Query().where(M(dataset="donations")).aggregate(A(avg=P("amountEur")))
    res = view.aggregations(q)
    assert res == {"avg": {"properties.amountEur": 139964.44534482757}}

    # reverse lookup (the `entities` group)
    entity_id = "783d918df9f9178400d6b3386439ab3b3679979c"
    q = Query().where(G(entities=entity_id))
    res = [p for p in view.query(q)]
    assert len(res) == 53
    tested = False
    for proxy in res:
        assert entity_id in proxy.get("beneficiary")
        tested = True
    assert tested

    q = Query().where(G(entities=entity_id), M(schema="Payment"))
    q = q.where(P(date__gte=2007))
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 37
    q = Query().where(G(entities=entity_id), M(schema="Person"))
    res = [p for p in q.apply_iter(proxies)]
    assert len(res) == 0

    # ids
    q = Query().where(M(entity_id="eu-authorities-chafea"))
    res = [p for p in view.query(q)]
    assert len(res) == 1
    q = Query().where(M(canonical_id="eu-authorities-chafea"))
    res = [p for p in view.query(q)]
    assert len(res) == 1
    q = Query().where(M(entity_id="eu-authorities-chafea", dataset="donations"))
    res = [p for p in view.query(q)]
    assert len(res) == 0
    q = Query().where(M(canonical_id="eu-authorities-chafea", dataset="donations"))
    res = [p for p in view.query(q)]
    assert len(res) == 0
    q = Query().where(M(entity_id__startswith="eu-authorities-"))
    res = [p for p in view.query(q)]
    assert len(res) == 151
    q = Query().where(M(canonical_id__startswith="eu-authorities-"))
    res = [p for p in view.query(q)]
    assert len(res) == 151

    # `null` presence filters must agree with the in-memory evaluator on every
    # backend (they used to compile to `value IS true/false`: silently empty on
    # sqlite, a cast error on duckdb)
    q = Query().where(P(name__null=False))
    assert len([p for p in view.query(q)]) == 246
    q = Query().where(P(name__null=True))
    assert len([p for p in view.query(q)]) == 625 - 246
    q = Query().where(P(deathDate__null=True))
    assert len([p for p in view.query(q)]) == 625
    q = Query().where(G(countries__null=False))
    assert len([p for p in view.query(q)]) == 321
    q = Query().where(G(countries__null=True))
    assert len([p for p in view.query(q)]) == 625 - 321
    q = Query().where(M(schema="Payment"), P(amountEur__null=False))
    res = [p for p in view.query(q)]
    assert len(res) == 290
    assert all(r.get("amountEur") for r in res)

    # boolean composition must agree with the in-memory evaluator on every
    # backend: the SQL translation used to OR all property filters into one row
    # predicate (so an AND over two props matched either) and to drop `~`
    q = Query().where(P(name__ilike="agency"))
    assert len([p for p in view.query(q)]) == 23
    q = Query().where(P(name__ilike="agency"), P(jurisdiction="eu"))
    assert len([p for p in view.query(q)]) == 23
    q = Query().where(P(name__ilike="agency"), P(country="eu"))  # no country prop
    assert len([p for p in view.query(q)]) == 0
    q = Query().where(P(name__ilike="bank") | P(name__ilike="agency"))
    assert len([p for p in view.query(q)]) == 30
    q = Query().where(~P(name__ilike="agency"))
    assert len([p for p in view.query(q)]) == 625 - 23
    q = Query().where(
        M(schema="Person") & (P(name__ilike="herren") | G(countries="de"))
    )
    assert len([p for p in view.query(q)]) == 22
    q = Query().where(G(countries="de"), G(names__ilike="herren"))
    assert len([p for p in view.query(q)]) == 1

    # `M(id=...)` addresses the entity, not the statement's own id column
    q = Query().where(M(id="eu-authorities-chafea"))
    assert len([p for p in view.query(q)]) == 1

    # negated / OR-ed schema filters - on the lake backend these must also
    # disable `bucket` partition pruning, which is only sound for positive
    # schema conjuncts
    n_person = len([p for p in view.query(Query().where(M(schema="Person")))])
    assert n_person > 0
    q = Query().where(~M(schema="Person"))
    assert len([p for p in view.query(q)]) == 625 - n_person
    q = Query().where(M(schema="Person") | M(schema="PublicBody"))
    assert len([p for p in view.query(q)]) == n_person + 151

    # an empty negated node matches nothing (it used to compile to TRUE)
    q = Query().where(~M())
    assert len([p for p in view.query(q)]) == 0

    # `notlike` is a real comparator, not a silent fall-through
    q = Query().where(P(name__notlike="xyzzy"))
    assert len([p for p in view.query(q)]) == 246

    # offset-only and empty slices must not be dropped
    q = Query().where(M(dataset="eu_authorities"))
    assert len([p for p in view.query(q[10:])]) == 141
    assert len([p for p in view.query(q[0:0])]) == 0

    # chained same-field filters AND (like the in-memory evaluator); spell
    # alternatives as `__in`
    q = Query().where(M(dataset="eu_authorities")).where(M(dataset="donations"))
    assert len([p for p in view.query(q)]) == 0

    # pop
    # FIXME
    if test_pop:
        statements = store.writer().pop("006dd13b055a6b66947f991ced6c854defe0e626")
        assert len(statements) == 7
        statements = store.writer().pop("006dd13b055a6b66947f991ced6c854defe0e626")
        assert len(statements) == 0

    # origin
    q = Query().where(C(origin="test"))
    res = [p for p in view.query(q)]
    assert len(res) == 0

    return True


def test_store_scoped_views(tmp_path):
    # a canonical entity spanning two datasets: the scope selects which
    # *entities* a view surfaces (those with a statement in a scoped dataset),
    # while filters and assembly see the full canonical entity - this is the
    # nomenklatura in-memory view behaviour, and the SQL/Lake compilation must
    # match it, including for `~` / `|` trees and anti-joins
    entities = [
        # e1 exists in ds_a (birthDate) and ds_b (name)
        make_entity(
            {"id": "e1", "schema": "Person", "properties": {"birthDate": ["1980"]}},
            StatementEntity,
            "ds_a",
        ),
        make_entity(
            {"id": "e1", "schema": "Person", "properties": {"name": ["Jane"]}},
            StatementEntity,
            "ds_b",
        ),
        make_entity(
            {"id": "e2", "schema": "Person", "properties": {"name": ["Alice"]}},
            StatementEntity,
            "ds_a",
        ),
        make_entity(
            {"id": "e3", "schema": "Person", "properties": {"name": ["Bob"]}},
            StatementEntity,
            "ds_b",
        ),
        make_entity(
            {"id": "e4", "schema": "Company", "properties": {"name": ["Acme"]}},
            StatementEntity,
            "ds_c",
        ),
    ]

    from followthemoney.dataset.dataset import Dataset as FtmDataset

    def scope(name, *names):
        # a uniquely named scope dataset - `get_scope_dataset` names every
        # scope "default", and followthemoney interns datasets by name, which
        # would clobber the "default" scope other tests rely on
        ds = FtmDataset({"name": name, "datasets": list(names)})
        ds.children = {make_dataset(n) for n in names}
        return ds

    def build(cls, **kwargs):
        store = cls(
            dataset=scope("scope_abc", "ds_a", "ds_b", "ds_c"),
            linker=get_resolver(),
            **kwargs,
        )
        with store.writer() as bulk:
            for e in entities:
                bulk.add_entity(e)
        return store

    from nomenklatura.db import get_metadata

    stores = [build(MemoryStore)]
    get_metadata.cache_clear()
    stores.append(build(SQLStore, uri=f"sqlite:///{tmp_path}/scope.db"))
    stores.append(build(LakeStore, uri=tmp_path / "scope-lake"))

    def ids(view, q):
        return {e.id for e in view.query(q)}

    for store in stores:
        view_a = store.view(make_dataset("ds_a"))
        view_ab = store.view(scope("scope_ab", "ds_a", "ds_b"))

        # e1 is in scope via its ds_a fragment; its name (from ds_b) is
        # visible because the canonical entity assembles across datasets
        assert ids(view_a, Query().where(P(name="Jane"))) == {"e1"}
        assert ids(view_a, Query().where(P(name__null=True))) == set()
        res = [e for e in view_a.query(Query().where(P(birthDate__null=False)))]
        assert [e.id for e in res] == ["e1"]
        assert res[0].get("name") == ["Jane"]
        # out-of-scope entities stay invisible, even when they match
        assert ids(view_a, Query().where(P(name="Bob"))) == set()
        assert ids(view_a, Query().where(P(name="Acme"))) == set()
        # negation composes with the scope: entities without a ds_a fragment
        assert ids(view_ab, Query().where(~M(dataset="ds_a"))) == {"e3"}
        # an out-of-scope dataset filter matches nothing (no error)
        assert ids(view_ab, Query().where(M(dataset="ds_c"))) == set()
        assert len([e for e in view_a.query(Query())]) == 2


def test_store_memory(proxies):
    assert _run_store_test_implicit(MemoryStore, proxies)
    assert _run_store_test(MemoryStore, proxies)


def test_store_leveldb(tmp_path, proxies):
    path = tmp_path / "level.db"
    assert _run_store_test_implicit(LevelDBStore, proxies, path=path)
    path = tmp_path / "level2.db"
    assert _run_store_test(LevelDBStore, proxies, test_pop=False, path=path)  # FIXME


def test_store_sql_sqlite(tmp_path, proxies):
    uri = f"sqlite:///{tmp_path}/test.db"
    assert _run_store_test_implicit(SQLStore, proxies, uri=uri)

    from nomenklatura.db import get_metadata

    get_metadata.cache_clear()
    assert _run_store_test(SQLStore, proxies, test_pop=False, uri=uri)  # FIXME


def test_store_lake(tmp_path, proxies):
    assert _run_store_test_implicit(LakeStore, proxies, uri=tmp_path)
    assert _run_store_test(LakeStore, proxies, uri=tmp_path)
    lake = LakeStore(uri=tmp_path)
    lake.writer().optimize(vacuum=True)

    # test source property
    from followthemoney import model

    lake_path = tmp_path / "lake_source_test"
    lake = LakeStore(uri=lake_path)

    e1 = model.make_entity("Person")
    e1.id = "person-1"
    e1.add("name", "John Doe")

    e2 = model.make_entity("Company")
    e2.id = "company-1"
    e2.add("name", "Acme Corp")

    # test source at writer level
    with lake.writer(origin="crawl", source="https://example.com/data.json") as bulk:
        bulk.add_entity(e1)

    # test source at entity level (overrides writer source)
    with lake.writer(origin="crawl", source="https://default.com") as bulk:
        bulk.add_entity(e2, source="https://specific.com/company.json")

    # verify source column exists in data
    # we can't use the store interface here as it returns Statement model which
    # doesn't include that field
    import duckdb

    from ftmq.store.lake import setup_duckdb_storage

    setup_duckdb_storage()
    rel = duckdb.arrow(lake.deltatable.to_pyarrow_dataset())
    df = rel.df()

    person_rows = df[df["entity_id"] == "person-1"]
    assert len(person_rows) > 0
    assert person_rows["source"].iloc[0] == "https://example.com/data.json"

    company_rows = df[df["entity_id"] == "company-1"]
    assert len(company_rows) > 0
    assert company_rows["source"].iloc[0] == "https://specific.com/company.json"


def test_store_lake_fragment(tmp_path):
    from followthemoney.statement import Statement

    from ftmq.store.lake import LakeStatement

    lake = LakeStore(uri=tmp_path / "fragment_lake", dataset="test")
    stmt = Statement(
        entity_id="person-1",
        prop="name",
        schema="Person",
        value="John Doe",
        dataset="test",
        last_seen="2024-01-01T00:00:00",
    )
    with lake.writer(origin="ingest") as bulk:
        bulk.add_statement(LakeStatement.from_statement(stmt, "row-1"))
        # same content under a second fragment: same id, distinct row –
        # the batch keys on dedupe_key, not the bare statement id
        bulk.add_statement(LakeStatement.from_statement(stmt, "row-2"))
        bulk.add_statement(
            stmt.clone(value="Jane Doe")  # plain statement -> empty sentinel
        )

    import duckdb

    df = duckdb.arrow(lake.deltatable.to_pyarrow_dataset()).df()
    assert sorted(df[df["value"] == "John Doe"]["fragment"]) == ["row-1", "row-2"]
    assert list(df[df["value"] == "Jane Doe"]["fragment"]) == [""]

    # pop reads rows back as LakeStatement, preserving fragment
    statements = lake.writer().pop("person-1")
    keys = {s.dedupe_key for s in statements}
    assert len(statements) == 3
    assert {k.split("\t")[1] for k in keys} == {"", "row-1", "row-2"}


def test_store_init(tmp_path):
    store = get_store()
    assert isinstance(store, SQLStore)
    store = get_store("memory:///")
    assert isinstance(store, MemoryStore)
    path = tmp_path / "level.db"
    store = get_store(f"leveldb://{path}")
    assert isinstance(store, LevelDBStore)
    store = get_store("sqlite:///:memory:")
    assert isinstance(store, SQLStore)
    store = get_store(dataset="test_dataset")
    assert store.dataset.name == "test_dataset"
    store = get_store("http+aleph://test_dataset@aleph.example.org")
    assert isinstance(store, AlephStore)
    assert store.dataset.name == "test_dataset"
    store = get_store(f"lake+{tmp_path}")
    assert isinstance(store, LakeStore)


def test_store_aleph():
    assert parse_uri("http://localhost") == ("http://localhost", None, None)
    assert parse_uri("http://localhost") == ("http://localhost", None, None)
    assert parse_uri("https://dataset@localhost") == (
        "https://localhost",
        None,
        "dataset",
    )
    assert parse_uri("https://dataset:api_key@localhost") == (
        "https://localhost",
        "api_key",
        "dataset",
    )


def test_store_fragments_to_lake(tmp_path):
    fragments = get_fragments("test", database_uri="sqlite:///:memory:")
    lake = get_store(f"lake+{tmp_path}")
    f1 = EntityProxy.from_dict(
        {"id": "1", "schema": "LegalEntity", "properties": {"name": ["Jane Doe"]}}
    )
    f2 = EntityProxy.from_dict(
        {"id": "1", "schema": "Person", "properties": {"birthDate": ["2016-04-03"]}}
    )
    f3 = EntityProxy.from_dict(
        {"id": "2", "schema": "Organization", "properties": {"name": ["DARC"]}}
    )
    fragments.put(f1, origin="source1")
    fragments.put(f2, origin="source2")
    fragments.put(f3)
    origins = set()
    schemata = set()
    ids = set()
    for stmt in fragments.statements():
        origins.add(stmt.origin)
        schemata.add(stmt.schema)
        ids.add(stmt.entity_id)
        assert stmt.last_seen is not None
    assert origins == {None, "source1", "source2"}
    assert schemata == {"LegalEntity", "Person", "Organization"}
    assert ids == {"1", "2"}

    with lake.writer(origin="ingest") as bulk:
        for stmt in fragments.statements():
            bulk.add_statement(stmt)
    entities = list(lake.iterate())
    assert len(entities) == 2
    assert lake.get_origins() == {"ingest", "source1", "source2"}


def _numeric_fixture() -> list[StatementEntity]:
    """Payments whose amounts arrive display-formatted, as followthemoney's
    `number` type stores them verbatim (it neither normalizes nor rejects)."""
    return [
        make_entity(
            {
                "id": f"pay-{i}",
                "schema": "Payment",
                "properties": {"amountEur": [raw], "date": ["2023-01-01"]},
            },
            StatementEntity,
            "numbers",
        )
        for i, raw in enumerate(["1,000.50", "2000", "3,000,000.00", "1,500"])
    ]


def test_store_writer_casts_types():
    # a store writer normalizes statement values on the way in, so the read
    # side can assume the canonical format of the property type
    proxy = _numeric_fixture()[0]
    store = MemoryStore(dataset="numbers", linker=get_resolver())
    with store.writer() as bulk:
        bulk.add_entity(proxy)
    entity = list(store.iterate())[0]
    assert entity.get("amountEur") == ["1000.50"]
    # the raw value survives as the source value
    amounts = [s for s in entity.statements if s.prop == "amountEur"]
    assert [s.original_value for s in amounts] == ["1,000.50"]

    # opt out for a store that gets its values normalized elsewhere
    store = MemoryStore(dataset="numbers", linker=get_resolver(), cast_types=False)
    with store.writer() as bulk:
        bulk.add_entity(proxy)
    assert list(store.iterate())[0].get("amountEur") == ["1,000.50"]


def test_store_numeric_aggregation_agrees_with_memory(tmp_path):
    # the sql backends cast `value` to NUMERIC, so they agree with the
    # in-memory evaluator (which reads through `registry.number.to_number`)
    # only for values in the canonical format - which is what every store
    # writer casts them into on the way in (see `ftmq.statements`)
    entities = _numeric_fixture()
    scope = get_scope_dataset("numbers")
    q = (
        Query()
        .where(M(schema="Payment"))
        .aggregate(
            A(
                sum=P("amountEur"),
                min=P("amountEur"),
                max=P("amountEur"),
                count=M("id"),
            )
        )
    )

    expected = {"sum": 3004500.5, "min": 1000.5, "max": 3000000.0}
    stores: dict[str, Store] = {
        "memory": MemoryStore(dataset=scope, linker=get_resolver()),
        "sqlite": SQLStore(
            dataset=scope, linker=get_resolver(), uri=f"sqlite:///{tmp_path}/num.db"
        ),
        "lake": LakeStore(dataset=scope, linker=get_resolver(), uri=tmp_path / "lake"),
    }
    for name, store in stores.items():
        with store.writer() as bulk:
            for proxy in entities:
                bulk.add_entity(proxy)
        res = store.default_view().aggregations(q)
        for func, value in expected.items():
            assert res[func]["properties.amountEur"] == value, (name, func)
        # the meta field counts entities, not the `value` of a `prop = "id"`
        # row - which is the *unresolved* id, i.e. referents
        assert res["count"]["id"] == len(entities) == store.default_view().count(q)


def test_store_numeric_aggregation_skips_non_numeric(tmp_path):
    # a value that isn't a number at all (an unmigrated store, or one written
    # with `cast_types=False`) must not fail the aggregation: the sql backends
    # read it as NULL, so it drops out the way it does in memory, where
    # `registry.number.to_number` returns None
    entities = [
        make_entity(
            {
                "id": f"pay-{i}",
                "schema": "Payment",
                "properties": {"amountEur": [raw]},
            },
            StatementEntity,
            "numbers",
        )
        for i, raw in enumerate(["1000.50", "n/a", "2000", "unknown"])
    ]
    scope = get_scope_dataset("numbers")
    q = (
        Query()
        .where(M(schema="Payment"))
        .aggregate(A(sum=P("amountEur"), min=P("amountEur"), max=P("amountEur")))
    )
    expected = {"sum": 3000.5, "min": 1000.5, "max": 2000.0}
    stores: dict[str, Store] = {
        "memory": MemoryStore(dataset=scope, linker=get_resolver(), cast_types=False),
        "sqlite": SQLStore(
            dataset=scope,
            linker=get_resolver(),
            uri=f"sqlite:///{tmp_path}/nan.db",
            cast_types=False,
        ),
        "lake": LakeStore(
            dataset=scope,
            linker=get_resolver(),
            uri=tmp_path / "lake",
            cast_types=False,
        ),
    }
    for name, store in stores.items():
        with store.writer() as bulk:
            for proxy in entities:
                bulk.add_entity(proxy)
        res = store.default_view().aggregations(q)
        for func, value in expected.items():
            assert res[func]["properties.amountEur"] == value, (name, func)


def test_store_default_dataset_name_resolves(tmp_path):
    # regression: an implicit scope over a dataset literally named "default"
    # collapsed to an empty `leaf_names`, so `get_entity` filtered on
    # `dataset IN ()` and every entity 404'd while list queries still worked
    entity = make_entity(
        {"id": "e1", "schema": "Company", "properties": {"name": ["Acme"]}},
        StatementEntity,
        "default",
    )
    for cls, kwargs in (
        (MemoryStore, {}),
        (SQLStore, {"uri": f"sqlite:///{tmp_path}/default.db"}),
        (LakeStore, {"uri": tmp_path / "default_lake"}),
    ):
        store = cls(linker=get_resolver(), **kwargs)
        with store.writer() as bulk:
            bulk.add_entity(entity)
        assert store.scope.leaf_names == {"default"}
        view = store.default_view()
        assert [e.id for e in view.query(Query())] == ["e1"]
        # the detail path has to resolve what the list path returned
        assert view.get_entity("e1") is not None
