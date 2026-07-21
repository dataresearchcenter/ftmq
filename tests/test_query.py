import pytest

from ftmq import C, G, M, P, Query, QueryError
from ftmq.query import Expr
from ftmq.util import make_entity

PERSON = make_entity(
    {
        "id": "p-1",
        "schema": "Person",
        "properties": {
            "name": ["Jane Doe"],
            "nationality": ["de"],
            "birthDate": ["1980-01-01"],
        },
        "datasets": ["people"],
    }
)
COMPANY = make_entity(
    {
        "id": "c-1",
        "schema": "Company",
        "properties": {"name": ["ACME Ltd"], "jurisdiction": ["de"]},
        "datasets": ["orgs"],
    }
)
DIRECTORSHIP = make_entity(
    {
        "id": "d-1",
        "schema": "Directorship",
        "properties": {"director": ["p-1"], "organization": ["c-1"]},
        "datasets": ["orgs"],
    }
)


def test_query_construction():
    # empty
    q = Query()
    assert not q
    assert q.to_dict() == {}
    assert q.apply(PERSON) is True

    # positional nodes AND-combine
    q = Query().where(M(schema="Person"), P(name="Jane Doe"))
    assert q
    assert len(list(q.q.iter_leaves())) == 2

    # constructor mirrors where()
    q2 = Query(M(schema="Person"), P(name="Jane Doe"))
    assert q2.to_dict() == q.to_dict()

    # chaining ANDs
    q3 = Query().where(M(schema="Person")).where(P(name="Jane Doe"))
    assert q3.to_dict() == q.to_dict()

    # operators build a tree
    expr = M(schema="Person") & P(name="Jane Doe")
    assert isinstance(expr, Expr)
    assert (P(name="a") | P(name="b")).connector == "OR"
    assert (~M(schema="Person")).negated is True


def test_apply_meta():
    assert Query().where(M(schema="Person")).apply(PERSON)
    assert not Query().where(M(schema="Person")).apply(COMPANY)
    assert Query().where(M(schema__in=["Person", "Company"])).apply(COMPANY)
    assert Query().where(M(schema__startswith="Pers")).apply(PERSON)

    # schemata is-a: Person is a LegalEntity, Company is a LegalEntity
    assert Query().where(M(schemata="LegalEntity")).apply(PERSON)
    assert Query().where(M(schemata="LegalEntity")).apply(COMPANY)
    # ... but a Person is not a Company
    assert not Query().where(M(schemata="Company")).apply(PERSON)

    assert Query().where(M(dataset="people")).apply(PERSON)
    assert not Query().where(M(dataset="orgs")).apply(PERSON)
    assert Query().where(M(dataset__in=["people", "orgs"])).apply(PERSON)

    assert Query().where(M(id="p-1")).apply(PERSON)
    assert Query().where(M(id__startswith="p-")).apply(PERSON)
    assert not Query().where(M(id="x")).apply(PERSON)


def test_apply_property_and_group():
    # specific property
    assert Query().where(P(name="Jane Doe")).apply(PERSON)
    assert Query().where(P(name__startswith="Jane")).apply(PERSON)
    assert Query().where(P(name__ilike="jane")).apply(PERSON)  # substring, in-memory
    assert not Query().where(P(name="Other")).apply(PERSON)

    # property-type group
    assert (
        Query().where(G(countries="de")).apply(PERSON)
    )  # nationality is country-typed
    assert Query().where(G(names__startswith="Jane")).apply(PERSON)
    assert Query().where(G(dates__gte="1979")).apply(PERSON)
    assert not Query().where(G(countries="fr")).apply(PERSON)


def test_apply_reverse_parity():
    """G(entities=id) == old `reverse`; P(<edgeProp>=id) is the narrow form."""
    assert Query().where(G(entities="p-1")).apply(DIRECTORSHIP)
    assert Query().where(G(entities="c-1")).apply(DIRECTORSHIP)
    assert Query().where(P(director="p-1")).apply(DIRECTORSHIP)
    assert not Query().where(G(entities="p-1")).apply(PERSON)


def test_apply_boolean():
    assert Query().where(P(name="X") | P(name="Jane Doe")).apply(PERSON)
    assert not Query().where(P(name="X") | P(name="Y")).apply(PERSON)
    assert Query().where(~M(schema="Company")).apply(PERSON)
    assert not Query().where(~M(schema="Person")).apply(PERSON)
    # nested
    q = Query().where(M(schema="Person") & (G(countries="de") | G(countries="at")))
    assert q.apply(PERSON)


def test_apply_null():
    # birthDate present -> null=True is False, null=False is True
    assert not Query().where(P(birthDate__null=True)).apply(PERSON)
    assert Query().where(P(birthDate__null=False)).apply(PERSON)
    # deathDate absent -> null=True is True
    assert Query().where(P(deathDate__null=True)).apply(PERSON)
    assert not Query().where(P(deathDate__null=False)).apply(PERSON)


def test_serialization_dict_roundtrip():
    q = (
        Query()
        .where(M(schemata="LegalEntity"), P(name__ilike="jane"))
        .where(G(countries="de") | G(countries="at"))
        .order_by("name", ascending=False)[10:20]
    )
    data = q.to_dict()
    assert "q" in data and data["order_by"] == ["-name"]
    assert data["limit"] == 10 and data["offset"] == 10
    assert Query.from_dict(data).to_dict() == data


def test_params_bridge():
    q = Query().where(M(schema="Person"), G(countries="de"))
    assert q.to_params() == {
        "filter:schema": ["Person"],
        "filter:countries": ["de"],
    }
    # keys are sorted for deterministic output
    assert q.to_string() == "filter:countries=de&filter:schema=Person"
    assert Query.from_string(q.to_string()).to_dict() == q.to_dict()

    # property, exclude, empty, range
    q2 = Query.from_string(
        "filter:properties.name=Jane"
        "&exclude:properties.country=ru"
        "&filter:gte:properties.date=2020"
        "&empty:properties.birthDate"
    )
    leaves = {(x.family, x.key, str(x.comparator)) for x in q2.q.iter_leaves()}
    assert ("P", "name", "eq") in leaves
    assert ("P", "country", "not") in leaves
    assert ("P", "date", "gte") in leaves
    assert ("P", "birthDate", "null") in leaves

    # multi-value -> __in, roundtrips to repeated params
    q3 = Query.from_params({"filter:schema": ["Person", "Company"]})
    (leaf,) = list(q3.q.iter_leaves())
    assert str(leaf.comparator) == "in"
    assert q3.to_params()["filter:schema"] == ["Company", "Person"]

    # dataset/collection aliases and id special-case
    q4 = Query.from_params({"filter:collection_id": ["ds1"], "filter:_id": ["e-1"]})
    assert q4.dataset_names == {"ds1"}
    assert {x.key for x in q4.q.iter_leaves()} == {"dataset", "id"}


def test_params_non_expressible():
    # cross-field OR cannot be an Aleph param
    with pytest.raises(QueryError):
        Query().where(P(name="a") | M(schema="Person")).to_params()
    # a negated multi-leaf group cannot either
    with pytest.raises(QueryError):
        Query().where(~(M(schema="Person") & P(name="a"))).to_params()


def test_collectors():
    q = Query().where(M(dataset="foo"), M(schema="Person"), G(countries="fr"))
    assert q.dataset_names == {"foo"}
    assert q.schemata_names == {"Person"}
    assert q.countries == {"fr"}

    q = Query().where(
        M(dataset__in=["foo", "bar"]),
        M(schema__in=["Person", "Company"]),
        G(countries__in=["de", "fr"]),
    )
    assert q.dataset_names == {"foo", "bar"}
    assert q.schemata_names == {"Company", "Person"}
    assert q.countries == {"de", "fr"}

    # schemata expands to the is-a set (self + non-abstract descendants)
    q = Query().where(M(schemata="LegalEntity"))
    assert q.schemata_names == {
        "LegalEntity",
        "Company",
        "Organization",
        "Person",
        "PublicBody",
    }

    # the `groups` collector includes the `entities` (reverse) group
    q = Query().where(G(entities="x-1"))
    assert len(q.groups) == 1


def test_order_and_slice():
    q = Query().order_by("date")
    assert q.to_dict() == {"order_by": ["date"]}
    q = Query().order_by("date", "name")
    assert q.to_dict() == {"order_by": ["date", "name"]}
    q = Query().order_by("date", ascending=False)
    assert q.to_dict() == {"order_by": ["-date"]}

    assert Query()[10].slice == slice(10, 11, None)
    assert Query()[:10].slice == slice(None, 10, None)
    q = Query()[1:10]
    assert q.slice == slice(1, 10, None)
    assert q.to_dict() == {"limit": 9, "offset": 1}

    with pytest.raises(QueryError):
        Query()[-1]
    with pytest.raises(QueryError):
        Query()[1:1:1]


def test_hash_and_eq():
    # 3+ nodes exercise the associativity flattening in to_dict
    a = Query().where(M(schema="Person"), P(name="Jane"), M(dataset="d"))
    b = Query().where(P(name="Jane")).where(M(dataset="d")).where(M(schema="Person"))
    # order-independent: structurally-equal queries serialize and hash equal
    assert a.to_dict() == b.to_dict()
    assert hash(a) == hash(b)
    assert hash(a) != hash(Query().where(M(schema="Company")))


def test_validation():
    with pytest.raises(QueryError):
        M(nonexistent="x")  # unknown meta field
    with pytest.raises(QueryError):
        M(schema="NotASchema")
    with pytest.raises(QueryError):
        P(notaprop="x")
    with pytest.raises(QueryError):
        G(notagroup="x")
    with pytest.raises(QueryError):
        P(name__notacomparator="x")


def test_aggregate_untouched():
    q = Query().where(M(schema="Payment"), P(date__gte="2023"), P(amount__null=False))
    q = q.aggregate("sum", "amountEur", "amount")
    data = q.to_dict()
    assert data["aggregations"] == {"sum": {"amount", "amountEur"}}
    assert "q" in data


def test_rql():
    # nested cross-field OR: M(schema=Person) & (P(name=jane) | G(countries=de))
    q = Query.from_rql(
        "and(eq(schema,Person),or(eq(properties.name,jane),eq(countries,de)))"
    )
    manual = Query().where(M(schema="Person") & (P(name="jane") | G(countries="de")))
    assert q.to_dict() == manual.to_dict()

    # not / in / range comparators + bare-property fallback
    q = Query.from_rql(
        "and(not(eq(schema,Organization)),in(name,(jane,joe)),gt(properties.amountEur,1000))"
    )
    manual = Query().where(
        ~M(schema="Organization"), P(name__in=["jane", "joe"]), P(amountEur__gt=1000)
    )
    assert q.to_dict() == manual.to_dict()

    # a single comparison (no and/or wrapper)
    assert Query.from_rql("eq(schema,Person)").to_dict() == (
        Query().where(M(schema="Person")).to_dict()
    )

    # unsupported operator raises
    with pytest.raises(QueryError):
        Query.from_rql("bogus(schema,Person)")

    # to_rql: nested tree round-trips through the string
    q = Query().where(M(schema="Person") & (P(name="jane") | G(countries="de")))
    assert q.to_rql() == (
        "and(eq(schema,Person),or(eq(properties.name,jane),eq(countries,de)))"
    )
    assert Query.from_rql(q.to_rql()).to_dict() == q.to_dict()

    # not / in / range round-trip and flatten the where()-nested ANDs
    q = Query().where(
        ~M(schema="Organization"), P(name__in=["jane", "joe"]), P(amountEur__gt=1000)
    )
    assert Query.from_rql(q.to_rql()).to_dict() == q.to_dict()

    assert Query().to_rql() == ""

    # comparators with no RQL equivalent raise on serialization
    with pytest.raises(QueryError):
        Query().where(P(name__startswith="ja")).to_rql()
    with pytest.raises(QueryError):
        Query().where(P(deathDate__null=True)).to_rql()


def test_context_node():
    entity = make_entity(
        {
            "id": "e1",
            "schema": "Person",
            "properties": {"name": ["Jane"]},
            "datasets": ["d"],
            "origin": ["crawl", "manual"],
        }
    )
    # in-memory: reads entity.context, multi-valued
    assert Query().where(C(origin="crawl")).apply(entity)
    assert Query().where(C(origin="manual")).apply(entity)
    assert not Query().where(C(origin="other")).apply(entity)
    # a missing context key just does not match (no error)
    assert not Query().where(C(fragment="x")).apply(entity)

    q = Query().where(C(origin="crawl"), M(schema="Person"))
    assert len(q.context) == 1
    # serialization round-trips
    assert Query.from_dict(q.to_dict()).to_dict() == q.to_dict()
    # Aleph bridge: `origin` is a known context field, others are not
    assert Query.from_string(q.to_string()).to_dict() == q.to_dict()
    with pytest.raises(QueryError):
        Query().where(C(fragment="x")).to_params()
