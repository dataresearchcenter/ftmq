from typing import Any

import pytest

from ftmq import A, C, G, M, P, Query, QueryError, Year
from ftmq.query import Expr
from ftmq.query.refs import GroupRef, PropRef
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


def test_dedupe():
    # a repeated condition is held once, on every surface
    q = Query(P(name="x"), P(name="x"))
    assert len(q._leaves) == 1
    assert q.to_dict() == Query(P(name="x")).to_dict()
    assert q.to_rql() == "eq(properties.name,x)"
    assert q.to_params() == {"filter:properties.name": ["x"]}
    # ... including in the compiled sql (a duplicate used to add a second,
    # identical `canonical_id IN (...)` subquery and lose the flat compilation)
    assert str(q.sql.statements) == str(Query(P(name="x")).sql.statements)

    # chained `.where()` and `|` dedupe the same way
    assert Query().where(P(name="x")).where(P(name="x")).to_dict() == q.to_dict()
    assert len(Query(P(name="x") | P(name="x"))._leaves) == 1

    # a duplicate across nesting levels: the sub-group is spliced in first
    a, b = P(name="x"), M(schema="Person")
    assert Query((a & b) & a).to_dict() == Query(a & b).to_dict()
    assert Query().where(a).where(a, b).to_dict() == Query(a & b).to_dict()
    assert Query((a | b) | a).to_dict() == Query(a | b).to_dict()

    # a leaf hashes over its canonical form, so alternatives spelled in another
    # order are the same condition
    assert len(Query(P(name__in=["a", "b"]), P(name__in=["b", "a"]))._leaves) == 1

    # what is not the same condition stays: distinct values, distinct families
    # (`topics` is both a property and a property-type group) and a negation
    assert len(Query(P(name="x"), P(name="y"))._leaves) == 2
    assert len(Query(P(topics="x"), G(topics="x"))._leaves) == 2
    assert len(Query(P(name="x"), ~P(name="x"))._leaves) == 2


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


def test_apply_notlike():
    # substring-negating comparators (used to silently match nothing)
    assert Query().where(P(name__notlike="XYZ")).apply(PERSON)
    assert not Query().where(P(name__notlike="Jane")).apply(PERSON)
    assert Query().where(P(name__notilike="xyz")).apply(PERSON)
    assert not Query().where(P(name__notilike="jane")).apply(PERSON)


def test_apply_between_removed():
    # `between` never evaluated (both evaluators raised); it is no longer part
    # of the grammar and is rejected when the lookup is parsed
    with pytest.raises(QueryError, match="Invalid comparator"):
        Query().where(P(name__between="a"))


def test_serialization_dict_roundtrip():
    q = (
        Query()
        .where(M(schemata="LegalEntity"), P(name__ilike="jane"))
        .where(G(countries="de") | G(countries="at"))
        .order_by("name", ascending=False)[10:20]
    )
    data = q.to_dict()
    assert "q" in data and data["order_by"] == "-name"
    assert data["limit"] == 10 and data["offset"] == 10
    assert Query.from_dict(data).to_dict() == data


def test_params_bridge():
    q = Query().where(M(schema="Person"), G(countries="de"))
    assert q.to_params() == {
        "filter:schema": ["Person"],
        "filter:group.countries": ["de"],
    }
    # keys are sorted for deterministic output
    assert q.to_string() == "filter:group.countries=de&filter:schema=Person"
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


def test_params_prefix_ops():
    # substring / prefix comparators are an ftmq extension of the grammar
    # (openaleph-search never emits them, interop stays a superset)
    q = Query.from_string(
        "filter:ilike:properties.name=jane" "&filter:startswith:canonical_id=eu-"
    )
    leaves = {(x.family, x.key, str(x.comparator)) for x in q.q.iter_leaves()}
    assert ("P", "name", "ilike") in leaves
    assert ("M", "canonical_id", "startswith") in leaves
    assert Query.from_string(q.to_string()).to_dict() == q.to_dict()

    q2 = Query().where(P(name__like="doe"), M(id__endswith="-x"))
    params = q2.to_params()
    assert params["filter:like:properties.name"] == ["doe"]
    assert params["filter:endswith:id"] == ["-x"]


def test_params_exclude_multi():
    # `not_in` accepts a list (like `in`); multi-value exclude round-trips
    q = Query().where(M(dataset__not_in=["a", "b"]))
    (leaf,) = list(q.q.iter_leaves())
    assert str(leaf.comparator) == "not_in"
    assert leaf.value == {"a", "b"}
    assert q.to_string() == "exclude:dataset=a&exclude:dataset=b"
    q2 = Query.from_string("exclude:schema=Person&exclude:schema=Company")
    (leaf2,) = list(q2.q.iter_leaves())
    assert str(leaf2.comparator) == "not_in"
    assert leaf2.value == {"Person", "Company"}


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
    assert q.to_dict() == {"order_by": "date"}
    q = Query().order_by("date", ascending=False)
    assert q.to_dict() == {"order_by": "-date"}
    # sorting is single-field (the SQL adapter never supported more)
    with pytest.raises(TypeError):
        Query().order_by("date", "name")
    # a builder never mutates the receiver
    q1 = Query().order_by("date")
    q2 = q1.order_by("name")
    assert q1.sort.value == "date" and q2.sort.value == "name"

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
    q = q.aggregate(A(sum=[P("amountEur"), P("amount")]))
    data = q.to_dict()
    assert data["aggregations"] == [
        {"func": "sum", "field": "properties.amount"},
        {"func": "sum", "field": "properties.amountEur"},
    ]
    assert "q" in data


def test_aggregate_params():
    # openaleph metric aggregations: `metric:<func>=<field>`, groups as `facet`;
    # fields take the same spelling as the filter keys
    q = (
        Query()
        .where(M(schema="Payment"))
        .aggregate(A(sum=P("amountEur"), by=P("beneficiary")))
    )
    params = q.to_params()
    assert params["metric:sum"] == ["properties.amountEur"]
    assert params["facet"] == ["properties.beneficiary"]
    assert Query.from_string(q.to_string()).aggregations == q.aggregations

    # ungrouped, multiple funcs / props
    q = Query().aggregate(A(sum=[P("amountEur"), P("amount")]), A(max=P("date")))
    params = q.to_params()
    assert params["metric:sum"] == ["properties.amount", "properties.amountEur"]
    assert params["metric:max"] == ["properties.date"]
    assert "facet" not in params
    assert Query.from_string(q.to_string()).aggregations == q.aggregations

    # parsed straight from openaleph-style params
    q = Query.from_params({"metric:avg": ["properties.amountEur"], "facet": ["year"]})
    assert q.aggregations == set(A(avg=P("amountEur"), by=Year()).aggs)

    # a `facet` with no `metric:` groups an entity count - dropping it would
    # discard the param in silence and answer with empty facets
    q = Query.from_params({"facet": ["dataset"]})
    assert q.aggregations == set(A(count=M("id"), by=M("dataset")).aggs)
    # ... but a bare query still has no aggregations at all
    assert Query.from_params({"filter:schema": ["Payment"]}).aggregations == set()

    # property-type groups are aggregatable under the same name the filter
    # grammar uses (`filter:group.countries=de` / `facet=group.countries`),
    # while a
    # property keeps its prefix - so `topics` stays distinguishable
    q = Query.from_params({"metric:count": ["id"], "facet": ["group.countries"]})
    assert q.aggregations == set(A(count=M("id"), by=G("countries")).aggs)
    q = Query.from_params(
        {"metric:count": ["properties.topics"], "facet": ["group.topics"]}
    )
    assert q.aggregations == set(A(count=P("topics"), by=G("topics")).aggs)
    assert Query.from_string(q.to_string()).aggregations == q.aggregations


def test_rql():
    # nested cross-field OR: M(schema=Person) & (P(name=jane) | G(countries=de))
    q = Query.from_rql(
        "and(eq(schema,Person),or(eq(properties.name,jane),eq(group.countries,de)))"
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
        "and(eq(schema,Person),or(eq(properties.name,jane),eq(group.countries,de)))"
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


def test_rql_aggregations():
    # ungrouped metrics are bare `func(prop)` calls (avg <-> mean)
    assert (
        Query().aggregate(A(sum=P("amountEur"))).to_rql() == "sum(properties.amountEur)"
    )
    assert (
        Query().aggregate(A(avg=P("amountEur"))).to_rql()
        == "mean(properties.amountEur)"
    )
    assert (
        Query().aggregate(A(min=P("date")), A(max=P("date"))).to_rql()
        == "and(max(properties.date),min(properties.date))"
    )

    # grouped metrics batch into one `aggregate(groups..., funcs...)`
    assert (
        Query().aggregate(A(sum=P("amountEur"), by=P("beneficiary"))).to_rql()
        == "aggregate(properties.beneficiary,sum(properties.amountEur))"
    )
    assert (
        Query().aggregate(A(max=P("amountEur"), by=[P("country"), Year()])).to_rql()
        == "aggregate(properties.country,year,max(properties.amountEur))"
    )

    # filter + aggregation sit side by side under the top-level `and`
    q = (
        Query()
        .where(M(schema="Payment"))
        .aggregate(A(count=M("id"), by=P("beneficiary")))
    )
    assert q.to_rql() == (
        "and(eq(schema,Payment),aggregate(properties.beneficiary,count(id)))"
    )

    # a full filter + multi-aggregation query round-trips losslessly
    q = (
        Query()
        .where(M(schema="Payment"), P(date__gte="2023"))
        .aggregate(A(sum=P("amountEur"), by=P("beneficiary")), A(avg=P("amountEur")))
    )
    rt = Query.from_rql(q.to_rql())
    assert rt.aggregations == q.aggregations
    assert rt.to_dict() == q.to_dict()

    # a hand-written aggregate() batches its metrics under the shared group
    q = Query.from_rql(
        "aggregate(properties.beneficiary,sum(properties.amountEur),count(id))"
    )
    assert q.aggregations == set(
        A(sum=P("amountEur"), by=P("beneficiary")).aggs
        + A(count=M("id"), by=P("beneficiary")).aggs
    )

    # an unsupported aggregate operator raises
    with pytest.raises(QueryError):
        Query.from_rql("aggregate(properties.beneficiary,median(properties.amountEur))")


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
    # every context field is expressible: the family is in the spelling
    # (`context.<key>`), so a backend-specific column round-trips like any
    # other field instead of being rejected
    assert Query.from_string(q.to_string()).to_dict() == q.to_dict()
    q = Query().where(C(fragment="x"))
    assert q.to_params() == {"filter:context.fragment": ["x"]}
    assert Query.from_string(q.to_string()).to_dict() == q.to_dict()


def test_select_projection():
    q = Query().where(M(schemata="Document")).select(P("title"), P("fileName"))
    assert q.selection == (PropRef("fileName"), PropRef("title"))
    # a projection is not a filter: it leaves the tree alone
    assert q.q == Query().where(M(schemata="Document")).q

    # only the families addressing the `prop` / `prop_type` column project
    for ref in (M("dataset"), M("id"), C("origin"), Year()):
        with pytest.raises(QueryError):
            Query().select(ref)
    assert Query().select(G("countries")).selection == (GroupRef("countries"),)

    # the wire spelling is the one the filter grammar uses
    assert q.to_dict()["select"] == ["properties.fileName", "properties.title"]
    assert q.to_params()["select"] == ["properties.fileName", "properties.title"]
    assert "select=properties.fileName&select=properties.title" in q.to_string()
    assert q.to_rql() == (
        "and(eq(schemata,Document),select(properties.fileName,properties.title))"
    )

    # ... and round-trips losslessly on all four surfaces, alongside every
    # other part of a query
    q = (
        q.where(P(country="de"))
        .aggregate(A(count=M("id"), by=G("countries")))
        .order_by("-title")[10:20]
    )
    for other in (
        Query.from_dict(q.to_dict()),
        Query.from_params(q.to_params()),
        Query.from_string(q.to_string()),
    ):
        assert other.to_dict() == q.to_dict()
    # rql carries no sort or slice
    assert Query.from_rql(q.to_rql()).selection == q.selection

    # selecting again unions
    assert Query().select(P("title")).select(P("title"), P("name")).selection == (
        PropRef("name"),
        PropRef("title"),
    )
    # an empty selection serializes nothing
    assert "select" not in Query().where(P(name="x")).to_dict()
    assert "select" not in Query().where(P(name="x")).to_params()


def test_select_projects_entities():
    entities = [
        make_entity(
            {
                "id": "e1",
                "schema": "Person",
                "datasets": ["d"],
                "properties": {
                    "name": ["Jane"],
                    "country": ["de"],
                    "notes": ["secret"],
                },
            }
        ),
        make_entity(
            {
                "id": "e2",
                "schema": "Person",
                "datasets": ["d"],
                "properties": {"notes": ["nothing selected"]},
            }
        ),
    ]
    q = Query().where(M(schema="Person")).select(P("name"), G("countries"))
    projected = list(q.apply_iter(iter(entities)))
    # a projection never drops a match, it only narrows what is read
    assert [e.id for e in projected] == ["e1", "e2"]
    assert projected[0].properties == {"name": ["Jane"], "country": ["de"]}
    assert projected[1].properties == {}
    # the caller's entities are cloned, not mutated
    assert entities[0].get("notes") == ["secret"]

    # the projection runs last, so aggregations still see the full entity
    def count_notes(query: Query) -> Any:
        _ = list(query.apply_iter(iter(entities)))
        return dict(query.aggregator.result)

    agg = Query().where(M(schema="Person")).aggregate(A(count=P("notes")))
    assert count_notes(agg)["count"]["properties.notes"] == 2
    assert count_notes(agg.select(P("name"))) == count_notes(agg)
