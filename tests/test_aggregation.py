import pytest

from ftmq import A, C, G, M, P, Query, QueryError, Year
from ftmq.query.aggregations import (
    Agg,
    Aggregator,
    aggregations_from_dict,
    aggregations_to_dict,
    make_agg,
)


def _run(aggs, proxies):
    agg = Aggregator(aggs)
    _ = list(agg.apply(proxies))
    return agg.result


def test_agg_node():
    # keyword form: `func=<ref>`, multi-field via a list, grouping via `by=`
    assert A(sum=P("amountEur")).aggs == (Agg("sum", P("amountEur")),)
    assert set(A(sum=[P("amountEur"), P("amount")]).aggs) == {
        Agg("sum", P("amountEur")),
        Agg("sum", P("amount")),
    }
    assert A(count=M("id"), by=P("beneficiary")).aggs == (
        Agg("count", M("id"), (P("beneficiary"),)),
    )
    # several functions in one node
    assert set(A(min=P("amountEur"), max=P("amountEur")).aggs) == {
        Agg("min", P("amountEur")),
        Agg("max", P("amountEur")),
    }
    # `A` is not a filter node - `Query.aggregate` collects its specs
    q = Query().aggregate(A(sum=P("amountEur")), A(max=P("date")))
    assert q.aggregations == {Agg("sum", P("amountEur")), Agg("max", P("date"))}


def test_agg_refs():
    # a field is addressed by the same family marker the filter grammar uses,
    # called with a bare field name instead of `field=value`
    assert P("amountEur").wire == "properties.amountEur"
    assert G("countries").wire == "group.countries"
    assert M("dataset").wire == "dataset"
    assert Year().wire == "year"
    # `topics` is both a property and a property-type group - a name alone
    # cannot say which, the marker can
    assert P("topics") != G("topics")
    assert P("topics").wire == "properties.topics"
    assert G("topics").wire == "group.topics"


def test_agg_make_agg_validation():
    assert make_agg("sum", P("amountEur")) == Agg("sum", P("amountEur"))
    with pytest.raises(QueryError):
        make_agg("notafunc", P("amountEur"))
    with pytest.raises(QueryError):
        P("notaprop")
    with pytest.raises(QueryError):
        A()  # empty: no func=<ref> pair
    # a bare string does not say which family it belongs to
    with pytest.raises(QueryError, match="expected a field reference"):
        A(sum="amountEur")


def test_agg_values(donations):
    res = _run(
        A(
            sum=P("amountEur"),
            min=P("amountEur"),
            max=P("amountEur"),
            avg=P("amountEur"),
        ).aggs,
        donations,
    )
    # results are keyed by the wire spelling of the field
    assert res["sum"]["properties.amountEur"] == 40589689.15
    assert res["min"]["properties.amountEur"] == 50000
    assert res["max"]["properties.amountEur"] == 2334526
    assert res["avg"]["properties.amountEur"] == 139964.44534482757

    assert _run(A(min=P("date")).aggs, donations)["min"]["properties.date"] == (
        "2002-07-04"
    )
    assert (
        _run(A(count=P("country")).aggs, donations)["count"]["properties.country"] == 4
    )


def test_agg_property_type_groups(proxies):
    # a property-type group is aggregatable via `G`, the same family the filter
    # grammar uses, so `filter:countries=` and `facet=countries` address the
    # same dimension. The two fixtures carry their countries on *different*
    # properties (donations on `country`, eu_authorities on `jurisdiction`),
    # so the group spans both while either property alone does not.
    assert make_agg("count", G("countries")) == Agg("count", G("countries"))
    assert _run(A(count=G("countries")).aggs, proxies)["count"]["group.countries"] == 5
    assert _run(A(count=P("country")).aggs, proxies)["count"]["properties.country"] == 4
    assert (
        _run(A(count=P("jurisdiction")).aggs, proxies)["count"][
            "properties.jurisdiction"
        ]
        == 1
    )

    res = _run(A(count=M("id"), by=G("countries")).aggs, proxies)
    assert res["groups"]["group.countries"]["count"]["id"] == {
        "eu": 151,
        "de": 163,
        "cy": 2,
        "gb": 3,
        "lu": 2,
    }


def test_multiple_aggs(donations):
    # `Query.aggregate` is variadic: several `A` nodes in one call ...
    q = Query().aggregate(
        A(sum=P("amountEur")),
        A(count=M("id")),
        A(max=P("date")),
    )
    # ... and it also accumulates across chained calls
    q = q.aggregate(A(min=P("date")))
    assert q.aggregations == {
        Agg("sum", P("amountEur")),
        Agg("count", M("id")),
        Agg("max", P("date")),
        Agg("min", P("date")),
    }
    res = _run(q.aggregations, donations)
    assert res["sum"]["properties.amountEur"] == 40589689.15
    assert res["count"]["id"] == 474
    assert res["max"]["properties.date"] == "2011-12-29"
    assert res["min"]["properties.date"] == "2002-07-04"


def test_agg_reuse_no_leak(donations):
    # the specs are immutable and a fresh Aggregator holds all state, so
    # applying the same specs twice never double-counts
    aggs = A(sum=P("amountEur")).aggs
    first = _run(aggs, donations)
    second = _run(aggs, donations)
    assert first == second == {"sum": {"properties.amountEur": 40589689.15}}


def test_agg_groupby(donations):
    res = _run(A(count=P("name"), by=P("country")).aggs, donations)
    assert res == {
        "count": {"properties.name": 95},
        "groups": {
            "properties.country": {
                "count": {"properties.name": {"de": 80, "cy": 1, "gb": 1, "lu": 1}}
            }
        },
    }


def test_agg_groupby_meta(donations):
    res = _run(A(count=M("id"), by=M("schema")).aggs, donations)
    assert res["count"]["id"] == 474
    assert res["groups"]["schema"]["count"]["id"] == {
        "Payment": 290,
        "Address": 89,
        "Organization": 17,
        "Company": 56,
        "Person": 22,
    }
    # every id belongs to exactly one schema
    assert sum(res["groups"]["schema"]["count"]["id"].values()) == res["count"]["id"]

    res = _run(A(count=M("id"), by=Year()).aggs, donations)
    assert res["groups"]["year"]["count"]["id"] == {
        "2011": 21,
        "2003": 20,
        "2004": 20,
        "2009": 46,
        "2008": 49,
        "2010": 28,
        "2007": 28,
        "2006": 27,
        "2002": 16,
        "2005": 35,
    }


def test_agg_context():
    # the context family is aggregatable too - nothing special-cases it, it is
    # just another ref. Context keys are open-ended, so they carry a prefix on
    # the wire instead of competing with the other families
    assert C("origin").wire == "context.origin"
    assert C("bucket").wire == "context.bucket"
    assert make_agg("count", C("origin")).key == "context.origin"
    assert aggregations_to_dict(A(count=C("origin")).aggs) == [
        {"func": "count", "field": "context.origin"}
    ]


def test_agg_serialization():
    # one spec per entry, fields spelled as on the wire, `by` only when grouped
    aggs = A(sum=P("amountEur"), by=P("beneficiary")).aggs + A(count=M("id")).aggs
    data = aggregations_to_dict(aggs)
    assert data == [
        {"func": "count", "field": "id"},
        {
            "func": "sum",
            "field": "properties.amountEur",
            "by": ["properties.beneficiary"],
        },
    ]
    assert aggregations_from_dict(data) == set(aggs)

    # groups are sorted at construction, so input order does not matter
    assert A(count=M("id"), by=[Year(), G("countries")]).aggs == (
        A(count=M("id"), by=[G("countries"), Year()]).aggs
    )
