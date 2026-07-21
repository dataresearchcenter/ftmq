import pytest

from ftmq import A, Query, QueryError
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
    # keyword form: `func=prop`, multi-prop via a list, grouping via `by=`
    assert A(sum="amountEur").aggs == (Agg("sum", "amountEur"),)
    assert set(A(sum=["amountEur", "amount"]).aggs) == {
        Agg("sum", "amountEur"),
        Agg("sum", "amount"),
    }
    assert A(count="id", by="beneficiary").aggs == (
        Agg("count", "id", ("beneficiary",)),
    )
    # several functions in one node
    assert set(A(min="amountEur", max="amountEur").aggs) == {
        Agg("min", "amountEur"),
        Agg("max", "amountEur"),
    }
    # `A` is not a filter node - `Query.aggregate` collects its specs
    q = Query().aggregate(A(sum="amountEur"), A(max="date"))
    assert q.aggregations == {Agg("sum", "amountEur"), Agg("max", "date")}


def test_agg_make_agg_validation():
    assert make_agg("sum", "amountEur") == Agg("sum", "amountEur")
    with pytest.raises(QueryError):
        make_agg("notafunc", "amountEur")
    with pytest.raises(QueryError):
        make_agg("sum", "notaprop")
    with pytest.raises(QueryError):
        A()  # empty: no func=prop pair


def test_agg_values(donations):
    res = _run(
        A(sum="amountEur", min="amountEur", max="amountEur", avg="amountEur").aggs,
        donations,
    )
    assert res["sum"]["amountEur"] == 40589689.15
    assert res["min"]["amountEur"] == 50000
    assert res["max"]["amountEur"] == 2334526
    assert res["avg"]["amountEur"] == 139964.44534482757

    assert _run(A(min="date").aggs, donations)["min"]["date"] == "2002-07-04"
    assert _run(A(count="country").aggs, donations)["count"]["country"] == 4


def test_multiple_aggs(donations):
    # `Query.aggregate` is variadic: several `A` nodes in one call ...
    q = Query().aggregate(
        A(sum="amountEur"),
        A(count="id"),
        A(max="date"),
    )
    # ... and it also accumulates across chained calls
    q = q.aggregate(A(min="date"))
    assert q.aggregations == {
        Agg("sum", "amountEur"),
        Agg("count", "id"),
        Agg("max", "date"),
        Agg("min", "date"),
    }
    res = _run(q.aggregations, donations)
    assert res["sum"]["amountEur"] == 40589689.15
    assert res["count"]["id"] == 474
    assert res["max"]["date"] == "2011-12-29"
    assert res["min"]["date"] == "2002-07-04"


def test_agg_reuse_no_leak(donations):
    # the specs are immutable and a fresh Aggregator holds all state, so
    # applying the same specs twice never double-counts
    aggs = A(sum="amountEur").aggs
    first = _run(aggs, donations)
    second = _run(aggs, donations)
    assert first == second == {"sum": {"amountEur": 40589689.15}}


def test_agg_groupby(donations):
    res = _run(A(count="name", by="country").aggs, donations)
    assert res == {
        "count": {"name": 95},
        "groups": {
            "country": {"count": {"name": {"de": 80, "cy": 1, "gb": 1, "lu": 1}}}
        },
    }


def test_agg_groupby_meta(donations):
    res = _run(A(count="id", by="schema").aggs, donations)
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

    res = _run(A(count="id", by="year").aggs, donations)
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


def test_agg_serialization():
    aggs = A(sum="amountEur", by="beneficiary").aggs + A(count="id").aggs
    data = aggregations_to_dict(aggs)
    assert data == {
        "sum": {"amountEur"},
        "count": {"id"},
        "groups": {"beneficiary": {"sum": {"amountEur"}}},
    }
    # round-trips, restoring each spec's groups from the nested `groups` mapping
    assert aggregations_from_dict(data) == set(aggs)
