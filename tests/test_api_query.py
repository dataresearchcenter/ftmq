import pytest

pytest.importorskip("fastapi")

from fastapi import HTTPException, Request  # noqa: E402

from ftmq.query import QueryError  # noqa: E402
from ftmq.query.aggregations import Agg  # noqa: E402


def _request(query_string: str) -> Request:
    return Request(
        {"type": "http", "query_string": query_string.encode(), "headers": []}
    )


def _build(query_string: str, authenticated: bool = False):
    from ftmq.api.query import build_query

    return build_query(_request(query_string), authenticated)


def test_api_query_defaults():
    q = _build("")
    assert q.to_dict() == {"limit": 100, "offset": 0}


def test_api_query_filters():
    q = _build("filter:dataset=donations")
    assert q.dataset_names == {"donations"}
    assert q.limit == 100 and q.offset == 0

    q = _build("filter:schema=Payment")
    assert q.schemata_names == {"Payment"}

    # is-a expansion via schemata
    q = _build("filter:schemata=LegalEntity")
    assert "Company" in q.schemata_names

    # properties, ranges, substring / prefix ops
    q = _build(
        "filter:properties.name=Jane"
        "&filter:gte:properties.date=2023"
        "&filter:ilike:properties.name=jane"
        "&filter:startswith:canonical_id=eu-"
    )
    leaves = {(x.family, x.key, str(x.comparator)) for x in q.q.iter_leaves()}
    assert ("P", "name", "eq") in leaves
    assert ("P", "date", "gte") in leaves
    assert ("P", "name", "ilike") in leaves
    assert ("M", "canonical_id", "startswith") in leaves

    # reverse lookup via the `entities` group
    q = _build("filter:entities=some-id")
    assert {g.key for g in q.groups} == {"entities"}

    # countries group (feeds the search stores)
    q = _build("filter:countries=de&filter:countries=lu")
    assert q.countries == {"de", "lu"}


def test_api_query_sort_paging():
    q = _build("sort=name:desc&limit=10&offset=20")
    assert q.sort.serialize() == ["-name"]
    assert q.limit == 10 and q.offset == 20

    # limit is capped for unauthenticated requests
    q = _build("limit=500")
    assert q.limit == 100
    q = _build("limit=500", authenticated=True)
    assert q.limit == 500


def test_api_query_aggregations():
    q = _build("metric:sum=amountEur&metric:count=id&facet=year")
    assert q.aggregations == {
        Agg(func="sum", prop="amountEur", groups=("year",)),
        Agg(func="count", prop="id", groups=("year",)),
    }


def test_api_query_invalid():
    with pytest.raises(HTTPException) as e:
        _build("filter:dataset=not_existent")
    assert e.value.status_code == 422

    with pytest.raises(QueryError):
        _build("filter:properties.foo=bar")
