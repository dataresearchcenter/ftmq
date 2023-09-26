import pytest

from ftmq.exceptions import ValidationError
from ftmq.query import Query


def test_query():
    q = Query()
    assert q.lookups == q.to_dict() == {}
    assert not q

    q = q.where(dataset="test")
    assert q.lookups == q.to_dict() == {"dataset": "test"}
    assert q
    fi = list(q.filters)[0]
    assert fi.key == "dataset"
    assert fi.value == "test"
    assert fi.to_dict() == {"dataset": "test"}

    q = q.where(schema="Event")
    assert q.lookups == q.to_dict() == {"dataset": "test", "schema": "Event"}

    q = q.where(prop="date", value=2023)
    assert len(q.filters) == 3
    assert (
        q.lookups
        == q.to_dict()  # noqa
        == {  # noqa
            "dataset": "test",
            "schema": "Event",
            "date": "2023",
        }
    )

    # multi dataset / schema
    q2 = Query().where(dataset__in=["d1", "d2"])
    assert q2.lookups == q2.to_dict() == {"dataset__in": {"d1", "d2"}}
    q2 = q2.where(schema="Event").where(schema__in=["Person", "Organization"])
    assert (
        q2.lookups
        == q2.to_dict()  # noqa
        == {  # noqa
            "dataset__in": {"d1", "d2"},
            "schema": "Event",
            "schema__in": {"Organization", "Person"},
        }
    )
    assert q2.dataset_names == {"d1", "d2"}
    q2 = q2.where(dataset=None, schema=None).where(dataset="test")
    assert q2.lookups == q2.to_dict() == {"dataset": "test"}
    assert q2.dataset_names == {"test"}

    q = q.where(prop="date", value=2023, comparator="gte")
    assert len(q.filters) == 3
    assert (
        q.lookups
        == q.to_dict()  # noqa
        == {  # noqa
            "dataset": "test",
            "schema": "Event",
            "date__gte": "2023",
        }
    )

    q = Query().where(prop="name", value=["test", "other"], comparator="in")
    assert q.to_dict() == {
        "name__in": {"test", "other"},
    }
    # filter uniqueness
    q = Query().where(dataset="test").where(dataset="test")
    assert len(q.filters) == 1
    q = Query().where(dataset="test").where(dataset="other")
    assert len(q.filters) == 2
    q = Query().where(prop="date", value=2023)
    assert len(q.filters) == 1
    q = q.where(prop="date", value=2023, comparator="gte")
    assert len(q.filters) == 1
    q = q.where(prop="date", value=2024)
    assert len(q.filters) == 2
    q = q.where(prop="startDate", value=2024)
    assert len(q.filters) == 3

    q = Query().order_by("date")
    assert q.to_dict() == {"order_by": ["date"]}
    assert q.lookups == {}
    q = Query().order_by("date", "name")
    assert q.to_dict() == {"order_by": ["date", "name"]}
    q = Query().order_by("date", ascending=False)
    assert q.to_dict() == {"order_by": ["-date"]}

    q = Query()[10]
    assert q.slice == slice(10, 11, None)
    q = Query()[:10]
    assert q.slice == slice(None, 10, None)
    q = Query()[1:10]
    assert q.slice == slice(1, 10, None)
    assert q.to_dict() == {"limit": 9, "offset": 1}

    with pytest.raises(ValidationError):
        Query().where(foo="bar")
    with pytest.raises(ValidationError):
        Query().where(schema="foo")
    with pytest.raises(ValidationError):
        Query().where(prop="foo")
    with pytest.raises(ValidationError):
        Query().where(prop="foo", value="bar")
    with pytest.raises(ValidationError):
        Query().where(prop="date", value=2023, comparator="foo")
    with pytest.raises(ValidationError):
        Query()[-1]
    with pytest.raises(ValidationError):
        Query()[1:1:1]
    with pytest.raises(ValidationError):
        Query().where(dataset=[1, 2, 3])


def test_query_cast():
    q = Query().where(prop="name", value="test", comparator="in")
    f = list(q.filters)[0]
    assert f.value == {"test"}
    q = Query().where(prop="date", value=2023)
    f = list(q.filters)[0]
    assert f.value == "2023"


def test_query_arbitrary_kwargs():
    q = Query().where(date__gte=2023, name__ilike="%jane%")
    assert q.to_dict() == {"date__gte": "2023", "name__ilike": "%jane%"}


def test_query_aggregate():
    q = Query().where(schema="Payment", date__gte=2023, amount__null=False)
    q = q.aggregate("sum", "amountEur", "amount")
    assert q.to_dict() == {
        "schema": "Payment",
        "date__gte": "2023",
        "amount__null": False,
        "aggregations": {"sum": {"amount", "amountEur"}},
    }


def test_query_reversed():
    q = Query().where(schema="Payment", date__gte=2023, amount__null=False)
    q = q.where(reverse="my_id")
    q = q.aggregate("sum", "amountEur", "amount")
    assert q.to_dict() == {
        "reverse": "my_id",
        "schema": "Payment",
        "date__gte": "2023",
        "amount__null": False,
        "aggregations": {"sum": {"amount", "amountEur"}},
    }


def test_query_filter_comparators():
    q = Query().where(dataset__in=["a", "b"])
    assert q.to_dict() == {"dataset__in": {"a", "b"}}

    q = Query().where(dataset__startswith="a")
    assert q.to_dict() == {"dataset__startswith": "a"}


def test_query_search():
    q = Query().where(dataset="test", schema="Event", date__gte=2023)
    q = q.search("meeting")
    assert q.to_dict() == {
        "date__gte": "2023",
        "dataset": "test",
        "schema": "Event",
        "search": {
            "firstName__ilike": "meeting",
            "middleName__ilike": "meeting",
            "lastName__ilike": "meeting",
            "name__ilike": "meeting",
        },
    }
    q = q.search("brussels", ["location"])
    assert q.to_dict() == {
        "date__gte": "2023",
        "dataset": "test",
        "schema": "Event",
        "search": {
            "location__ilike": "brussels",
        },
    }
