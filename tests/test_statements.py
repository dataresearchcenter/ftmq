import pytest
from followthemoney import Statement

from ftmq.statements import (
    cast_date,
    cast_number,
    cast_statement,
    cast_types,
    get_casters,
)


def _stmt(prop: str, value: str, **kwargs) -> Statement:
    return Statement(
        entity_id="pay-1",
        prop=prop,
        schema="Payment",
        value=value,
        dataset="d",
        **kwargs,
    )


def test_statements_cast_number():
    assert cast_number("1,000.50") == "1000.50"
    assert cast_number("2000") == "2000"
    # a unit is dropped (it survives in `original_value`)
    assert cast_number("5 kg") == "5"
    # kept as a string, not a float: `to_number` would round this one
    assert cast_number("1234567890123456789") == "1234567890123456789"
    # what the followthemoney parser rejects as ambiguous stays unparseable
    assert cast_number("1.000,00") is None
    assert cast_number("not-a-number") is None


def test_statements_cast_date():
    assert cast_date("2023-01-01") == "2023-01-01"
    assert cast_date("2023") == "2023"  # partial dates are kept
    assert cast_date("2023-13-45") is None
    assert cast_date("1.1.2021") is None


def test_statements_cast_types():
    stmts = [
        _stmt("amountEur", "1,000.50"),
        _stmt("date", "2023-01-01"),
        _stmt("purpose", "1,000.50"),  # not a number / date property: untouched
        _stmt("amountEur", "not-a-number"),
    ]
    res = list(cast_types(stmts))
    assert [s.value for s in res] == [
        "1000.50",
        "2023-01-01",
        "1,000.50",
        "not-a-number",
    ]
    # the raw value moves into `original_value` for changed statements only
    assert [s.original_value for s in res] == ["1,000.50", None, None, None]
    # the id is a content hash over the value, so a changed value changes it
    assert res[0].id != stmts[0].id
    assert res[1].id == stmts[1].id

    # an existing source value is not overwritten
    stmt = _stmt("amountEur", "1,000.50", original_value="ein tausend")
    assert list(cast_types([stmt]))[0].original_value == "ein tausend"

    # unparseable values can be dropped instead of passed through
    assert len(list(cast_types(stmts, drop_invalid=True))) == 3

    # restricting the types leaves the others alone
    res = list(cast_types([_stmt("amountEur", "1,000.50"), _stmt("date", "1.1.2021")]))
    assert [s.value for s in res] == ["1000.50", "1.1.2021"]
    res = list(cast_types([_stmt("amountEur", "1,000.50")], types=["date"]))
    assert [s.value for s in res] == ["1,000.50"]


def test_statements_cast_statement():
    # the single-statement helper the store writers use
    assert cast_statement(_stmt("amountEur", "1,000.50")).value == "1000.50"
    assert cast_statement(_stmt("purpose", "1,000.50")).value == "1,000.50"
    assert cast_statement(_stmt("amountEur", "not-a-number")) is None


def test_statements_get_casters():
    assert set(get_casters(["number", "date"])) == {"number", "date"}
    with pytest.raises(ValueError, match="Invalid cast type"):
        get_casters(["names"])
    # validation happens eagerly, before a consumer pulls the first statement
    with pytest.raises(ValueError, match="Invalid cast type"):
        cast_types([], types=["foo"])
