from datetime import datetime, timedelta, timezone

import pytest
from followthemoney import EntityProxy, model
from followthemoney.dataset import Dataset
from followthemoney.entity import ValueEntity
from followthemoney.statement.entity import StatementEntity

from ftmq import util
from ftmq.query.exceptions import QueryError
from ftmq.query.leaves import parse_lookup
from ftmq.query.refs import NUMERIC_PROPS


def test_util_make_dataset():
    ds = util.make_dataset("test")
    assert isinstance(ds, Dataset)
    assert ds.to_dict() == {
        "name": "test",
        "title": "Test",
        "tags": [],
        "resources": [],
        "children": [],
        "deprecated": False,
    }


def test_util_ensure_dataset():
    ds = util.ensure_dataset("test")
    assert isinstance(ds, Dataset)
    assert ds.name == "test"

    ds = util.ensure_dataset(ds)
    assert isinstance(ds, Dataset)
    assert ds.name == "test"

    ds = util.ensure_dataset()
    assert isinstance(ds, Dataset)
    assert ds.name == "default"


def test_util_get_scope_dataset():
    ds = util.get_scope_dataset("donations", "eu_authorities")
    assert ds.leaf_names == {"donations", "eu_authorities"}

    # a single dataset is its own scope - no synthetic collection to collide
    assert util.get_scope_dataset("default") is util.make_dataset("default")

    # regression: the scope collection used to be named "default" too, and
    # followthemoney identifies datasets by name - so a member called
    # "default" (the conventional store-side name) was absorbed by its own
    # parent and dropped from `leaf_names`, emptying the scope of every
    # dataset-filtered read (`View.get_entity` -> 404 for every entity)
    assert util.get_scope_dataset("default").leaf_names == {"default"}
    assert util.get_scope_dataset("default", "other").leaf_names == {
        "default",
        "other",
    }


def test_util_ensure_dataset_identity():
    # regression: `ensure_dataset` used to be cached, and followthemoney
    # compares datasets by name - so two scopes spanning different datasets
    # (both named `ftmq_scope`) collided and the second store silently read
    # through the first one's scope
    a = util.get_scope_dataset("a", "b")
    b = util.get_scope_dataset("c", "d")
    assert a == b  # same name, so followthemoney considers them equal ...
    assert util.ensure_dataset(a) is a  # ... but each is passed through as-is
    assert util.ensure_dataset(b) is b


def test_util_parse_lookup_key():
    assert parse_lookup("foo") == ("foo", "eq")
    assert parse_lookup("foo__gte") == ("foo", "gte")
    with pytest.raises(QueryError):  # unknown operator
        parse_lookup("foo__bar")
    with pytest.raises(QueryError):  # `between` never worked and is gone
        parse_lookup("foo__between")


def test_util_country():
    assert util.get_country_name("de") == "Germany"
    assert util.get_country_name("xx") == "xx"
    assert util.get_country_code("Germany") == "de"
    assert util.get_country_code("Deutschland") == "de"
    assert util.get_country_code("Berlin, Deutschland") == "de"
    assert util.get_country_code("Foo") is None
    assert util.get_country_code("uk") == "gb"


def test_util_get_year():
    assert util.get_year_from_iso(None) is None
    assert util.get_year_from_iso("2023") == 2023
    assert util.get_year_from_iso(2020) == 2020
    assert util.get_year_from_iso(datetime.now()) >= 2023
    assert util.get_year_from_iso("2000-01") == 2000


def test_util_clean():
    assert util.clean_string(" foo\n bar") == "foo bar"
    assert util.clean_string("foo Bar, baz") == "foo Bar, baz"
    assert util.clean_string(None) is None
    assert util.clean_string("") is None
    assert util.clean_string("  ") is None
    assert util.clean_string(100) == "100"

    assert util.clean_name("  foo\n bar") == "foo bar"
    assert util.clean_name("- - . *") is None


def test_util_fingerprints():
    assert util.make_fingerprint("Mrs. Jane Doe") == "doe jane mrs"
    assert util.make_fingerprint("Mrs. Jane Mrs. Doe") == "doe jane mrs"
    assert util.make_fingerprint("#") is None
    assert util.make_fingerprint(" ") is None
    assert util.make_fingerprint("") is None
    assert util.make_fingerprint(None) is None

    fps = {"doe jane", "mrs. jane doe"}
    assert util.make_fingerprints("Mrs. Jane Doe", schemata={model["Person"]}) == fps
    entity = util.make_entity(
        {"id": "jane", "schema": "Person", "properties": {"name": ["Mrs. Jane Doe"]}}
    )
    assert util.entity_fingerprints(entity) == fps

    assert util.make_fingerprints("Українська") == {"ukraí̈nsʹka"}
    assert util.make_fingerprints("乌克兰语") == {"乌克兰语"}


def test_util_numeric_props():
    assert "name" not in NUMERIC_PROPS
    assert "amountEur" in NUMERIC_PROPS


def test_util_ensure_entity():
    data = {
        "id": "org",
        "schema": "LegalEntity",
        "properties": {"name": ["Test"]},
    }
    # from dict
    entity = util.ensure_entity(data, StatementEntity)
    assert isinstance(entity, StatementEntity)
    assert entity.datasets == {"default"}
    entity = util.ensure_entity(data, StatementEntity, "foo")
    assert entity.datasets == {"foo"}
    # from EntityProxy
    entity = util.ensure_entity(model.get_proxy(data), StatementEntity)
    assert isinstance(entity, StatementEntity)
    assert entity.datasets == {"default"}
    entity = util.ensure_entity(model.get_proxy(data), StatementEntity, "foo")
    assert entity.datasets == {"foo"}
    # dict -> ValueEntity
    entity = util.ensure_entity(data, ValueEntity)
    assert isinstance(entity, ValueEntity)
    assert entity.datasets == {"default"}
    entity = util.ensure_entity(data, ValueEntity, "foo")
    assert entity.datasets == {"foo"}
    # ValueEntity -> StatementEntity
    sentity = util.ensure_entity(entity, StatementEntity)
    assert sentity.datasets == {"foo"}


def test_util_apply_entity():
    data = {
        "id": "org",
        "schema": "LegalEntity",
        "properties": {"name": ["Test"]},
    }
    entity = util.make_entity(data, entity_type=ValueEntity)
    assert entity.datasets == {"default"}
    entity = util.apply_dataset(entity, "foo")
    assert entity.datasets == {"default", "foo"}
    entity = util.apply_dataset(entity, "foo", replace=True)
    assert entity.datasets == {"foo"}

    entity = util.make_entity(data, entity_type=StatementEntity)
    assert entity.datasets == {"default"}
    entity = util.apply_dataset(entity, "foo")
    assert entity.datasets == {"foo"}
    entity = util.apply_dataset(entity, "foo", replace=True)
    assert entity.datasets == {"foo"}


def test_util_symbols():
    entity = util.make_entity(
        {"id": "j", "schema": "Person", "properties": {"name": ["Jane Doe"]}}
    )
    symbols = map(str, util.get_symbols(entity))
    assert "[NAME:Q1682564]" in symbols
    entity = util.make_entity(
        {
            "id": "Q1234",
            "schema": "Company",
            "properties": {
                "name": ["Gazprom Bank OOO"],
            },
        }
    )
    symbols = list(map(str, util.get_symbols(entity)))
    assert "[ORGCLS:LLC]" in symbols
    assert "[DOMAIN:BANK]" in symbols

    entity.add("indexText", "foo")
    util.inline_symbols(entity)
    symbols = list(map(str, util.select_symbols(entity)))
    assert "[ORGCLS:LLC]" in symbols
    assert "[DOMAIN:BANK]" in symbols
    assert "foo" in entity.get("indexText")

    # no symbols for e.g. mention entity (invalid indexText prop)
    entity = util.make_entity({"id": "m1", "schema": "Mention"})
    assert util.select_symbols(entity) == set()


def test_util_iso_datetime():
    assert util.iso_datetime(None) is None
    assert util.iso_datetime("") is None
    assert util.iso_datetime("2024-01-15T10:30:00") == datetime(
        2024, 1, 15, 10, 30, tzinfo=timezone.utc
    )
    # unlike rigour.time.iso_datetime, microseconds survive
    assert util.iso_datetime("2024-01-15T10:30:00.123456") == datetime(
        2024, 1, 15, 10, 30, 0, 123456, tzinfo=timezone.utc
    )
    # an explicit offset is converted to UTC (unlike rigour, which drops it)
    assert util.iso_datetime("2024-01-15T12:00:00+02:00") == datetime(
        2024, 1, 15, 10, 0, tzinfo=timezone.utc
    )


def test_util_datetime_iso():
    # naive datetimes are assumed UTC, aware ones converted
    assert (
        util.datetime_iso(datetime(2024, 1, 15, 10, 30)) == "2024-01-15T10:30:00+00:00"
    )
    cest = timezone(timedelta(hours=2))
    assert (
        util.datetime_iso(datetime(2024, 1, 15, 12, 0, tzinfo=cest))
        == "2024-01-15T10:00:00+00:00"
    )
    # strings pass through unchanged
    assert util.datetime_iso("2024-01-15") == "2024-01-15"
    # empty input defaults to the current UTC timestamp unless disabled
    assert util.datetime_iso(None, default_now=False) is None
    now = util.iso_datetime(util.datetime_iso(None))
    assert now is not None
    assert now.tzinfo == timezone.utc
    # round-trip with iso_datetime
    assert (
        util.datetime_iso(util.iso_datetime("2024-01-15T10:30:00.123456"))
        == "2024-01-15T10:30:00.123456+00:00"
    )


def test_util_make_entity():
    entity = util.make_entity(
        {"id": "j", "schema": "Person", "properties": {"name": ["Jane Doe"]}}
    )
    assert entity.__class__ == ValueEntity
    assert entity.to_dict()
    assert entity.to_full_dict()

    entity = util.make_entity(
        {"id": "j", "schema": "Person", "properties": {"name": ["Jane Doe"]}},
        EntityProxy,
    )
    assert entity.__class__ == EntityProxy
    assert entity.to_dict()
    assert entity.to_full_dict()

    entity = util.make_entity(
        {"id": "j", "schema": "Person", "properties": {"name": ["Jane Doe"]}},
        StatementEntity,
    )
    assert entity.__class__ == StatementEntity
    assert entity.to_dict()
    assert entity.to_full_dict()
