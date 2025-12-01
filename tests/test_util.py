import sys
from datetime import datetime

import cloudpickle
import pytest
from followthemoney import DefaultDataset, EntityProxy, model
from followthemoney.dataset import Dataset
from followthemoney.entity import ValueEntity
from followthemoney.statement.entity import StatementEntity

from ftmq import util
from ftmq.enums import Comparators, StrEnum

if sys.version_info >= (3, 11):
    from enum import EnumType


def test_util_make_dataset():
    ds = util.make_dataset("test")
    assert isinstance(ds, Dataset)
    assert ds.to_dict() == {
        "name": "test",
        "title": "Test",
        "tags": [],
        "resources": [],
        "children": [],
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
    assert ds == DefaultDataset


def test_util_str_enum():
    enum = StrEnum("Foo", ["a", "b", 2])
    assert enum.a == "a"
    assert str(enum.a) == "a"
    assert "a" in enum
    if sys.version_info >= (3, 11):
        assert isinstance(enum, EnumType)

        # https://gist.github.com/simonwoerpel/bdb9959de75e550349961677549624fb
        enum = StrEnum("Foo", ["name", "name2"])
        assert "name" in enum.__dict__
        dump = cloudpickle.dumps(enum)
        assert isinstance(dump, bytes)
        enum2 = cloudpickle.loads(dump)
        assert enum2 == enum


def test_util_unknown_filters():
    res = (("country", "de", Comparators.eq), ("name", "alice", Comparators.eq))
    args = ("--country", "de", "--name", "alice")
    assert tuple(util.parse_unknown_filters(args)) == res
    args = ("--country=de", "--name=alice")
    assert tuple(util.parse_unknown_filters(args)) == res
    args = ("--country", "de", "--name=alice")
    assert tuple(util.parse_unknown_filters(args)) == res
    args = ()
    assert tuple(util.parse_unknown_filters(args)) == ()

    args = ("--country", "de", "--year__gte", "2023")
    res = (("country", "de", Comparators.eq), ("year", "2023", Comparators.gte))
    assert tuple(util.parse_unknown_filters(args)) == res


def test_util_parse_lookup_key():
    assert util.parse_comparator("foo") == ("foo", Comparators.eq)
    assert util.parse_comparator("foo__gte") == ("foo", Comparators.gte)
    with pytest.raises(KeyError):  # unknown operator
        util.parse_comparator("foo__bar")


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


def test_util_prop_is_numeric():
    assert not util.prop_is_numeric(model.get("Person"), "name")
    assert util.prop_is_numeric(model.get("Payment"), "amountEur")


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
    assert "[NAME:1682564]" in symbols
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
