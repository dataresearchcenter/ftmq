import sys
from datetime import datetime

import cloudpickle
import pytest
from followthemoney import model
from nomenklatura.dataset import Dataset
from nomenklatura.entity import CompositeEntity

from ftmq import util
from ftmq.enums import Comparators, StrEnum

if sys.version_info >= (3, 11):
    from enum import EnumType


def test_util_make_dataset():
    ds = util.make_dataset("Test")
    assert isinstance(ds, Dataset)
    assert ds.to_dict() == {
        "name": "Test",
        "title": "Test",
        "tags": [],
        "resources": [],
    }


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


def test_util_numeric():
    assert util.to_numeric("1") == 1
    assert util.to_numeric("1.0") == 1
    assert util.to_numeric("1.1") == 1.1
    assert util.to_numeric("1,101,000") == 1_101_000
    assert util.to_numeric("1.000,1") == 1000.1
    assert util.to_numeric("foo") is None


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

    assert util.clean_string(" foo\n bar") == "foo bar"
    assert util.clean_string("foo Bar, baz") == "foo Bar, baz"
    assert util.clean_string(None) is None
    assert util.clean_string("") is None
    assert util.clean_string("  ") is None
    assert util.clean_string(100) == "100"

    assert util.clean_name("  foo\n bar") == "foo bar"
    assert util.clean_name("- - . *") is None

    assert util.make_fingerprint("Mrs. Jane Doe") == "doe jane mrs"
    assert util.make_fingerprint("Mrs. Jane Mrs. Doe") == "doe jane mrs"
    assert util.make_fingerprint("#") is None
    assert util.make_fingerprint(" ") is None
    assert util.make_fingerprint("") is None
    assert util.make_fingerprint(None) is None


def test_util_prop_is_numeric():
    assert not util.prop_is_numeric(model.get("Person"), "name")
    assert util.prop_is_numeric(model.get("Payment"), "amountEur")


def test_util_ensure_proxy():
    data = {
        "id": "org",
        "schema": "LegalEntity",
        "properties": {"name": ["Test"]},
    }
    assert isinstance(util.ensure_proxy(data), CompositeEntity)
    assert isinstance(util.ensure_proxy(model.get_proxy(data)), CompositeEntity)
    assert isinstance(util.ensure_proxy(util.make_proxy(data)), CompositeEntity)
