import re
from functools import cache, lru_cache
from typing import Any, Generator

import pycountry
from banal import ensure_list, is_listish
from followthemoney.proxy import E, EntityProxy
from followthemoney.schema import Schema
from followthemoney.types import registry
from followthemoney.util import make_entity_id, sanitize_text
from nomenklatura.dataset import Dataset
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.statement import Statement
from nomenklatura.stream import StreamEntity
from normality import collapse_spaces, slugify

from ftmq.enums import Comparators
from ftmq.exceptions import ValidationError
from ftmq.types import SE, SGenerator


@cache
def make_dataset(name: str) -> Dataset:
    return Dataset.make({"name": name, "title": name})


@cache
def ensure_dataset(ds: str | Dataset | None = None) -> Dataset | None:
    if not ds:
        return
    if isinstance(ds, str):
        return make_dataset(ds)
    return ds


DefaultDataset = make_dataset("default")


def parse_comparator(key: str) -> tuple[str, Comparators]:
    key, *comparator = key.split("__", 1)
    if comparator:
        comparator = Comparators[comparator[0]]
    else:
        comparator = Comparators["eq"]
    return key, comparator


def parse_unknown_filters(
    filters: tuple[str],
) -> Generator[tuple[str, str, str], None, None]:
    filters = (f for f in filters)
    for prop in filters:
        prop = prop.lstrip("-")
        if "=" in prop:  # 'country=de'
            prop, value = prop.split("=")
        else:  # ("country", "de"):
            value = next(filters)

        prop, *op = prop.split("__")
        op = op[0] if op else Comparators.eq
        if op == Comparators["in"]:
            # de,fr or ["de", "fr"]
            if is_listish(value):
                value = ensure_list(value)
            else:
                value = value.split(",")
        yield prop, value, op


def make_proxy(data: dict[str, Any], dataset: str | Dataset | None = None) -> CE:
    """
    Create a `nomenklatura.entity.CompositeEntity` from a json dict.

    Args:
        data: followthemoney data dict that represents entity data.
        dataset: A default dataset

    Returns:
        The composite entity proxy
    """
    datasets = ensure_list(data.pop("datasets", None))
    if dataset is not None:
        if isinstance(dataset, str):
            dataset = make_dataset(dataset)
        datasets.append(dataset.name)
    elif datasets:
        dataset = datasets[0]
        dataset = make_dataset(dataset)
    else:
        dataset = DefaultDataset
    proxy = CompositeEntity(dataset, data)
    if len(datasets) > 1:
        if proxy.id is None:
            raise ValidationError("Entity has no ID.")
        statements = get_statements(proxy, *datasets)
        return CompositeEntity.from_statements(dataset, statements)
    return proxy


def ensure_proxy(data: dict[str, Any] | CE | E | SE) -> CompositeEntity:
    if isinstance(data, CompositeEntity):
        return data
    if isinstance(data, (EntityProxy, StreamEntity)):
        data = data.to_full_dict()
    return make_proxy(data)


def get_statements(proxy: CE, *datasets: str) -> SGenerator:
    """
    Get statements from a `nomenklatura.entity.CompositeEntity` with multiple
    datasets if needed

    Args:
        proxy: `nomenklatura.entity.CompositeEntity`
        *datasets: Any (additional) datasets to create statements for

    Yields:
        A generator of `nomenklatura.statement.Statement`
    """
    datasets = datasets or ("default",)
    for ix, dataset in enumerate(datasets):
        for sx, stmt in enumerate(Statement.from_entity(proxy, dataset)):
            stmt = stmt.to_dict()
            stmt["external"] = stmt.get("external") or False
            stmt = Statement.from_dict(stmt)
            yield stmt
            if ix and sx:
                break


@cache
def get_country_name(code: str) -> str:
    """
    Get the (english) country name for the given 2-letter iso code via
    [pycountry](https://pypi.org/project/pycountry/)

    Examples:
        >>> get_country_name("de")
        "Germany"
        >>> get_country_name("xx")
        "xx"
        >>> get_country_name("gb") == get_country_name("uk")
        True  # United Kingdom

    Args:
        code: Two-letter iso code, case insensitive

    Returns:
        Either the country name for a valid code or the code as fallback.
    """
    code_clean = get_country_code(code)
    if code_clean is None:
        code_clean = code.lower()
    try:
        country = pycountry.countries.get(alpha_2=code_clean)
        if country is not None:
            return country.name
    except (LookupError, AttributeError):
        return code
    return code_clean


@lru_cache(1024)
def get_country_code(value: Any, splitter: str | None = ",") -> str | None:
    """
    Get the 2-letter iso country code for an arbitrary country name

    Examples:
        >>> get_country_code("Germany")
        "de"
        >>> get_country_code("Deutschland")
        "de"
        >>> get_country_code("Berlin, Deutschland")
        "de"
        >>> get_country_code("Foo")
        None

    Args:
        value: Any input that will be [cleaned][ftmq.util.clean_string]
        splitter: Character to use to get text tokens to find country name for

    Returns:
        The iso code or `None`
    """
    value = clean_string(value)
    if not value:
        return
    code = registry.country.clean_text(value)
    if code:
        return code
    for token in value.split(splitter):
        code = registry.country.clean_text(token)
        if code:
            return code
    return


NUMERIC_US = re.compile(r"^-?\d+(?:,\d{3})*(?:\.\d+)?$")
NUMERIC_DE = re.compile(r"^-?\d+(?:\.\d{3})*(?:,\d+)?$")


def to_numeric(value: str) -> float | int | None:
    """
    Convert a string value into a primitive numeric dtype (`int` or `float`)
    taking US and DE formatting into account via regex

    Examples:
        >>> to_numeric("1")
        1
        >>> to_numeric("1.0")
        1
        >>> to_numeric("1.1")
        1.1
        >>> to_numeric("1,101,000")
        1101000
        >>> to_numeric("1.000,1")
        1000.1
        >>> to_numeric("foo")
        None

    Args:
        value: The input

    Returns:
        The converted number or `None` if conversion fails
    """
    value = str(value).strip()
    try:
        value = float(value)
        if int(value) == value:
            return int(value)
        return value
    except ValueError:
        if re.match(NUMERIC_US, value):
            return to_numeric(value.replace(",", ""))
        if re.match(NUMERIC_DE, value):
            return to_numeric(value.replace(".", "").replace(",", "."))


def join_slug(
    *parts: str | None,
    prefix: str | None = None,
    sep: str = "-",
    strict: bool = True,
    max_len: int = 255,
) -> str | None:
    """
    Create a stable slug from parts with optional validation

    Examples:
        >>> join_slug("foo", "bar")
        "foo-bar"
        >>> join_slug("foo", None, "bar")
        None
        >>> join_slug("foo", None, "bar", strict=False)
        "foo-bar"
        >>> join_slug("foo", "bar", sep="_")
        "foo_bar"
        >>> join_slug("a very long thing", max_len=15)
        "a-very-5c156cf9"

    Args:
        *parts: Multiple (ordered) parts to compute the slug from
        prefix: Add a prefix to the slug
        sep: Parts separator
        strict: Ensure all parts are not `None`
        max_len: Maximum length of the slug. If it exceeds, the returned value
            will get a computed hash suffix

    Returns:
        The computed slug or `None` if validation fails
    """
    sections = [slugify(p, sep=sep) for p in parts]
    if strict and None in sections:
        return None
    texts = [p for p in sections if p is not None]
    if not len(texts):
        return None
    prefix = slugify(prefix, sep=sep)
    if prefix is not None:
        texts = [prefix, *texts]
    slug = sep.join(texts)
    if len(slug) <= max_len:
        return slug
    # shorten slug but ensure uniqueness
    ident = make_entity_id(slug)[:8]
    slug = slug[: max_len - 9].strip(sep)
    return f"{slug}-{ident}"


def get_year_from_iso(value: Any) -> int | None:
    """
    Extract the year from a iso date string or `datetime` object.

    Examples:
        >>>  get_year_from_iso(None)
        None
        >>>  get_year_from_iso("2023")
        2023
        >>>  get_year_from_iso(2020)
        2020
        >>>  get_year_from_iso(datetime.now())
        2024
        >>>  get_year_from_iso("2000-01")
        2000

    Args:
        value: Any input that will be [cleaned][ftmq.util.clean_string]

    Returns:
        The year or `None`
    """
    value = clean_string(value)
    if not value:
        return
    try:
        return int(str(value)[:4])
    except ValueError:
        return


@lru_cache(1024)
def clean_string(value: Any) -> str | None:
    """
    Convert a value to `None` or a sanitized string without linebreaks

    Examples:
        >>> clean_string(" foo\n bar")
        "foo bar"
        >>> clean_string("foo Bar, baz")
        "foo Bar, baz"
        >>> clean_string(None)
        None
        >>> clean_string("")
        None
        >>> clean_string("  ")
        None
        >>> clean_string(100)
        "100"

    Args:
        value: Any input that will be converted to string

    Returns:
        The cleaned value or `None`
    """
    value = sanitize_text(value)
    if value is None:
        return
    return collapse_spaces(value)


@lru_cache(1024)
def clean_name(value: Any) -> str | None:
    """
    Clean a value and only return it if it is a "name" in the sense of, doesn't
    contain exclusively of special chars

    Examples:
        >>> clean_name("  foo\n Bar")
        "foo Bar"
        >>> clean_name("- - . *")
        None

    Args:
        value: Any input that will be [cleaned][ftmq.util.clean_string]

    Returns:
        The cleaned name or `None`
    """
    value = clean_string(value)
    if slugify(value) is None:
        return
    return value


@lru_cache(1024)
def make_fingerprint(value: Any) -> str | None:
    """
    Create a stable but simplified string or `None` from input that can be used
    to generate ids (to mimic `fingerprints.generate` which is unstable for IDs
    as its algorithm could change)

    Examples:
        >>> make_fingerprint("Mrs. Jane Doe")
        "doe jane mrs"
        >>> make_fingerprint("Mrs. Jane Mrs. Doe")
        "doe jane mrs"
        >>> make_fingerprint("#")
        None
        >>> make_fingerprint(" ")
        None
        >>> make_fingerprint("")
        None
        >>> make_fingerprint(None)
        None

    Args:
        value: Any input that will be [cleaned][ftmq.util.clean_name]

    Returns:
        The simplified string (fingerprint) or `None` if value is not feasible
            to fingerprint.
    """
    value = clean_name(value)
    if value is None:
        return
    return " ".join(sorted(set(slugify(value).split("-"))))


@lru_cache(1024)
def make_string_id(*values: Any) -> str | None:
    """
    Compute a hash id based on values

    Args:
        *values: Parts to compute id from that will be
            [cleaned][ftmq.util.clean_name]

    Returns:
        The computed hash id or `None` if a parts cleaned value is `None`
    """
    return make_entity_id(*map(clean_name, values))


@lru_cache(1024)
def make_fingerprint_id(*values: Any) -> str | None:
    """
    Compute a hash id based on values fingerprints

    Args:
        *values: Parts to compute id from that will be
            [fingerprinted][ftmq.util.make_fingerprint]

    Returns:
        The computed hash id or `None` if a parts fingerprinted value is `None`
    """
    return make_entity_id(*map(make_fingerprint, values))


@cache
def prop_is_numeric(schema: Schema, prop: str) -> bool:
    """
    Indicate if the given property is numeric type

    Args:
        schema: followthemoney schema
        prop: Property

    Returns:
        `False` if the property is not numeric type or not found in the schema
            at all
    """
    prop_ = schema.get(prop)
    if prop_ is not None:
        return prop_.type == registry.number
    return False


def get_proxy_caption_property(proxy: CE) -> dict[str, str]:
    for prop in proxy.schema.caption:
        for value in proxy.get(prop):
            return {prop: value}
    return {}


def get_dehydrated_proxy(proxy: CE) -> CE:
    """
    Reduce proxy payload to only include caption property

    Args:
        proxy: `nomenklatura.entity.CompositeEntity`

    Returns:
        A `nomenklatura.entity.CompositeEntity` with only the caption property.
    """
    return make_proxy(
        {
            "id": proxy.id,
            "schema": proxy.schema.name,
            "properties": get_proxy_caption_property(proxy),
            "datasets": proxy.datasets,
        }
    )


def get_featured_proxy(proxy: CE) -> CE:
    """
    Reduce proxy payload to only include featured properties

    Args:
        proxy: `nomenklatura.entity.CompositeEntity`

    Returns:
        A `nomenklatura.entity.CompositeEntity` with only the featured
            properties for its schema.
    """
    featured = get_dehydrated_proxy(proxy)
    for prop in proxy.schema.featured:
        featured.add(prop, proxy.get(prop))
    return featured


def must_str(value: Any) -> str:
    value = clean_string(value)
    if not value:
        raise ValueError(f"Value invalid: `{value}`")
    return value
