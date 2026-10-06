from datetime import datetime, timezone
from functools import cache
from typing import Any, Type

from anystore.types import SDict, StrGenerator
from followthemoney import E, model
from followthemoney.compare import _normalize_names
from followthemoney.dataset import Dataset
from followthemoney.entity import ValueEntity
from followthemoney.names import schema_type_tag
from followthemoney.proxy import EntityProxy
from followthemoney.schema import Schema
from followthemoney.types import registry
from followthemoney.util import make_entity_id, sanitize_text
from normality import latinize_text, slugify, squash_spaces
from rigour.names import NameTypeTag, Symbol, analyze_names
from rigour.territories import lookup_territory
from rigour.text.scripts import can_latinize
from rigour.time import utc_now

from ftmq.types import Entity

DEFAULT_DATASET = "default"
SCOPE_DATASET = "ftmq_scope"


@cache
def make_dataset(name: str | None = DEFAULT_DATASET) -> Dataset:
    name = name or DEFAULT_DATASET
    return Dataset.make({"name": name, "title": name.title()})


def ensure_dataset(ds: str | Dataset | None = None) -> Dataset:
    # not cached: equal (same-named) datasets may differ in their members
    return ds if isinstance(ds, Dataset) else make_dataset(ds)


@cache
def get_scope_dataset(*names: str) -> Dataset:
    """Get a store's read scope over `names`: a single dataset is its own scope,
    several are wrapped in a synthetic `ftmq_scope` collection."""
    if len(names) == 1:
        return make_dataset(names[0])
    # a collection named like a member would absorb that member
    ds = Dataset({"name": SCOPE_DATASET, "datasets": names})
    ds.children = {make_dataset(n) for n in names}
    return ds


def make_entity(
    data: SDict,
    entity_type: Type[E] | None = ValueEntity,
    default_dataset: str | Dataset | None = None,
) -> E:
    """
    Create an `Entity` from a json dict. The input `data` is not mutated.

    Args:
        data: followthemoney data dict that represents entity data.
        entity_type: The entity class to use (`StatementEntity` or `ValueEntity`)
        default_dataset: A default dataset if no dataset in data

    Returns:
        The Entity instance
    """
    etype = entity_type or ValueEntity
    if data.get("id") is None:
        raise ValueError("Entity has no ID.")
    # the entity constructors pop fields off the dict they are given
    data = dict(data)
    if etype == EntityProxy:
        return EntityProxy.from_dict(data)
    if etype == ValueEntity:
        if not data.get("datasets"):
            dataset = make_dataset(default_dataset).name
            data["datasets"] = [dataset]
        return etype.from_dict(data)
    datasets = data.get("datasets", [])
    if len(datasets) == 1:
        dataset = ensure_dataset(datasets[0])
    else:
        dataset = ensure_dataset(default_dataset)
    return etype.from_data(dataset, data)


def ensure_entity(
    data: dict[str, Any] | Entity | EntityProxy,
    entity_type: Type[E],
    default_dataset: str | Dataset | None = None,
) -> E:
    """
    Ensure input data to be of the given `Entity` type.

    Args:
        data: entity or data
        entity_type: The entity class to use (`StatementEntity` or `ValueEntity`)
        default_dataset: A default dataset if no dataset in data

    Returns:
        The Entity instance
    """
    if isinstance(data, entity_type):
        if hasattr(data, "datasets"):
            return data
    if isinstance(data, EntityProxy):
        data = data.to_dict()
    return make_entity(data, entity_type, default_dataset)


def apply_dataset(entity: E, dataset: str | Dataset, replace: bool | None = False) -> E:
    dataset = ensure_dataset(dataset)
    data = entity.to_dict()
    if replace:
        data["datasets"] = [dataset.name]
    else:
        data["datasets"].append(dataset.name)
    return make_entity(data, entity.__class__, dataset)


@cache
def get_country_name(code: str) -> str:
    """
    Get the (english) country name for a 2-letter iso code via
    [rigour.territories](https://rigour.followthemoney.tech/territories/).

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
    territory = lookup_territory(code)
    if territory is not None:
        return territory.name
    return code


@cache
def get_country_code(value: Any, splitter: str | None = ",") -> str | None:
    """
    Get the 2-letter iso country code for an arbitrary country name via
    [rigour.territories](https://rigour.followthemoney.tech/territories/).

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
        splitter: Split the value into tokens to look up if it doesn't match

    Returns:
        The iso code or `None`
    """
    value = clean_string(value)
    if not value:
        return None
    territory = lookup_territory(value)
    if territory is not None:
        return territory.ftm_country
    if splitter:
        for token in value.split(splitter):
            territory = lookup_territory(token.strip())
            if territory is not None:
                return territory.ftm_country
    return None


def join_slug(
    *parts: str | None,
    prefix: str | None = None,
    sep: str = "-",
    strict: bool = True,
    max_len: int = 255,
) -> str | None:
    """
    Create a stable slug from parts.

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
        *parts: Ordered parts to compute the slug from
        prefix: Prefix for the slug
        sep: Parts separator
        strict: Return `None` if any part is empty or `None`
        max_len: Maximum length; a longer slug is cut and gets a hash suffix

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
    Extract the year from an iso date string or `datetime` object.

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


def clean_string(value: Any) -> str | None:
    """
    Convert a value to a sanitized string without linebreaks, or `None` if empty.

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
    return squash_spaces(value)


def clean_name(value: Any) -> str | None:
    """
    Clean a value and return it only if it doesn't consist of special chars only.

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


def make_fingerprint(value: Any) -> str | None:
    """
    Create a stable, simplified string from input to generate ids from.

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


def entity_fingerprints(entity: EntityProxy) -> set[str]:
    """Get the entity's name fingerprints, latinized if possible and with org /
    person tags removed depending on its schema."""
    return make_fingerprints(*entity.names, schemata={entity.schema})


def make_fingerprints(*names: str, schemata: set[Schema] | None = None) -> set[str]:
    """Get the name fingerprints, latinized if possible and with org / person
    tags removed depending on the given schemata."""
    # FIXME private import
    schemata = schemata or {model["LegalEntity"]}
    fps: set[str] = set()
    for schema in schemata:
        fps.update(set(_normalize_names(schema, names)))
    return {latinize_text(fp) if can_latinize(fp) else fp for fp in fps}


def make_string_id(*values: Any) -> str | None:
    """
    Compute a hash id based on values.

    Args:
        *values: Parts to compute id from that will be
            [cleaned][ftmq.util.clean_name]

    Returns:
        The computed hash id or `None` if a part's cleaned value is `None`
    """
    return make_entity_id(*map(clean_name, values))


def make_fingerprint_id(*values: Any) -> str | None:
    """
    Compute a hash id based on the values' fingerprints.

    Args:
        *values: Parts to compute id from that will be
            [fingerprinted][ftmq.util.make_fingerprint]

    Returns:
        The computed hash id or `None` if a part's fingerprint is `None`
    """
    return make_entity_id(*map(make_fingerprint, values))


def get_dehydrated_entity(e: Entity) -> Entity:
    """Reduce an entity to the properties needed to compute its caption."""
    properties: SDict = {}
    for prop in e.schema.caption:
        values = [e.caption] if e.caption else e.get(prop)[:1]
        if values:
            properties = {prop: values}
            break
    data = {"id": e.id, "schema": e.schema.name, "properties": properties}
    return make_entity(data, e.__class__)


def get_featured_entity(e: Entity) -> Entity:
    """Reduce an entity to its caption and featured properties."""
    featured = get_dehydrated_entity(e)
    for prop in e.schema.featured:
        featured.add(prop, e.get(prop))
    return featured


def must_str(value: Any) -> str:
    value = clean_string(value)
    if not value:
        raise ValueError(f"Value invalid: `{value}`")
    return value


SELECT_SYMBOLS = "__symbols__"
SELECT_ANNOTATED = "__annotated__"


def get_name_symbols(schema: Schema, *names: str) -> set[Symbol]:
    """Get the rigour name symbols for the given schema and names."""
    type_tag = schema_type_tag(schema)
    if type_tag in (NameTypeTag.UNK, NameTypeTag.OBJ):
        return set()
    symbols: set[Symbol] = set()
    for name in analyze_names(type_tag, list(names)):
        symbols.update(name.symbols)
    return symbols


def get_symbols(entity: EntityProxy) -> set[Symbol]:
    """Get the rigour name symbols for the given entity."""
    if not entity.schema.is_a("LegalEntity"):
        return set()
    names = entity.get_type_values(registry.name, matchable=True)
    return get_name_symbols(entity.schema, *names)


def inline_symbols(entity: EntityProxy) -> None:
    """Write the entity's rigour name symbols to `indexText`, replacing old ones."""
    for text in entity.pop("indexText"):
        if not text.startswith(SELECT_SYMBOLS):
            entity.add("indexText", text)
    symbols = get_symbols(entity)
    entity.add("indexText", f"{SELECT_SYMBOLS} {','.join(map(str, symbols))}")


def select_data(e: EntityProxy, prefix: str) -> StrGenerator:
    """Select data stored in `indexText` under the given prefix."""
    for text in e.get("indexText", quiet=True):
        if text.startswith(prefix):
            yield text.replace(prefix, "").strip()


def select_symbols(e: EntityProxy) -> set[str]:
    """Select the symbols stored in `indexText`."""
    symbols: set[str] = set()
    for data in select_data(e, SELECT_SYMBOLS):
        symbols.update(data.split(","))
    return symbols


def select_annotations(e: EntityProxy) -> set[str]:
    """Select the annotations stored in `indexText`."""
    return {s for s in select_data(e, SELECT_ANNOTATED)}


def iso_datetime(v: str | datetime | None) -> datetime | None:
    """
    Parse an ISO datetime string into an aware UTC `datetime`.

    A naive value is assumed to be UTC, an offset is converted to UTC.
    Unlike `rigour.time.iso_datetime`, microseconds are kept.

    Examples:
        >>> iso_datetime("2024-01-15T10:30:00")
        datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> iso_datetime("2024-01-15T10:30:00.123456")
        datetime(2024, 1, 15, 10, 30, 0, 123456, tzinfo=timezone.utc)
        >>> iso_datetime(None)
        None

    Args:
        v: An ISO datetime string, a `datetime` or `None`

    Returns:
        An aware datetime in UTC, or `None` for empty input
    """
    if not v:
        return None
    if isinstance(v, str):
        v = datetime.fromisoformat(v)
    if v.tzinfo is None:
        # astimezone on a naive datetime would assume local time, not utc
        return v.replace(tzinfo=timezone.utc)
    return v.astimezone(timezone.utc)


def datetime_iso(v: datetime | str | None, default_now: bool = False) -> str | None:
    """
    Ensure a UTC ISO datetime string from an arbitrary value.

    A naive `datetime` is assumed to be UTC, an aware one is converted to UTC; a
    string is passed through unchanged.

    Examples:
        >>> datetime_iso(datetime(2024, 1, 15, 10, 30))
        "2024-01-15T10:30:00+00:00"
        >>> datetime_iso("2024-01-15")
        "2024-01-15"
        >>> datetime_iso(None)
        None

    Args:
        v: A `datetime`, an ISO string, or `None`
        default_now: Return the current UTC timestamp for empty input instead
            of `None`

    Returns:
        The ISO datetime string, or `None`
    """
    if not v:
        if default_now:
            return utc_now().isoformat()
        return None

    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        else:
            v = v.astimezone(timezone.utc)
        return v.isoformat()
    return v
