from functools import lru_cache
from typing import Any, Generator, Type

import pycountry
from anystore.functools import weakref_cache as cache
from anystore.types import SDict, StrGenerator
from banal import ensure_list, is_listish
from followthemoney import E, model
from followthemoney.compare import _normalize_names
from followthemoney.dataset import Dataset
from followthemoney.entity import ValueEntity
from followthemoney.proxy import EntityProxy
from followthemoney.schema import Schema
from followthemoney.types import registry
from followthemoney.util import make_entity_id, sanitize_text
from normality import collapse_spaces, latinize_text, slugify
from rigour.names import Name, Symbol, tag_org_name, tag_person_name
from rigour.names.tokenize import normalize_name
from rigour.text.scripts import can_latinize

from ftmq.enums import Comparators
from ftmq.types import Entity

DEFAULT_DATASET = "default"


@cache
def make_dataset(name: str | None = DEFAULT_DATASET) -> Dataset:
    name = name or DEFAULT_DATASET
    return Dataset.make({"name": name, "title": name.title()})


@cache
def ensure_dataset(ds: str | Dataset | None = None) -> Dataset:
    if not ds:
        return make_dataset()
    if isinstance(ds, str):
        return make_dataset(ds)
    return ds


@cache
def get_scope_dataset(*names: str) -> Dataset:
    ds = Dataset({"name": "default", "datasets": names})
    ds.children = {make_dataset(n) for n in names}
    return ds


def parse_comparator(key: str) -> tuple[str, Comparators]:
    key, *comparator = key.split("__", 1)
    if comparator:
        comparator = Comparators[comparator[0]]
    else:
        comparator = Comparators["eq"]
    return key, comparator


def parse_unknown_filters(
    filters: tuple[str, ...],
) -> Generator[tuple[str, str | list[str], str], None, None]:
    _filters = (f for f in filters)
    for prop in _filters:
        prop = prop.lstrip("-")
        if "=" in prop:  # 'country=de'
            prop, value = prop.split("=")
        else:  # ("country", "de"):
            value = next(_filters)

        prop, *op = prop.split("__")
        op = op[0] if op else Comparators.eq
        if op == Comparators["in"]:
            # de,fr or ["de", "fr"]
            if is_listish(value):
                value = ensure_list(value)
            else:
                value = value.split(",")
        yield prop, value, op


def make_entity(
    data: SDict,
    entity_type: Type[E] | None = ValueEntity,
    default_dataset: str | Dataset | None = None,
) -> E:
    """
    Create a `Entity` from a json dict.

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
    Ensure input data to be specified `Entity` type

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


def entity_fingerprints(entity: EntityProxy) -> set[str]:
    """Get the set of entity name fingerprints, latinized if the alphabet allows
    it and with org / person tags removed depending on entity schema"""
    return make_fingerprints(*entity.names, schemata={entity.schema})


def make_fingerprints(*names: str, schemata: set[Schema] | None = None) -> set[str]:
    """Get the set of name fingerprints, latinized if the alphabet allows
    it and with org / person tags removed depending on given schemata"""
    # FIXME private import
    schemata = schemata or {model["LegalEntity"]}
    fps: set[str] = set()
    for schema in schemata:
        fps.update(set(_normalize_names(schema, names)))
    # add latinized if appropriate
    return {latinize_text(fp) if can_latinize(fp) else fp for fp in fps}


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


def get_entity_caption_property(e: Entity) -> SDict:
    """Get the minimal properties dict required to compute the caption"""
    for prop in e.schema.caption:
        if e.caption:
            return {prop: [e.caption]}
        for value in e.get(prop):
            return {prop: [value]}
    return {}


def get_dehydrated_entity(e: Entity) -> Entity:
    """
    Reduce an Entity to only its property dict that is needed to compute the
    caption.
    """
    data = {
        "id": e.id,
        "schema": e.schema.name,
        "properties": get_entity_caption_property(e),
    }
    return make_entity(data, e.__class__)


def get_featured_entity(e: Entity) -> Entity:
    """
    Reduce an Entity with only its featured properties
    """
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
    """Get the rigour names symbols for the given schema and list of names"""
    symbols: set[Symbol] = set()
    if schema.is_a("Person"):
        taggers = [tag_person_name]
    elif schema.is_a("Organization"):
        taggers = [tag_org_name]
    elif schema.is_a("LegalEntity"):
        taggers = [tag_org_name, tag_person_name]
    else:
        return symbols
    for name in names:
        n = Name(name)
        for tagger in taggers:
            for symbol in tagger(n, normalize_name).symbols:
                symbols.add(symbol)
    return symbols


def get_symbols(entity: EntityProxy) -> set[Symbol]:
    """Get the rigour names symbols for the given entity"""
    if not entity.schema.is_a("LegalEntity"):
        return set()
    names = entity.get_type_values(registry.name, matchable=True)
    return get_name_symbols(entity.schema, *names)


def inline_symbols(entity: EntityProxy) -> None:
    """Get the rigour names symbols for the given entity and write them to `indexText`"""
    # clean up old symbols from indexText:
    for text in entity.pop("indexText"):
        if not text.startswith(SELECT_SYMBOLS):
            entity.add("indexText", text)
    symbols = get_symbols(entity)
    entity.add("indexText", f"{SELECT_SYMBOLS} {','.join(map(str, symbols))}")


def select_data(e: EntityProxy, prefix: str) -> StrGenerator:
    """Select arbitrary stored data in `indexText` identified by given prefix"""
    for text in e.get("indexText", quiet=True):
        if text.startswith(prefix):
            yield text.replace(prefix, "").strip()


def select_symbols(e: EntityProxy) -> set[str]:
    """Select stored symbols in `indexText`"""
    symbols: set[str] = set()
    for data in select_data(e, SELECT_SYMBOLS):
        symbols.update(data.split(","))
    return symbols


def select_annotations(e: EntityProxy) -> set[str]:
    """Select stored annotations in `indexText`"""
    return {s for s in select_data(e, SELECT_ANNOTATED)}
