"""
Cast statement values into the canonical format of their property type.

`cast_types` normalizes `number` and `date` values: the parsed value goes into
`value`, the raw string into `original_value` (unless already set), and the
statement `id` is regenerated. Statement stores apply this on write, as the SQL
backends `CAST` the `value` column. Existing dumps are migrated via the CLI:

```bash
cat statements.csv | ftmq statements cast-types > statements.typed.csv
```
"""

from typing import Callable, Iterable, Iterator, TypeAlias

from anystore.logging import get_logger
from followthemoney import registry
from followthemoney.statement import Statement

log = get_logger(__name__)

Caster: TypeAlias = Callable[[str], str | None]


def cast_number(value: str) -> str | None:
    """Get the canonical numeric form of a value, or `None` if it doesn't parse.

    Kept as a string (no float precision loss); a unit suffix is dropped
    (`"5 kg"` -> `"5"`).
    """
    number, _unit = registry.number.parse(value)
    return number


def cast_date(value: str) -> str | None:
    """Get the canonical ISO (prefix) form of a date value, or `None` if it doesn't
    parse. Partial dates (`"2021"`, `"2021-06"`) are kept."""
    return registry.date.clean_text(value)


CASTERS: dict[str, Caster] = {
    registry.number.name: cast_number,
    registry.date.name: cast_date,
}
DEFAULT_TYPES: tuple[str, ...] = tuple(CASTERS)


def get_casters(types: Iterable[str]) -> dict[str, Caster]:
    """Get the caster per property type, keyed as `Statement.prop_type`."""
    casters: dict[str, Caster] = {}
    for type_ in types:
        caster = CASTERS.get(type_)
        if caster is None:
            raise ValueError(
                f"Invalid cast type: `{type_}` - one of ({', '.join(DEFAULT_TYPES)})"
            )
        casters[type_] = caster
    return casters


def cast_statement(
    stmt: Statement, casters: dict[str, Caster] | None = None
) -> Statement | None:
    """Cast a statement's value into the canonical format of its property type.

    Args:
        stmt: The statement to cast
        casters: Caster per property type (default: `number`, `date`)

    Returns:
        The statement, cloned if its value changed, or `None` if the value
            doesn't parse
    """
    caster = (CASTERS if casters is None else casters).get(stmt.prop_type)
    if caster is None:
        return stmt
    value = caster(stmt.value)
    if value is None:
        log.warning(
            f"Invalid `{stmt.prop_type}` value: `{stmt.value}`",
            entity_id=stmt.entity_id,
            prop=stmt.prop,
        )
        return None
    if value == stmt.value:
        return stmt
    # `clone` regenerates the id, which is a content hash over the value
    return stmt.clone(value=value, original_value=stmt.original_value or stmt.value)


def cast_types(
    statements: Iterable[Statement],
    types: Iterable[str] = DEFAULT_TYPES,
    drop_invalid: bool = False,
) -> Iterator[Statement]:
    """Normalize the values of the given property types in a statement stream.

    Args:
        statements: Stream of statements
        types: Property types to cast (default: `number`, `date`)
        drop_invalid: Drop statements whose value doesn't parse instead of
            passing them through unchanged

    Returns:
        A generator of `Statement` instances
    """
    # validate eagerly, before a consumer has written any output
    return _cast_types(statements, get_casters(types), drop_invalid)


def _cast_types(
    statements: Iterable[Statement],
    casters: dict[str, Caster],
    drop_invalid: bool,
) -> Iterator[Statement]:
    invalid = 0
    for stmt in statements:
        casted = cast_statement(stmt, casters)
        if casted is None:
            invalid += 1
            if not drop_invalid:
                yield stmt
            continue
        yield casted
    if invalid:
        log.warning(
            f"{invalid} invalid values, {'dropped' if drop_invalid else 'unchanged'}"
        )
