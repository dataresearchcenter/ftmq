"""
`ftmq statements` - work on raw statement streams (csv, json, pack).
"""

from typing import Annotated, Optional

import typer
from anystore.cli import ErrorHandler
from anystore.logging import get_logger
from followthemoney.statement import CSV, FORMATS
from nomenklatura import settings as nk_settings

from ftmq.io import smart_read_statements, smart_write_statements
from ftmq.statements import DEFAULT_TYPES, cast_types

log = get_logger(__name__)
statements_cli = typer.Typer(no_args_is_help=True)


def _ensure_format(value: str) -> str:
    if value in FORMATS:
        return value
    typer.secho(
        f"Invalid format: `{value}` - one of ({', '.join(FORMATS)})",
        fg=typer.colors.RED,
        err=True,
    )
    raise typer.Exit(1)


@statements_cli.command("cast-types")
def cli_statements_cast_types(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
    input_format: Annotated[
        str, typer.Option("--input-format", help="csv, json or pack")
    ] = CSV,
    output_format: Annotated[
        str, typer.Option("--output-format", help="csv, json or pack")
    ] = CSV,
    type: Annotated[
        Optional[list[str]],
        typer.Option("-t", "--type", help="Property type(s) to cast"),
    ] = None,
    drop_invalid: Annotated[
        bool,
        typer.Option("--drop-invalid", help="Drop statements that don't parse"),
    ] = False,
) -> None:
    """
    Cast statement values into the canonical format of their property type
    (`number`, `date`), moving the raw value into the `original_value` column.

    This is the format the read side (in particular the SQL backends) assumes,
    as followthemoney doesn't normalize these types on write.
    """
    # argument validation stays outside `ErrorHandler`, which swallows the
    # `typer.Exit` it would otherwise turn into an exit code of 0
    types = list(type or DEFAULT_TYPES)
    input_format = _ensure_format(input_format)
    output_format = _ensure_format(output_format)
    with ErrorHandler():
        statements = smart_read_statements(input_uri, format=input_format)
        typed = cast_types(statements, types, drop_invalid=drop_invalid)
        smart_write_statements(output_uri, typed, format=output_format)


@statements_cli.command("read")
def cli_statements_read(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="store uri")
    ] = nk_settings.DB_URL,
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
    output_format: Annotated[
        str, typer.Option("--output-format", help="csv, json or pack")
    ] = CSV,
    dataset: Annotated[
        Optional[str], typer.Option("-d", "--dataset", help="Dataset to limit scope to")
    ] = None,
) -> None:
    """
    Dump the raw statements of a store into a statement stream.

    The rows come back as stored, external statements included and in no
    particular order, so the dump loads back verbatim via `ftmq statements
    write`. Only the SQL family of backends can do this.
    """
    output_format = _ensure_format(output_format)
    with ErrorHandler():
        statements = smart_read_statements(input_uri, dataset=dataset)
        smart_write_statements(output_uri, statements, format=output_format)


@statements_cli.command("write")
def cli_statements_write(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="store uri")
    ] = nk_settings.DB_URL,
    input_format: Annotated[
        str, typer.Option("--input-format", help="csv, json or pack")
    ] = CSV,
) -> None:
    """
    Load a statement stream into a store.

    Each statement keeps the `canonical_id` it carries - it is never
    re-derived from a resolver - so a stream stamped by `nomenklatura
    apply-statements` stays resolved. A SQL store upserts on the statement id,
    so loading a dump back into the store it came from updates it in place.
    """
    input_format = _ensure_format(input_format)
    with ErrorHandler():
        statements = smart_read_statements(input_uri, format=input_format)
        count = smart_write_statements(output_uri, statements)
        log.info(f"Wrote `{count}` statements.", uri=output_uri)
