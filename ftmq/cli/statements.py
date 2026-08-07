"""
`ftmq statements` - work on raw statement streams (csv, json, pack).
"""

import io
from typing import IO, Annotated, Any, BinaryIO, Optional, cast

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_open
from followthemoney.statement import CSV, FORMATS, read_statements, write_statements

from ftmq.statements import DEFAULT_TYPES, cast_types

statements_cli = typer.Typer(no_args_is_help=True)


class KeepOpen(io.RawIOBase):
    """Pass writes through to a handle that outlives this wrapper.

    The csv / pack statement writers wrap the output in a `TextIOWrapper`,
    which closes what it wraps as soon as it is collected - tearing down a
    shared stdout for whatever else runs in the same process. Closing the real
    handle is `smart_open`'s job.
    """

    def __init__(self, fh: IO[bytes]) -> None:
        self.fh = fh

    def writable(self) -> bool:
        return True

    def write(self, data: Any) -> int:
        return self.fh.write(data)

    def flush(self) -> None:
        self.fh.flush()


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
        with smart_open(input_uri, mode="rb") as in_fh:
            statements = read_statements(cast(BinaryIO, in_fh), format=input_format)
            typed = cast_types(statements, types, drop_invalid=drop_invalid)
            with smart_open(output_uri, mode="wb") as out_fh:
                fh = cast(BinaryIO, KeepOpen(cast(IO[bytes], out_fh)))
                write_statements(fh, output_format, typed)
