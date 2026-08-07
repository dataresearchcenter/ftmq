"""
`ftmq fragments` - read entity fragment stores.
"""

from datetime import datetime
from typing import Annotated, Optional

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_write, smart_write_json

from ftmq.io import smart_write_proxies
from ftmq.store.fragments import get_fragments
from ftmq.store.fragments import get_store as get_fragments_store
from ftmq.store.fragments.settings import Settings as FragmentsSettings

fragments_cli = typer.Typer(no_args_is_help=True)

fragments_settings = FragmentsSettings()


@fragments_cli.command("list-datasets")
def cli_fragments_list_datasets(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = fragments_settings.database_uri,
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
) -> None:
    """
    List datasets within a fragments store
    """
    with ErrorHandler():
        store = get_fragments_store(input_uri)
        datasets = [ds.name for ds in store.all()]
        smart_write(output_uri, "\n".join(datasets).encode() + b"\n")


@fragments_cli.command("iterate")
def cli_fragments_iterate(
    dataset: Annotated[
        str, typer.Option("-d", "--dataset", help="Dataset name to iterate")
    ],
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="fragments store input uri")
    ] = fragments_settings.database_uri,
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
    schema: Annotated[
        Optional[str], typer.Option("-s", "--schema", help="Filter by schema")
    ] = None,
    since: Annotated[
        Optional[str],
        typer.Option(
            "--since",
            help="Filter by timestamp (since), ISO format: YYYY-MM-DDTHH:MM:SS",
        ),
    ] = None,
    until: Annotated[
        Optional[str],
        typer.Option(
            "--until",
            help="Filter by timestamp (until), ISO format: YYYY-MM-DDTHH:MM:SS",
        ),
    ] = None,
) -> None:
    """
    Iterate all entities from a fragments dataset
    """
    with ErrorHandler():
        fragments = get_fragments(dataset, database_uri=input_uri)

        # Parse timestamp strings to datetime objects
        since_dt = datetime.fromisoformat(since) if since else None
        until_dt = datetime.fromisoformat(until) if until else None

        smart_write_proxies(
            output_uri, fragments.iterate(schema=schema, since=since_dt, until=until_dt)
        )


@fragments_cli.command("iterate-fragments")
def cli_fragments_iterate_fragments(
    dataset: Annotated[
        str, typer.Option("-d", "--dataset", help="Dataset name to iterate")
    ],
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="fragments store input uri")
    ] = fragments_settings.database_uri,
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
) -> None:
    """
    Iterate all fragments from a dataset, unsorted and not aggregated. Useful
    for streaming into another storage that does dedupe by itself.
    """
    with ErrorHandler():
        fragments = get_fragments(dataset, database_uri=input_uri)
        smart_write_json(
            output_uri, fragments.fragments(sort=False, include_fragment=True)
        )
