"""
`ftmq store` - read nomenklatura statement stores.
"""

from typing import Annotated

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_write
from nomenklatura import settings as nk_settings

from ftmq.io import smart_write_proxies
from ftmq.store import get_store

store_cli = typer.Typer(no_args_is_help=True)


@store_cli.command("list-datasets")
def cli_store_list_datasets(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = nk_settings.DB_URL,
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
) -> None:
    """
    List datasets within a store
    """
    with ErrorHandler():
        store = get_store(input_uri)
        catalog = store.get_scope()
        datasets = [ds.name for ds in catalog.datasets]
        smart_write(output_uri, "\n".join(datasets).encode() + b"\n")


@store_cli.command("iterate")
def cli_store_iterate(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="store input uri")
    ] = nk_settings.DB_URL,
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
) -> None:
    """
    Iterate all entities from in to out
    """
    with ErrorHandler():
        store = get_store(input_uri)
        smart_write_proxies(output_uri, store.iterate())
