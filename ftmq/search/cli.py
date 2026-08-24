from typing import Annotated, Iterable, Optional

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_write_json
from anystore.types import SDictGenerator
from anystore.util import model_dump
from pydantic import BaseModel

from ftmq.cli_util import DefaultCmdTyperGroup
from ftmq.search.logic import index, transform
from ftmq.search.settings import Settings
from ftmq.search.store import get_store

settings = Settings()


class SearchDefaultGroup(DefaultCmdTyperGroup):
    """`ftmq search "jane doe"` routes the bare query to the `search` command;
    a bare `ftmq search` shows the help (via `no_args_is_help`)."""

    default_cmd_name = "search"
    insert_default_if_no_args = False


search_cli = typer.Typer(cls=SearchDefaultGroup, no_args_is_help=True)

state = {"uri": settings.uri}


def serialize(items: Iterable[BaseModel]) -> SDictGenerator:
    for item in items:
        yield model_dump(item)


@search_cli.callback()
def cli_search_group(
    uri: Annotated[
        Optional[str], typer.Option(..., help="Search store uri")
    ] = settings.uri,
) -> None:
    """
    Search stores for followthemoney entities
    """
    state["uri"] = uri or settings.uri


@search_cli.command("transform")
def cli_transform(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
) -> None:
    """
    Create search documents from a stream of followthemoney entities
    """
    with ErrorHandler():
        transform(input_uri, output_uri)


@search_cli.command("index")
def cli_index(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
) -> None:
    """
    Index a stream of search documents to a store
    """
    with ErrorHandler():
        index(input_uri, get_store(uri=state["uri"]))


@search_cli.command("search")
def cli_search(
    q: str,
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
) -> None:
    """
    Simple search against the store
    """
    with ErrorHandler():
        store = get_store(uri=state["uri"])
        smart_write_json(output_uri, serialize(store.search(q)))


@search_cli.command("autocomplete")
def cli_autocomplete(
    q: str,
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
) -> None:
    """
    Autocomplete based on entities captions
    """
    with ErrorHandler():
        store = get_store(uri=state["uri"])
        smart_write_json(output_uri, serialize(store.autocomplete(q)))
