from typing import Annotated, Optional

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_write_json, smart_write_model
from anystore.logging import configure_logging, get_logger
from anystore.settings import Settings
from followthemoney import ValueEntity
from rich import print
from typer.main import get_group

from ftmq import __version__
from ftmq.aggregate import aggregate
from ftmq.cli.dataset import catalog_cli, dataset_cli
from ftmq.cli.fragments import fragments_cli
from ftmq.cli.statements import statements_cli
from ftmq.cli.store import store_cli
from ftmq.cli_util import DefaultCmdTyperGroup
from ftmq.io import smart_get_store, smart_read_proxies, smart_write_proxies
from ftmq.model.stats import Collector
from ftmq.query import M, Query
from ftmq.search.cli import search_cli
from ftmq.util import apply_dataset

log = get_logger(__name__)
settings = Settings()

cli = typer.Typer(cls=DefaultCmdTyperGroup, pretty_exceptions_enable=settings.debug)


@cli.callback(invoke_without_command=True)
def cli_ftmq(
    version: Annotated[Optional[bool], typer.Option(..., help="Show version")] = False,
) -> None:
    if version:
        print(__version__)
        raise typer.Exit()
    configure_logging()


@cli.command("q")
def cli_q(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
    dataset: Annotated[
        Optional[list[str]],
        typer.Option("-d", "--dataset", help="Dataset(s) to filter for"),
    ] = None,
    query: Annotated[
        Optional[list[str]],
        typer.Option(
            "-q",
            "--query",
            help="Aleph query string, e.g. "
            "'filter:schema=Person&filter:group.countries=de'",
        ),
    ] = None,
    rql: Annotated[
        Optional[list[str]],
        typer.Option(
            "--rql",
            help="RQL query string (nested & | ~), e.g. "
            "'and(eq(schema,Person),or(eq(group.countries,de),eq(group.countries,at)))'",
        ),
    ] = None,
    stats: Annotated[
        bool,
        typer.Option("--stats", help="Output coverage statistics of the result"),
    ] = False,
) -> None:
    """
    Filter entities from a json stream or a store.

    Several `-q` / `--rql` strings AND together. Writes the matching entities, or
    instead their coverage statistics (`--stats`) or the result of an aggregating
    query.
    """
    with ErrorHandler():
        # each string is a whole query (aggregations, select, sort, slice), later wins
        parsed = [Query.from_string(value) for value in query or ()]
        parsed += [Query.from_rql(value) for value in rql or ()]
        q = Query(
            aggregations={a for sub in parsed for a in sub.aggregations},
            selection={ref for sub in parsed for ref in sub.selection},
            sort=next((sub.sort for sub in reversed(parsed) if sub.sort), None),
            slice=next((sub.slice for sub in reversed(parsed) if sub.slice), None),
            facet_sort=next(
                (sub.facet_sort for sub in reversed(parsed) if sub.facet_sort), None
            ),
            facet_sizes={r: n for sub in parsed for r, n in sub.facet_sizes.items()},
        )
        # several query strings AND together
        for sub in parsed:
            if sub.q is not None:
                q = q.where(sub.q)
        # `-d a -d b` means a OR b, chained same-field `.where()` calls would AND
        if dataset:
            q = q.where(M(dataset__in=list(dataset)))

        # `--stats` wins over aggregations; a store computes both itself, over
        # every dataset it holds (`-d` filters within it)
        store = smart_get_store(input_uri)
        if store is not None and (stats or q.aggregations):
            view = store.default_view()
            if stats:
                smart_write_model(output_uri, view.stats(q))
            else:
                smart_write_json(output_uri, [view.aggregations(q)], clean=True)
            return
        proxies = smart_read_proxies(input_uri, query=q)
        if stats:
            smart_write_model(output_uri, Collector().collect_many(proxies))
        elif q.aggregations:
            for _ in proxies:  # `apply_iter` collects into `q.aggregator`
                pass
            result = q.aggregator.result if q.aggregator else {}
            smart_write_json(output_uri, [result], clean=True)
        else:
            smart_write_proxies(output_uri, proxies)


@cli.command("apply-dataset")
def cli_apply_dataset(
    dataset: Annotated[str, typer.Option("-d", "--dataset", help="Dataset to apply")],
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
    replace_dataset: Annotated[bool, typer.Option("--replace-dataset")] = False,
) -> None:
    """
    Add a dataset to the entities of a stream (`--replace-dataset` replaces theirs).
    """
    with ErrorHandler():
        proxies = smart_read_proxies(input_uri, entity_type=ValueEntity)
        proxies = (apply_dataset(p, dataset, replace=replace_dataset) for p in proxies)
        smart_write_proxies(output_uri, proxies)


cli.add_typer(dataset_cli, name="dataset")
cli.add_typer(catalog_cli, name="catalog")
cli.add_typer(store_cli, name="store")
cli.add_typer(search_cli, name="search")
cli.add_typer(fragments_cli, name="fragments")
cli.add_typer(statements_cli, name="statements")


@cli.command("aggregate")
def cli_aggregate(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
    downgrade: Annotated[bool, typer.Option("--downgrade")] = False,
) -> None:
    """
    Merge entities by id in memory.

    With `--downgrade`, entities with conflicting schemata merge into their common
    parent schema.
    """
    with ErrorHandler():
        proxies = aggregate(smart_read_proxies(input_uri), downgrade=downgrade)
        smart_write_proxies(output_uri, proxies)


# click-compatible object for docs generation (mkdocs-click)
typer_cli = get_group(cli)
