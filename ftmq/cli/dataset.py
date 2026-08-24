"""
`ftmq dataset` / `ftmq catalog` - work on dataset and catalog metadata.
"""

from typing import Annotated

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_write
from anystore.logging import get_logger

from ftmq.io import smart_write_proxies
from ftmq.model.dataset import Catalog, Dataset
from ftmq.model.stats import Collector

log = get_logger(__name__)

dataset_cli = typer.Typer(no_args_is_help=True)
catalog_cli = typer.Typer(no_args_is_help=True)


@dataset_cli.command("iterate")
def cli_dataset_iterate(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
) -> None:
    with ErrorHandler():
        dataset = Dataset._from_uri(input_uri)
        smart_write_proxies(output_uri, dataset.iterate())


@dataset_cli.command("generate")
def cli_dataset_generate(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
    stats: Annotated[bool, typer.Option("--stats", help="Calculate stats")] = False,
) -> None:
    """
    Convert dataset YAML specification into json and optionally calculate statistics
    """
    with ErrorHandler():
        dataset = Dataset._from_uri(input_uri)
        if stats:
            collector = Collector()
            statistics = collector.collect_many(dataset.iterate())
            dataset.apply_stats(statistics)
        smart_write(output_uri, dataset.model_dump_json().encode())


@catalog_cli.command("iterate")
def cli_catalog_iterate(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
) -> None:
    with ErrorHandler():
        catalog = Catalog._from_uri(input_uri)
        smart_write_proxies(output_uri, catalog.iterate())


@catalog_cli.command("generate")
def cli_catalog_generate(
    input_uri: Annotated[
        str, typer.Option("-i", "--input-uri", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        str, typer.Option("-o", "--output-uri", help="output file or uri")
    ] = "-",
    stats: Annotated[
        bool, typer.Option("--stats", help="Calculate stats for each dataset")
    ] = False,
) -> None:
    """
    Convert catalog YAML specification into json and fetch dataset metadata
    """
    with ErrorHandler():
        catalog = Catalog._from_uri(input_uri)
        if stats:
            for dataset in catalog.datasets:
                log.info(f"Generating stats for `{dataset.name}` ...")
                collector = Collector()
                statistics = collector.collect_many(dataset.iterate())
                dataset.apply_stats(statistics)
        smart_write(output_uri, catalog.model_dump_json().encode())
