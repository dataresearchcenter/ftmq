from datetime import datetime
from typing import Annotated, Callable, Optional

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_write, smart_write_json, smart_write_model
from anystore.logging import configure_logging, get_logger
from anystore.settings import Settings
from followthemoney import ValueEntity
from nomenklatura import settings as nk_settings
from rich import print
from typer.main import get_group

from ftmq import __version__
from ftmq.aggregate import aggregate
from ftmq.cli_util import DefaultCmdTyperGroup
from ftmq.io import smart_read_proxies, smart_write_proxies
from ftmq.model.dataset import Catalog, Dataset
from ftmq.model.stats import Collector
from ftmq.query import A, C, Expr, G, M, P, Query
from ftmq.search.cli import search_cli
from ftmq.store import get_store
from ftmq.store.fragments import get_fragments
from ftmq.store.fragments import get_store as get_fragments_store
from ftmq.store.fragments.settings import Settings as FragmentsSettings
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


def _node(family: Callable[..., Expr], arg: str) -> Expr:
    """Parse a `field[__op]=value` CLI argument into an `M`/`P`/`G`/`C` node.

    For the `in` / `not_in` comparators the value is comma-split into a list.
    """
    key, _, value = arg.partition("=")
    val: str | list[str] = (
        value.split(",") if key.endswith(("__in", "__not_in")) else value
    )
    return family(**{key: val})


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
    schema: Annotated[
        Optional[list[str]],
        typer.Option("-s", "--schema", help="Schema(s) to filter for"),
    ] = None,
    schema_include_descendants: Annotated[
        bool, typer.Option("--schema-include-descendants")
    ] = False,
    schema_include_matchable: Annotated[
        bool, typer.Option("--schema-include-matchable")
    ] = False,
    query: Annotated[
        Optional[list[str]],
        typer.Option(
            "-q",
            "--query",
            help="Aleph filter query string, e.g. 'filter:schema=Person&filter:countries=de'",
        ),
    ] = None,
    rql: Annotated[
        Optional[list[str]],
        typer.Option(
            "--rql",
            help="RQL query string (nested & | ~), e.g. "
            "'and(eq(schema,Person),or(eq(countries,de),eq(countries,at)))'",
        ),
    ] = None,
    meta: Annotated[
        Optional[list[str]],
        typer.Option(
            "-m", "--meta", help="Meta filter, e.g. schema=Person, id__startswith=de-"
        ),
    ] = None,
    prop: Annotated[
        Optional[list[str]],
        typer.Option("-p", "--prop", help="Property filter, e.g. name__ilike=jane%"),
    ] = None,
    group: Annotated[
        Optional[list[str]],
        typer.Option(
            "-g", "--group", help="Property-type group filter, e.g. countries=de"
        ),
    ] = None,
    context: Annotated[
        Optional[list[str]],
        typer.Option("-c", "--context", help="Context filter, e.g. origin=crawl"),
    ] = None,
    sort: Annotated[
        Optional[list[str]], typer.Option("--sort", help="Properties to sort for")
    ] = None,
    sort_ascending: Annotated[
        bool,
        typer.Option(
            "--sort-ascending/--sort-descending", help="Sort in ascending order"
        ),
    ] = True,
    stats_uri: Annotated[
        Optional[str],
        typer.Option(
            "--stats-uri",
            help="If specified, print statistic coverage information to this uri",
        ),
    ] = None,
    store_dataset: Annotated[
        Optional[str],
        typer.Option(
            "--store-dataset",
            help="If specified, default dataset for source and target stores",
        ),
    ] = None,
    sum: Annotated[
        Optional[list[str]],
        typer.Option("--sum", help="Properties for sum aggregation"),
    ] = None,
    min: Annotated[
        Optional[list[str]],
        typer.Option("--min", help="Properties for min aggregation"),
    ] = None,
    max: Annotated[
        Optional[list[str]],
        typer.Option("--max", help="Properties for max aggregation"),
    ] = None,
    avg: Annotated[
        Optional[list[str]],
        typer.Option("--avg", help="Properties for avg aggregation"),
    ] = None,
    count: Annotated[
        Optional[list[str]],
        typer.Option("--count", help="Properties for count (distinct) aggregation"),
    ] = None,
    groups: Annotated[
        Optional[list[str]],
        typer.Option("--groups", help="Properties for grouping aggregation"),
    ] = None,
    aggregation_uri: Annotated[
        Optional[str],
        typer.Option(
            "--aggregation-uri",
            help="If specified, print aggregation information to this uri",
        ),
    ] = None,
) -> None:
    """
    Apply ftmq filter to a json stream of ftm entities.
    """
    with ErrorHandler():
        q = Query()
        # -q: Aleph filter query string(s), merged in via the Aleph bridge
        for value in query or ():
            sub = Query.from_string(value).q
            if sub is not None:
                q = q.where(sub)
        # --rql: RQL query string(s) (nested & | ~), merged in via `pyrql`
        for value in rql or ():
            sub = Query.from_rql(value).q
            if sub is not None:
                q = q.where(sub)
        for value in dataset or ():
            q = q.where(M(dataset=value))
        # both legacy schema-expansion flags map to the `schemata` (is-a) field
        schema_isa = schema_include_descendants or schema_include_matchable
        for value in schema or ():
            q = q.where(M(schemata=value) if schema_isa else M(schema=value))
        # family-prefixed filter flags: -m meta, -p property, -g group, -c context
        for family, args in ((M, meta), (P, prop), (G, group), (C, context)):
            for arg in args or ():
                q = q.where(_node(family, arg))
        if sort:
            q = q.order_by(*sort, ascending=sort_ascending)

        if dataset and len(dataset) == 1:
            store_dataset = store_dataset or dataset[0]
        aggs = {
            k: v
            for k, v in {
                "sum": sum,
                "min": min,
                "max": max,
                "avg": avg,
                "count": count,
            }.items()
            if v
        }
        if aggregation_uri and aggs:
            q = q.aggregate(A(**aggs, by=list(groups or ())))
        proxies = smart_read_proxies(input_uri, dataset=store_dataset, query=q)
        stats = Collector()
        if stats_uri:
            proxies = stats.apply(proxies)
        smart_write_proxies(output_uri, proxies, dataset=store_dataset)
        if stats_uri:
            smart_write_model(stats_uri, stats.export())
        if q.aggregator and aggregation_uri:
            smart_write_json(aggregation_uri, [q.aggregator.result], clean=True)


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
    Uplevel an entity stream to nomenklatura entities and apply dataset(s) property
    """
    with ErrorHandler():
        proxies = smart_read_proxies(input_uri, entity_type=ValueEntity)
        proxies = (apply_dataset(p, dataset, replace=replace_dataset) for p in proxies)
        smart_write_proxies(output_uri, proxies)


dataset_cli = typer.Typer(no_args_is_help=True)
cli.add_typer(dataset_cli, name="dataset")


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


catalog_cli = typer.Typer(no_args_is_help=True)
cli.add_typer(catalog_cli, name="catalog")


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


store_cli = typer.Typer(no_args_is_help=True)
cli.add_typer(store_cli, name="store")


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


cli.add_typer(search_cli, name="search")

fragments_cli = typer.Typer(no_args_is_help=True)
cli.add_typer(fragments_cli, name="fragments")

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
    In-memory aggregation of entities, allowing to merge entities with a common
    parent schema (as opposed to standard `ftm aggregate`)
    """
    with ErrorHandler():
        proxies = aggregate(smart_read_proxies(input_uri), downgrade=downgrade)
        smart_write_proxies(output_uri, proxies)


# click-compatible object for docs generation (mkdocs-click)
typer_cli = get_group(cli)
