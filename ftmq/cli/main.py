from typing import Annotated, Callable, Optional

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
from ftmq.io import smart_read_proxies, smart_write_proxies
from ftmq.model.stats import Collector
from ftmq.query import A, C, Expr, G, M, P, Query, ref_from_wire
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
            help="Filter query string, e.g. 'filter:schema=Person&filter:group.countries=de'",
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
        Optional[str], typer.Option("--sort", help="Property to sort by")
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
        typer.Option("--sum", help="Field(s) for sum aggregation"),
    ] = None,
    min: Annotated[
        Optional[list[str]],
        typer.Option("--min", help="Field(s) for min aggregation"),
    ] = None,
    max: Annotated[
        Optional[list[str]],
        typer.Option("--max", help="Field(s) for max aggregation"),
    ] = None,
    avg: Annotated[
        Optional[list[str]],
        typer.Option("--avg", help="Field(s) for avg aggregation"),
    ] = None,
    count: Annotated[
        Optional[list[str]],
        typer.Option("--count", help="Field(s) for count (distinct) aggregation"),
    ] = None,
    groups: Annotated[
        Optional[list[str]],
        typer.Option("--groups", help="Field(s) to group the aggregations by"),
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
        # repeated flags are alternatives (`-d a -d b` means a OR b), so they
        # combine via `__in` - chained same-field `.where()` calls would AND
        if dataset:
            q = q.where(M(dataset__in=list(dataset)))
        # both legacy schema-expansion flags map to the `schemata` (is-a) field
        schema_isa = schema_include_descendants or schema_include_matchable
        if schema:
            values = list(schema)
            q = q.where(M(schemata__in=values) if schema_isa else M(schema__in=values))
        # family-prefixed filter flags: -m meta, -p property, -g group, -c context
        for family, args in ((M, meta), (P, prop), (G, group), (C, context)):
            for arg in args or ():
                q = q.where(_node(family, arg))
        if sort:
            q = q.order_by(sort, ascending=sort_ascending)

        if dataset and len(dataset) == 1:
            store_dataset = store_dataset or dataset[0]
        # aggregation fields are spelled as in a query string / rql (a property
        # is `properties.<name>`, a group / meta field is bare), so one
        # spelling covers `--sum`, `-q` and `--rql`
        aggs = {
            k: [ref_from_wire(f) for f in v]
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
            by = [ref_from_wire(g) for g in groups or ()]
            q = q.aggregate(A(**aggs, by=by))
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


# sub-command groups
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
    In-memory aggregation of entities, allowing to merge entities with a common
    parent schema (as opposed to standard `ftm aggregate`)
    """
    with ErrorHandler():
        proxies = aggregate(smart_read_proxies(input_uri), downgrade=downgrade)
        smart_write_proxies(output_uri, proxies)


# click-compatible object for docs generation (mkdocs-click)
typer_cli = get_group(cli)
