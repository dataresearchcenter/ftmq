from datetime import datetime

import click
from anystore.io import smart_write, smart_write_json, smart_write_model
from anystore.logging import configure_logging, get_logger
from click_default_group import DefaultGroup
from followthemoney import ValueEntity
from nomenklatura import settings

from ftmq.aggregate import aggregate
from ftmq.io import smart_read_proxies, smart_write_proxies
from ftmq.model.dataset import Catalog, Dataset
from ftmq.model.stats import Collector
from ftmq.query import C, Expr, G, M, P, Query
from ftmq.store import get_store
from ftmq.store.fragments import get_fragments
from ftmq.store.fragments import get_store as get_fragments_store
from ftmq.store.fragments.settings import Settings as FragmentsSettings
from ftmq.util import apply_dataset

log = get_logger(__name__)


@click.group(cls=DefaultGroup, default="q", default_if_no_args=True)
def cli() -> None:
    configure_logging()


def _node(family: type[Expr], arg: str) -> Expr:
    """Parse a `field[__op]=value` CLI argument into an `M`/`P`/`G`/`C` node.

    For the `in` / `not_in` comparators the value is comma-split into a list.
    """
    key, _, value = arg.partition("=")
    val: str | list[str] = (
        value.split(",") if key.endswith(("__in", "__not_in")) else value
    )
    return family(**{key: val})


@cli.command()
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option("-d", "--dataset", multiple=True, help="Dataset(s) to filter for")
@click.option("-s", "--schema", multiple=True, help="Schema(s) to filter for")
@click.option(
    "--schema-include-descendants", is_flag=True, default=False, show_default=True
)
@click.option(
    "--schema-include-matchable", is_flag=True, default=False, show_default=True
)
@click.option(
    "-q",
    "--query",
    multiple=True,
    help="Aleph filter query string, e.g. 'filter:schema=Person&filter:countries=de'",
)
@click.option(
    "-m",
    "--meta",
    multiple=True,
    help="Meta filter, e.g. schema=Person, id__startswith=de-",
)
@click.option(
    "-p", "--prop", multiple=True, help="Property filter, e.g. name__ilike=jane%"
)
@click.option(
    "-g", "--group", multiple=True, help="Property-type group filter, e.g. countries=de"
)
@click.option(
    "-c", "--context", multiple=True, help="Context filter, e.g. origin=crawl"
)
@click.option("--sort", help="Properties to sort for", multiple=True)
@click.option(
    "--sort-ascending/--sort-descending",
    is_flag=True,
    help="Sort in ascending order",
    default=True,
    show_default=True,
)
@click.option(
    "--stats-uri",
    default=None,
    show_default=True,
    help="If specified, print statistic coverage information to this uri",
)
@click.option(
    "--store-dataset",
    default=None,
    show_default=True,
    help="If specified, default dataset for source and target stores",
)
@click.option("--sum", multiple=True, help="Properties for sum aggregation")
@click.option("--min", multiple=True, help="Properties for min aggregation")
@click.option("--max", multiple=True, help="Properties for max aggregation")
@click.option("--avg", multiple=True, help="Properties for avg aggregation")
@click.option(
    "--count", multiple=True, help="Properties for count (distinct) aggregation"
)
@click.option("--groups", multiple=True, help="Properties for grouping aggregation")
@click.option(
    "--aggregation-uri",
    default=None,
    show_default=True,
    help="If specified, print aggregation information to this uri",
)
def q(
    input_uri: str = "-",
    output_uri: str = "-",
    dataset: tuple[str, ...] = (),
    schema: tuple[str, ...] = (),
    schema_include_descendants: bool = False,
    schema_include_matchable: bool = False,
    query: tuple[str, ...] = (),
    meta: tuple[str, ...] = (),
    prop: tuple[str, ...] = (),
    group: tuple[str, ...] = (),
    context: tuple[str, ...] = (),
    sort: tuple[str, ...] = (),
    sort_ascending: bool = True,
    stats_uri: str | None = None,
    store_dataset: str | None = None,
    sum: tuple[str, ...] = (),
    min: tuple[str, ...] = (),
    max: tuple[str, ...] = (),
    avg: tuple[str, ...] = (),
    count: tuple[str, ...] = (),
    groups: tuple[str, ...] = (),
    aggregation_uri: str | None = None,
):
    """
    Apply ftmq filter to a json stream of ftm entities.
    """
    q = Query()
    # -q: Aleph filter query string(s), merged in via the Aleph bridge
    for value in query:
        sub = Query.from_string(value).q
        if sub is not None:
            q = q.where(sub)
    for value in dataset:
        q = q.where(M(dataset=value))
    # both legacy schema-expansion flags map to the `schemata` (is-a) field
    schema_isa = schema_include_descendants or schema_include_matchable
    for value in schema:
        q = q.where(M(schemata=value) if schema_isa else M(schema=value))
    # family-prefixed filter flags: -m meta, -p property, -g group, -c context
    for family, args in ((M, meta), (P, prop), (G, group), (C, context)):
        for arg in args:
            q = q.where(_node(family, arg))
    if len(sort):
        q = q.order_by(*sort, ascending=sort_ascending)

    if len(dataset) == 1:
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
        for func, props in aggs.items():
            q = q.aggregate(func, *props, groups=groups)
    proxies = smart_read_proxies(input_uri, dataset=store_dataset, query=q)
    stats = Collector()
    if stats_uri:
        proxies = stats.apply(proxies)
    smart_write_proxies(output_uri, proxies, dataset=store_dataset)
    if stats_uri:
        stats = stats.export()
        smart_write_model(stats_uri, stats)
    if q.aggregator and aggregation_uri:
        smart_write_json(aggregation_uri, [q.aggregator.result], clean=True)


@cli.command("apply-dataset")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option("-d", "--dataset", help="Dataset to apply", required=True)
@click.option("--replace-dataset", is_flag=True, default=False, show_default=True)
def apply(
    dataset: str,
    input_uri: str | None = "-",
    output_uri: str | None = "-",
    replace_dataset: bool | None = False,
):
    """
    Uplevel an entity stream to nomenklatura entities and apply dataset(s) property
    """

    proxies = smart_read_proxies(input_uri or "-", entity_type=ValueEntity)
    proxies = (apply_dataset(p, dataset, replace=replace_dataset) for p in proxies)
    smart_write_proxies(output_uri or "-", proxies)


@cli.group()
def dataset():
    pass


@dataset.command("iterate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
def dataset_iterate(input_uri: str = "-", output_uri: str = "-"):
    dataset = Dataset._from_uri(input_uri)
    smart_write_proxies(output_uri, dataset.iterate())


@dataset.command("generate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option(
    "--stats",
    is_flag=True,
    default=False,
    show_default=True,
    help="Calculate stats",
)
def make_dataset(
    input_uri: str = "-",
    output_uri: str = "-",
    stats: bool = False,
):
    """
    Convert dataset YAML specification into json and optionally calculate statistics
    """
    dataset = Dataset._from_uri(input_uri)
    if stats:
        collector = Collector()
        statistics = collector.collect_many(dataset.iterate())
        dataset.apply_stats(statistics)
    smart_write(output_uri, dataset.model_dump_json().encode())


@cli.group()
def catalog():
    pass


@catalog.command("iterate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
def catalog_iterate(input_uri: str = "-", output_uri: str = "-"):
    catalog = Catalog._from_uri(input_uri)
    smart_write_proxies(output_uri, catalog.iterate())


@catalog.command("generate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option(
    "--stats",
    is_flag=True,
    default=False,
    show_default=True,
    help="Calculate stats for each dataset",
)
def make_catalog(
    input_uri: str = "-",
    output_uri: str = "-",
    stats: bool = False,
):
    """
    Convert catalog YAML specification into json and fetch dataset metadata
    """
    catalog = Catalog._from_uri(input_uri)
    if stats:
        for dataset in catalog.datasets:
            log.info(f"Generating stats for `{dataset.name}` ...")
            collector = Collector()
            statistics = collector.collect_many(dataset.iterate())
            dataset.apply_stats(statistics)
    smart_write(output_uri, catalog.model_dump_json().encode())


@cli.group()
def store():
    pass


@store.command("list-datasets")
@click.option(
    "-i",
    "--input-uri",
    default=settings.DB_URL,
    show_default=True,
    help="input file or uri",
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
def store_list_datasets(
    input_uri: str = settings.DB_URL,
    output_uri: str = "-",
):
    """
    List datasets within a store
    """
    store = get_store(input_uri)
    catalog = store.get_scope()
    datasets = [ds.name for ds in catalog.datasets]
    smart_write(output_uri, "\n".join(datasets).encode() + b"\n")


@store.command("iterate")
@click.option(
    "-i",
    "--input-uri",
    default=settings.DB_URL,
    show_default=True,
    help="store input uri",
)
@click.option(
    "-o", "--output-uri", default=None, show_default=True, help="output file or uri"
)
def store_iterate(
    input_uri: str = settings.DB_URL,
    output_uri: str = "-",
):
    """
    Iterate all entities from in to out
    """
    store = get_store(input_uri)
    smart_write_proxies(output_uri, store.iterate())


@cli.group()
def fragments():
    pass


fragments_settings = FragmentsSettings()


@fragments.command("list-datasets")
@click.option(
    "-i",
    "--input-uri",
    default=fragments_settings.database_uri,
    show_default=True,
    help="input file or uri",
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
def fragments_list_datasets(
    input_uri: str = fragments_settings.database_uri,
    output_uri: str = "-",
):
    """
    List datasets within a fragments store
    """
    store = get_fragments_store(input_uri)
    datasets = [ds.name for ds in store.all()]
    smart_write(output_uri, "\n".join(datasets).encode() + b"\n")


@fragments.command("iterate")
@click.option(
    "-i",
    "--input-uri",
    default=fragments_settings.database_uri,
    show_default=True,
    help="fragments store input uri",
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option("-d", "--dataset", required=True, help="Dataset name to iterate")
@click.option("-s", "--schema", default=None, help="Filter by schema")
@click.option(
    "--since",
    default=None,
    help="Filter by timestamp (since), ISO format: YYYY-MM-DDTHH:MM:SS",
)
@click.option(
    "--until",
    default=None,
    help="Filter by timestamp (until), ISO format: YYYY-MM-DDTHH:MM:SS",
)
def fragments_iterate(
    input_uri: str = fragments_settings.database_uri,
    output_uri: str = "-",
    dataset: str = None,
    schema: str | None = None,
    since: str | None = None,
    until: str | None = None,
):
    """
    Iterate all entities from a fragments dataset
    """
    fragments = get_fragments(dataset, database_uri=input_uri)

    # Parse timestamp strings to datetime objects
    since_dt = datetime.fromisoformat(since) if since else None
    until_dt = datetime.fromisoformat(until) if until else None

    smart_write_proxies(
        output_uri, fragments.iterate(schema=schema, since=since_dt, until=until_dt)
    )


@fragments.command("iterate-fragments")
@click.option(
    "-i",
    "--input-uri",
    default=fragments_settings.database_uri,
    show_default=True,
    help="fragments store input uri",
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option("-d", "--dataset", required=True, help="Dataset name to iterate")
def fragments_iterate_unsorted(
    input_uri: str = fragments_settings.database_uri,
    output_uri: str = "-",
    dataset: str = None,
):
    """
    Iterate all fragments from a dataset, unsorted and not aggregated. Useful
    for streaming into another storage that does dedupe by itself.
    """
    fragments = get_fragments(dataset, database_uri=input_uri)
    smart_write_json(output_uri, fragments.fragments(sort=False, include_fragment=True))


@cli.command("aggregate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option("--downgrade", is_flag=True, default=False, show_default=True)
def cli_aggregate(
    input_uri: str = "-",
    output_uri: str = "-",
    downgrade: bool = False,
):
    """
    In-memory aggregation of entities, allowing to merge entities with a common
    parent schema (as opposed to standard `ftm aggregate`)
    """
    proxies = aggregate(smart_read_proxies(input_uri), downgrade=downgrade)
    smart_write_proxies(output_uri, proxies)
