from pathlib import Path

import orjson
from anystore.logging import configure_logging
from followthemoney import Statement, ValueEntity
from followthemoney.dataset.dataset import DatasetModel
from followthemoney.statement import read_statements, write_statements
from typer.testing import CliRunner

from ftmq.cli import cli
from ftmq.io import make_entity
from ftmq.model.dataset import Catalog

runner = CliRunner()


def _get_lines(output: str) -> list[str]:
    lines = output.strip().split("\n")
    return [li.strip() for li in lines if li.strip()]


def test_cli(fixtures_path: Path):
    configure_logging()

    result = runner.invoke(cli, "--help")
    assert result.exit_code == 0

    in_uri = str(fixtures_path / "eu_authorities.ftm.json")
    result = runner.invoke(cli, ["-i", in_uri, "-d", "eu_authorities"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151
    proxy = make_entity(orjson.loads(lines[0]), ValueEntity)
    assert isinstance(proxy, ValueEntity)

    result = runner.invoke(cli, ["-i", in_uri, "-d", "other_dataset"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 0

    result = runner.invoke(cli, ["-i", in_uri, "-s", "PublicBody"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151

    result = runner.invoke(
        cli, ["-i", in_uri, "-s", "PublicBody", "-p", "jurisdiction=eu"]
    )
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151

    result = runner.invoke(
        cli, ["-i", in_uri, "-s", "PublicBody", "-p", "jurisdiction=fr"]
    )
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 0

    in_uri = str(fixtures_path / "donations.ijson")
    result = runner.invoke(cli, ["-i", in_uri, "-s", "Payment", "-p", "date__gte=2010"])
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 49

    in_uri = str(fixtures_path / "donations.ijson")
    result = runner.invoke(cli, ["-i", in_uri, "-s", "Person", "--sort", "name"])
    lines = _get_lines(result.output)
    data = orjson.loads(lines[0])
    assert data["caption"] == "Dr.-Ing. E. h. Martin Herrenknecht"
    result = runner.invoke(
        cli, ["-i", in_uri, "-s", "Person", "--sort", "name", "--sort-descending"]
    )
    lines = _get_lines(result.output)
    data = orjson.loads(lines[0])
    assert data["caption"] == "Johanna Quandt"


def test_cli_store_roundtrip(fixtures_path: Path, tmp_path: Path):
    # a store uri needs no dataset scope on either side: the writer keeps each
    # entity's own dataset, the reader spans every dataset in the store (this
    # is what the removed `--store-dataset` flag used to be needed for, and it
    # silently returned nothing when omitted)
    in_uri = str(fixtures_path / "eu_authorities.ftm.json")
    store_uri = f"sqlite:///{tmp_path}/cli.db"

    result = runner.invoke(cli, ["-i", in_uri, "-o", store_uri])
    assert result.exit_code == 0

    result = runner.invoke(cli, ["-i", store_uri])
    assert result.exit_code == 0
    assert len(_get_lines(result.output)) == 151

    result = runner.invoke(cli, ["-i", store_uri, "-d", "eu_authorities"])
    assert result.exit_code == 0
    assert len(_get_lines(result.output)) == 151

    result = runner.invoke(cli, ["-i", store_uri, "-d", "other_dataset"])
    assert result.exit_code == 0
    assert len(_get_lines(result.output)) == 0

    # entities without a dataset land in `default`; `apply-dataset` stamps one
    ds_uri = f"sqlite:///{tmp_path}/cli_ds.db"
    plain = tmp_path / "plain.json"
    plain.write_text('{"id":"x1","schema":"Company","properties":{"name":["Acme"]}}\n')
    result = runner.invoke(
        cli,
        [
            "apply-dataset",
            "-d",
            "my_dataset",
            "--replace-dataset",
            "-i",
            str(plain),
            "-o",
            ds_uri,
        ],
    )
    assert result.exit_code == 0
    result = runner.invoke(cli, ["-i", ds_uri, "-d", "my_dataset"])
    assert result.exit_code == 0
    assert len(_get_lines(result.output)) == 1


def test_cli_apply(fixtures_path: Path):
    configure_logging()

    in_uri = str(fixtures_path / "eu_authorities.ftm.json")

    result = runner.invoke(
        cli, ["apply-dataset", "-i", in_uri, "-d", "another_dataset"]
    )
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151
    proxy = make_entity(orjson.loads(lines[0]), ValueEntity)
    assert isinstance(proxy, ValueEntity)
    assert "another_dataset" in proxy.datasets
    assert "eu_authorities" in proxy.datasets
    assert "default" not in proxy.datasets

    # replace dataset
    result = runner.invoke(
        cli,
        ["apply-dataset", "-i", in_uri, "-d", "another_dataset", "--replace-dataset"],
    )
    assert result.exit_code == 0
    lines = _get_lines(result.output)
    assert len(lines) == 151
    proxy = make_entity(orjson.loads(lines[0]), ValueEntity)
    assert isinstance(proxy, ValueEntity)
    assert "another_dataset" in proxy.datasets
    assert "eu_authorities" not in proxy.datasets
    assert "default" not in proxy.datasets


def test_cli_stats(fixtures_path: Path):
    configure_logging()

    in_uri = str(fixtures_path / "donations.ijson")
    result = runner.invoke(cli, ["-i", in_uri, "-o", "/dev/null", "--stats-uri", "-"])
    assert result.exit_code == 0
    test_result = orjson.loads(result.output)
    test_result["countries"] = sorted(test_result["countries"])
    test_result["things"]["countries"] = sorted(
        test_result["things"]["countries"], key=lambda x: x["code"]
    )
    test_result["things"]["schemata"] = sorted(
        test_result["things"]["schemata"], key=lambda x: x["name"]
    )
    assert test_result == {
        "start": "2002-07-04",
        "end": "2011-12-29",
        "countries": ["cy", "de", "gb", "lu"],
        "things": {
            "total": 184,
            "countries": [
                {"code": "cy", "count": 2, "label": "Cyprus"},
                {"code": "de", "count": 163, "label": "Germany"},
                {"code": "gb", "count": 3, "label": "United Kingdom"},
                {"code": "lu", "count": 2, "label": "Luxembourg"},
            ],
            "schemata": [
                {
                    "name": "Address",
                    "count": 89,
                    "label": "Address",
                    "plural": "Addresses",
                },
                {
                    "name": "Company",
                    "count": 56,
                    "label": "Company",
                    "plural": "Companies",
                },
                {
                    "name": "Organization",
                    "count": 17,
                    "label": "Organization",
                    "plural": "Organizations",
                },
                {"name": "Person", "count": 22, "label": "Person", "plural": "People"},
            ],
        },
        "intervals": {
            "total": 290,
            "countries": [],
            "schemata": [
                {
                    "name": "Payment",
                    "count": 290,
                    "label": "Payment",
                    "plural": "Payments",
                }
            ],
        },
        "entity_count": 474,
    }


def test_cli_aggregation(fixtures_path: Path):
    configure_logging()

    in_uri = str(fixtures_path / "donations.ijson")
    result = runner.invoke(
        cli,
        [
            "-i",
            in_uri,
            "-o",
            "/dev/null",
            "--aggregation-uri",
            "-",
            "--sum",
            # aggregation fields take the wire spelling, as in `-q` / `--rql`
            "properties.amountEur",
        ],
    )
    assert result.exit_code == 0
    result = orjson.loads(result.output)
    assert result == {"sum": {"properties.amountEur": 40589689.15}}

    result = runner.invoke(
        cli,
        [
            "-i",
            in_uri,
            "-o",
            "/dev/null",
            "--aggregation-uri",
            "-",
            "--max",
            "properties.name",
            "--groups",
            "properties.country",
        ],
    )
    assert result.exit_code == 0
    result = orjson.loads(result.output)
    assert result == {
        "max": {"properties.name": "YOC AG"},
        "groups": {
            "properties.country": {
                "max": {
                    "properties.name": {
                        "de": "YOC AG",
                        "cy": "Schoeller Holdings Ltd.",
                        "gb": "Matthias Rath Limited",
                        "lu": "Eurolottoclub AG",
                    }
                }
            }
        },
    }


def test_cli_generate(fixtures_path: Path):
    configure_logging()

    # dataset
    uri = str(fixtures_path / "dataset.yml")
    res = runner.invoke(cli, ["dataset", "generate", "-i", uri])
    res = orjson.loads(res.stdout.split("\n")[-1])  # FIXME logging
    assert DatasetModel(**res)

    # catalog
    uri = str(fixtures_path / "catalog.yml")
    res = runner.invoke(cli, ["catalog", "generate", "-i", uri])
    res = orjson.loads(res.stdout.split("\n")[-1])  # FIXME logging
    assert Catalog(**res)


def test_cli_fragments_iterate_fragments(tmp_path: Path):
    from ftmq.store.fragments import get_fragments

    configure_logging()

    uri = f"sqlite:///{tmp_path / 'fragments.db'}"
    get_fragments.cache_clear()
    dataset = get_fragments("my_dataset", database_uri=uri)

    # key1 has two un-merged fragments, key2 one, key3 one with a custom origin
    dataset.put({"id": "key1", "schema": "Person", "properties": {"name": ["Alice"]}})
    dataset.put(
        {"id": "key1", "schema": "Person", "properties": {"lastName": ["Smith"]}},
        fragment="f",
    )
    dataset.put({"id": "key2", "schema": "Person", "properties": {"name": ["Bob"]}})
    dataset.put(
        {"id": "key3", "schema": "Company", "properties": {"name": ["ACME"]}},
        fragment="2",
        origin="test_o",
    )
    dataset.store.close()
    get_fragments.cache_clear()

    result = runner.invoke(
        cli, ["fragments", "iterate-fragments", "-i", uri, "-d", "my_dataset"]
    )
    assert result.exit_code == 0, result.output
    lines = _get_lines(result.output)

    # unaggregated: 4 raw fragments (key1 appears twice, not merged)
    assert len(lines) == 4
    fragments = [orjson.loads(li) for li in lines]
    ids = sorted(f["id"] for f in fragments)
    assert ids == ["key1", "key1", "key2", "key3"]
    assert all(f["datasets"] == ["my_dataset"] for f in fragments)
    assert {f["schema"] for f in fragments} == {"Company", "Person"}
    assert {f["fragment"] for f in fragments} == {"2", "default", "f"}

    # origin is passed through for the fragment that has one
    by_id = {}
    for f in fragments:
        by_id.setdefault(f["id"], []).append(f)
    assert by_id["key3"][0]["origin"] == "test_o"

    # contrast: regular `iterate` aggregates key1's fragments into one entity
    get_fragments.cache_clear()
    result = runner.invoke(cli, ["fragments", "iterate", "-i", uri, "-d", "my_dataset"])
    assert result.exit_code == 0, result.output
    entities = []
    for li in _get_lines(result.output):
        try:
            data = orjson.loads(li)
        except orjson.JSONDecodeError:
            continue  # skip interleaved log lines
        if isinstance(data, dict) and data.get("id"):
            entities.append(data)
    assert sorted(e["id"] for e in entities) == ["key1", "key2", "key3"]
    key1 = next(e for e in entities if e["id"] == "key1")
    assert key1["properties"].get("name") == ["Alice"]
    assert key1["properties"].get("lastName") == ["Smith"]


def test_cli_statements_cast_types(tmp_path: Path):
    configure_logging()

    stmts = [
        Statement(
            entity_id="pay-1",
            prop=prop,
            schema="Payment",
            value=value,
            dataset="donations",
        )
        for prop, value in (
            ("amountEur", "1,000.50"),
            ("date", "2023-01-01"),
            ("amountEur", "not-a-number"),
        )
    ]
    in_uri = tmp_path / "statements.csv"
    with open(in_uri, "wb") as fh:
        write_statements(fh, "csv", stmts)

    out_uri = tmp_path / "typed.csv"
    result = runner.invoke(
        cli, ["statements", "cast-types", "-i", str(in_uri), "-o", str(out_uri)]
    )
    assert result.exit_code == 0, result.output
    with open(out_uri, "rb") as fh:
        res = list(read_statements(fh, "csv"))
    assert [s.value for s in res] == ["1000.50", "2023-01-01", "not-a-number"]
    assert [s.original_value for s in res] == ["1,000.50", None, None]

    # unparseable values can be dropped, and other formats round-trip
    out_uri = tmp_path / "typed.json"
    result = runner.invoke(
        cli,
        [
            "statements",
            "cast-types",
            "-i",
            str(in_uri),
            "-o",
            str(out_uri),
            "--output-format",
            "json",
            "--drop-invalid",
        ],
    )
    assert result.exit_code == 0, result.output
    with open(out_uri, "rb") as fh:
        res = list(read_statements(fh, "json"))
    assert [s.value for s in res] == ["1000.50", "2023-01-01"]

    # only the given types are cast
    result = runner.invoke(
        cli,
        ["statements", "cast-types", "-i", str(in_uri), "-t", "date"],
    )
    assert result.exit_code == 0, result.output
    assert "1,000.50" in result.output

    result = runner.invoke(
        cli, ["statements", "cast-types", "-i", str(in_uri), "--output-format", "xml"]
    )
    assert result.exit_code == 1
