from pathlib import Path

from typer.testing import CliRunner

from ftmq.cli import cli

runner = CliRunner()


def test_search_cli(fixtures_path: Path, tmp_path: Path):
    in_uri = str(fixtures_path / "donations.ijson")
    docs_uri = str(tmp_path / "docs.ndjson")
    store_uri = f"sqlite:///{tmp_path / 'ftmqs.db'}"

    result = runner.invoke(cli, ["search", "transform", "-i", in_uri, "-o", docs_uri])
    assert result.exit_code == 0

    result = runner.invoke(cli, ["search", "--uri", store_uri, "index", "-i", docs_uri])
    assert result.exit_code == 0

    # a bare query routes to the `search` subcommand
    result = runner.invoke(cli, ["search", "--uri", store_uri, "metall"])
    assert result.exit_code == 0
    assert "62ad0fe6f56dbbf6fee57ce3da76e88c437024d5" in result.output

    result = runner.invoke(
        cli, ["search", "--uri", store_uri, "autocomplete", "verband"]
    )
    assert result.exit_code == 0
