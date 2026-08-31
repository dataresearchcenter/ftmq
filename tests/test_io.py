from pathlib import Path

import orjson
from followthemoney import Statement, StatementEntity, ValueEntity

from ftmq.io import (
    make_entity,
    smart_read_proxies,
    smart_read_statements,
    smart_write_proxies,
    smart_write_statements,
)
from ftmq.store import get_store
from ftmq.types import Entity


def test_io_read(fixtures_path: Path):
    success = False
    for proxy in smart_read_proxies(fixtures_path / "eu_authorities.ftm.json"):
        assert isinstance(proxy, ValueEntity)
        success = True
        break
    assert success
    success = False
    for proxy in smart_read_proxies(
        fixtures_path / "eu_authorities.ftm.json", entity_type=StatementEntity
    ):
        assert isinstance(proxy, StatementEntity)
        success = True
        break
    assert success

    # read from an iterable of uris
    uri = fixtures_path / "eu_authorities.ftm.json"
    uris = [uri, uri]
    proxies = smart_read_proxies(uris)
    assert len([p for p in proxies]) == 302


def test_io_write(tmp_path: Path, proxies: list[Entity], fixtures_path: Path):
    path = tmp_path / "proxies.json"
    res = smart_write_proxies(path, proxies[:99])
    assert res == 99
    success = False
    for proxy in smart_read_proxies(path, entity_type=StatementEntity):
        assert isinstance(proxy, StatementEntity)
        success = True
        break
    assert success

    # write ValueEntity
    entities = smart_read_proxies(fixtures_path / "eu_authorities.ftm.json")
    fp = tmp_path / "stream_proxies.ftm.json"
    smart_write_proxies(fp, entities)
    success = False
    for proxy in smart_read_proxies(fp, entity_type=StatementEntity):
        assert isinstance(proxy, StatementEntity)
        success = True
        break
    assert success


def test_io_write_stdout(capsys, proxies: list[Entity]):
    res = smart_write_proxies("-", proxies[:5])
    assert res == 5
    captured = capsys.readouterr()
    proxy = None
    for line in captured.out.split("\n"):
        proxy = make_entity(orjson.loads(line), StatementEntity)
        break
    assert isinstance(proxy, StatementEntity)


def test_io_store(tmp_path, eu_authorities):
    uri = f"leveldb://{tmp_path}/level.db"
    store = get_store(uri, dataset="eu_authorities")
    with store.writer() as bulk:
        for proxy in eu_authorities:
            bulk.add_entity(proxy)
            break
    tested = False
    for proxy in smart_read_proxies(uri, dataset="eu_authorities"):
        assert isinstance(proxy, StatementEntity)
        tested = True
        break
    assert tested

    res = smart_write_proxies(uri, eu_authorities, dataset="eu_authorities")
    assert res == 151
    res = [p for p in smart_read_proxies(uri, dataset="eu_authorities")]
    assert len(res) == 151


def test_io_store_without_dataset(tmp_path, eu_authorities):
    # regression: reading a store uri without `dataset=` used to scope the
    # view to the store's "default" writer dataset instead of its implicit
    # scope (every dataset in the backend), so nothing came back
    uri = f"sqlite:///{tmp_path}/store.db"
    res = smart_write_proxies(uri, eu_authorities)
    assert res == 151
    res = [p for p in smart_read_proxies(uri)]
    assert len(res) == 151
    assert {ds for p in res for ds in p.datasets} == {"eu_authorities"}


def _statements(canonical_id: str | None = None) -> list[Statement]:
    return [
        Statement(
            entity_id="io-1",
            prop=prop,
            schema="Person",
            value=value,
            dataset="io_test",
            canonical_id=canonical_id,
        )
        for prop, value in (("name", "Jane Doe"), ("country", "de"))
    ]


def test_io_statements(tmp_path: Path):
    """Statement streams round trip through a file, canonical ids included."""
    stmts = _statements("NK-io")
    for format in ("csv", "json"):
        uri = tmp_path / f"statements.{format}"
        assert smart_write_statements(uri, stmts, format=format) == 2
        res = list(smart_read_statements(uri, format=format))
        assert [s.id for s in res] == [s.id for s in stmts], format
        assert {s.canonical_id for s in res} == {"NK-io"}, format
        assert {s.entity_id for s in res} == {"io-1"}, format

    # `pack` can not carry a canonical id - it collapses onto the entity id
    uri = tmp_path / "statements.pack"
    smart_write_statements(uri, stmts, format="pack")
    res = list(smart_read_statements(uri, format="pack"))
    assert {s.canonical_id for s in res} == {"io-1"}


def test_io_statements_store(tmp_path: Path):
    """A store keeps the canonical id a statement carries, and upserts on it."""
    uri = f"sqlite:///{tmp_path}/statements.db"
    stmts = _statements("NK-io")

    # the store's own linker knows nothing about this merge
    assert smart_write_statements(uri, stmts, dataset="io_test") == 2
    res = list(smart_read_statements(uri, dataset="io_test"))
    assert {s.canonical_id for s in res} == {"NK-io"}
    assert {s.id for s in res} == {s.id for s in stmts}

    # re-loading the same statements under a different canonical id updates
    # them in place - the statement id does not cover `canonical_id`
    smart_write_statements(uri, _statements("NK-other"), dataset="io_test")
    res = list(smart_read_statements(uri, dataset="io_test"))
    assert len(res) == 2
    assert {s.canonical_id for s in res} == {"NK-other"}
