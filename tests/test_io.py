from pathlib import Path

import orjson
from followthemoney import StatementEntity, ValueEntity

from ftmq.io import make_entity, smart_read_proxies, smart_write_proxies
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
