import os

import pytest
from followthemoney import ValueEntity

from ftmq.store.fragments import get_fragments


def test_fragment_store_settings(monkeypatch):
    dataset = get_fragments("test")
    assert dataset.store.database_uri == "sqlite:///ftm_fragments.db"  # default

    get_fragments.cache_clear()
    uri = "sqlite:///fragments.store"
    monkeypatch.setenv("FTM_FRAGMENTS_URI", uri)
    dataset = get_fragments("test")
    assert dataset.store.database_uri == uri


def test_fragment_store_postgres():
    uri = os.environ.get("TESTING_FRAGMENTS_PSQL_URI")
    if not uri:
        print("Skipping psql test (no `TESTING_FRAGMENTS_PSQL_URI` env)")
        return

    with pytest.raises(ValueError):
        dataset = get_fragments("TEST-US-OFAC", database_uri=uri)

    dataset = get_fragments("test_us_ofac", database_uri=uri)
    assert dataset.name == "test_us_ofac"
    assert dataset.store.database_uri.startswith("postgres")

    entity1 = {"id": "key1", "schema": "Person", "properties": {}}
    entity1f = {"id": "key1", "schema": "LegalEntity", "properties": {}}
    entity2 = {"id": "key2", "schema": "Person", "properties": {}}
    props = {"name": ["Banana Man"]}
    entity3 = {"id": "key3", "schema": "Person", "properties": props}

    dataset.put(entity1)
    dataset.put(entity1f, fragment="f")
    dataset.put(entity2)
    dataset.put(entity3, fragment="2")

    entity = dataset.get("key1")
    assert entity is not None
    assert isinstance(entity, ValueEntity)
    assert entity.schema.name == "Person"
    assert entity.datasets == {"test_us_ofac"}

    assert len(list(dataset.iterate())) == 3
    assert len(dataset) == 3
    assert len(list(dataset.iterate(entity_id="key1"))) == 1
    assert len(list(dataset.iterate(entity_id="key3"))) == 1

    both = ["key1", "key3"]
    assert len(list(dataset.iterate(entity_id=both))) == 2

    dataset.delete(entity_id="key1")
    assert len(list(dataset.iterate(entity_id="key1"))) == 0

    bulk = dataset.bulk()
    bulk.put(entity1)
    bulk.put(entity1)
    bulk.put(entity1f, fragment="f")
    bulk.flush()
    assert len(list(dataset.iterate(entity_id="key1"))) == 1
    assert len(list(dataset.fragments(entity_ids="key1"))) == 2

    dataset.drop()
    dataset.store.close()


def test_fragment_store_sqlite():
    uri = "sqlite://"
    with pytest.raises(ValueError):
        dataset = get_fragments("TEST-US-OFAC", database_uri=uri)
    dataset = get_fragments("test_us_ofac", database_uri=uri)
    assert dataset.name == "test_us_ofac"
    assert len(dataset.store) == 0
    dataset.drop()
    assert len(dataset.store) == 0

    entity1 = {"id": "key1", "schema": "Person", "properties": {}}
    entity1f = {"id": "key1", "schema": "LegalEntity", "properties": {}}
    entity2 = {"id": "key2", "schema": "Person", "properties": {}}
    props = {"name": ["Banana Man"]}
    entity3 = {"id": "key3", "schema": "Person", "properties": props}

    dataset.put(entity1)
    dataset.put(entity1f, fragment="f")
    dataset.put(entity2)
    dataset.put(entity3, fragment="2")

    entity = dataset.get("key1")
    assert entity is not None
    assert isinstance(entity, ValueEntity)
    assert entity.schema.name == "Person"
    assert entity.datasets == {"test_us_ofac"}

    assert len(list(dataset.iterate())) == 3
    assert len(list(dataset)) == 3
    assert len(dataset) == 3
    assert len(list(dataset.iterate(entity_id="key1"))) == 1
    assert len(list(dataset.iterate(entity_id="key3"))) == 1
    assert len(dataset.store) == 1

    dataset.drop()
    dataset.store.close()
