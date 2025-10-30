import os
import time
from datetime import datetime, timedelta

import pytest
from followthemoney import EntityProxy

from ftmq.store.fragments import get_fragments


def test_fragment_store_settings(monkeypatch):
    get_fragments.cache_clear()
    dataset = get_fragments("test")
    assert dataset.store.database_uri == "sqlite:///ftm_fragments.db"  # default

    get_fragments.cache_clear()
    uri = "sqlite:///fragments.store"
    monkeypatch.setenv("FTM_FRAGMENTS_URI", uri)
    dataset = get_fragments("test")
    assert dataset.store.database_uri == uri

    get_fragments.cache_clear()
    uri = "postgresql:///fragments"
    monkeypatch.setenv("FTM_FRAGMENTS_URI", uri)
    dataset = get_fragments("test")
    assert dataset.store.database_uri == uri.replace("postgresql", "postgresql+psycopg")
    assert dataset.store.is_postgres

    get_fragments.cache_clear()
    uri = "sqlite:///:memory:"
    monkeypatch.setenv("FTM_FRAGMENTS_URI", uri)
    dataset = get_fragments("test")
    assert dataset.store.database_uri == "sqlite:///:memory:"
    assert not dataset.store.is_postgres


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
    dataset.put(entity3, fragment="2", origin="test_o")

    entity = dataset.get("key1")
    assert entity is not None
    assert isinstance(entity, EntityProxy)
    assert entity.schema.name == "Person"
    # assert entity.datasets == {"test_us_ofac"}

    assert len(list(dataset.iterate())) == 3
    assert len(dataset) == 3
    assert len(list(dataset.iterate(entity_id="key1"))) == 1
    assert len(list(dataset.iterate(entity_id="key3"))) == 1

    both = ["key1", "key3"]
    assert len(list(dataset.iterate(entity_id=both))) == 2

    # Test schema filtering
    # Note: schema filtering works at the fragment level
    assert len(list(dataset.iterate(schema="Person"))) == 3
    assert len(list(dataset.iterate(schema="LegalEntity"))) == 1
    person_entities = list(dataset.iterate(schema="Person"))
    assert all(e.schema.name == "Person" for e in person_entities)
    legal_entities = list(dataset.iterate(schema="LegalEntity"))
    assert all(e.schema.name == "LegalEntity" for e in legal_entities)

    dataset.delete(entity_id="key1")
    assert len(list(dataset.iterate(entity_id="key1"))) == 0

    bulk = dataset.bulk()
    bulk.put(entity1)
    bulk.put(entity1)
    bulk.put(entity1f, fragment="f")
    bulk.flush()
    assert len(list(dataset.iterate(entity_id="key1"))) == 1
    assert len(list(dataset.fragments(entity_ids="key1"))) == 2

    assert len(list(dataset.iterate_batched(batch_size=2))) == 3
    assert next(dataset.get_sorted_id_batches()) == ["key1", "key2", "key3"]

    entity = dataset.get("key3")
    assert entity.context.get("origin") == "test_o"
    assert entity.to_dict()["origin"] == "test_o"

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
    dataset.put(entity3, fragment="2", origin="test_o")

    entity = dataset.get("key1")
    assert entity is not None
    assert isinstance(entity, EntityProxy)
    assert entity.schema.name == "Person"
    # assert entity.datasets == {"test_us_ofac"}

    assert len(list(dataset.iterate())) == 3
    assert len(list(dataset)) == 3
    assert len(dataset) == 3
    assert len(list(dataset.iterate(entity_id="key1"))) == 1
    assert len(list(dataset.iterate(entity_id="key3"))) == 1
    assert len(dataset.store) == 1

    # Test schema filtering
    # Note: schema filtering works at the fragment level
    assert len(list(dataset.iterate(schema="Person"))) == 3
    assert len(list(dataset.iterate(schema="LegalEntity"))) == 1
    person_entities = list(dataset.iterate(schema="Person"))
    assert all(e.schema.name == "Person" for e in person_entities)
    legal_entities = list(dataset.iterate(schema="LegalEntity"))
    assert all(e.schema.name == "LegalEntity" for e in legal_entities)

    assert len(list(dataset.iterate_batched(batch_size=2))) == 3
    assert next(dataset.get_sorted_id_batches()) == ["key1", "key2", "key3"]

    entity = dataset.get("key3")
    assert entity.context.get("origin") == "test_o"
    assert entity.to_dict()["origin"] == "test_o"

    dataset.drop()
    dataset.store.close()


def test_fragment_store_timestamp_filter_sqlite():
    """Test timestamp filtering with since and until parameters"""
    uri = "sqlite://"
    dataset = get_fragments("test_timestamp", database_uri=uri)
    dataset.drop()  # Clean slate

    # Record timestamps for filtering
    before = datetime.utcnow()
    time.sleep(0.01)  # Small delay to ensure timestamp differences

    # Add first entity
    entity1 = {"id": "key1", "schema": "Person", "properties": {"name": ["First"]}}
    dataset.put(entity1)

    time.sleep(0.01)
    middle = datetime.utcnow()
    time.sleep(0.01)

    # Add second entity
    entity2 = {"id": "key2", "schema": "Person", "properties": {"name": ["Second"]}}
    dataset.put(entity2)

    time.sleep(0.01)
    after = datetime.utcnow()

    # Test: no filters should return all entities
    assert len(list(dataset.iterate())) == 2

    # Test: since filter (only get entity2)
    entities_since = list(dataset.iterate(since=middle))
    assert len(entities_since) == 1
    assert entities_since[0].id == "key2"

    # Test: until filter (only get entity1)
    entities_until = list(dataset.iterate(until=middle))
    assert len(entities_until) == 1
    assert entities_until[0].id == "key1"

    # Test: both since and until (get entities in middle range)
    entities_range = list(dataset.iterate(since=before, until=after))
    assert len(entities_range) == 2

    # Test: range that excludes everything
    entities_empty = list(dataset.iterate(since=after, until=after))
    assert len(entities_empty) == 0

    # Test: fragments() method with timestamp filters
    fragments_since = list(dataset.fragments(since=middle))
    assert len(fragments_since) == 1
    assert fragments_since[0]["id"] == "key2"

    # Test: partials() method with timestamp filters
    partials_until = list(dataset.partials(since=middle))
    assert len(partials_until) == 1
    assert partials_until[0].id == "key2"

    # Test: iterate_batched() with timestamp filters
    batched_since = list(dataset.iterate_batched(batch_size=1, since=middle))
    assert len(batched_since) == 1
    assert batched_since[0].id == "key2"

    # Test: get_sorted_id_batches() with timestamp filters
    id_batches = list(dataset.get_sorted_id_batches(batch_size=10, since=middle))
    assert len(id_batches) == 1
    assert id_batches[0] == ["key2"]

    # Test: get_sorted_ids() with timestamp filters
    sorted_ids = list(dataset.get_sorted_ids(since=middle))
    assert sorted_ids == ["key2"]

    # Test: statements() method with until filter
    statements_until = list(dataset.statements(until=middle))
    entity1_statements = [s for s in statements_until if s.entity_id == "key1"]
    assert len(entity1_statements) > 0
    entity2_statements = [s for s in statements_until if s.entity_id == "key2"]
    assert len(entity2_statements) == 0

    dataset.drop()
    dataset.store.close()


def test_fragment_store_timestamp_filter_postgres():
    """Test timestamp filtering with since and until parameters on PostgreSQL"""
    uri = os.environ.get("TESTING_FRAGMENTS_PSQL_URI")
    if not uri:
        print(
            "Skipping psql timestamp filter test (no `TESTING_FRAGMENTS_PSQL_URI` env)"
        )
        return

    dataset = get_fragments("test_timestamp_pg", database_uri=uri)
    dataset.drop()  # Clean slate

    # Record timestamps for filtering
    before = datetime.utcnow()
    time.sleep(0.01)  # Small delay to ensure timestamp differences

    # Add first entity
    entity1 = {"id": "key1", "schema": "Person", "properties": {"name": ["First"]}}
    dataset.put(entity1)

    time.sleep(0.01)
    middle = datetime.utcnow()
    time.sleep(0.01)

    # Add second entity
    entity2 = {"id": "key2", "schema": "Person", "properties": {"name": ["Second"]}}
    dataset.put(entity2)

    time.sleep(0.01)
    after = datetime.utcnow()

    # Test: no filters should return all entities
    assert len(list(dataset.iterate())) == 2

    # Test: since filter (only get entity2)
    entities_since = list(dataset.iterate(since=middle))
    assert len(entities_since) == 1
    assert entities_since[0].id == "key2"

    # Test: until filter (only get entity1)
    entities_until = list(dataset.iterate(until=middle))
    assert len(entities_until) == 1
    assert entities_until[0].id == "key1"

    # Test: both since and until (get entities in middle range)
    entities_range = list(dataset.iterate(since=before, until=after))
    assert len(entities_range) == 2

    # Test: range that excludes everything
    entities_empty = list(dataset.iterate(since=after, until=after))
    assert len(entities_empty) == 0

    # Test: fragments() method with timestamp filters
    fragments_since = list(dataset.fragments(since=middle))
    assert len(fragments_since) == 1
    assert fragments_since[0]["id"] == "key2"

    # Test: partials() method with timestamp filters
    partials_until = list(dataset.partials(since=middle))
    assert len(partials_until) == 1
    assert partials_until[0].id == "key2"

    # Test: iterate_batched() with timestamp filters
    batched_since = list(dataset.iterate_batched(batch_size=1, since=middle))
    assert len(batched_since) == 1
    assert batched_since[0].id == "key2"

    # Test: get_sorted_id_batches() with timestamp filters
    id_batches = list(dataset.get_sorted_id_batches(batch_size=10, since=middle))
    assert len(id_batches) == 1
    assert id_batches[0] == ["key2"]

    # Test: get_sorted_ids() with timestamp filters
    sorted_ids = list(dataset.get_sorted_ids(since=middle))
    assert sorted_ids == ["key2"]

    # Test: statements() method with until filter
    statements_until = list(dataset.statements(until=middle))
    entity1_statements = [s for s in statements_until if s.entity_id == "key1"]
    assert len(entity1_statements) > 0
    entity2_statements = [s for s in statements_until if s.entity_id == "key2"]
    assert len(entity2_statements) == 0

    dataset.drop()
    dataset.store.close()
