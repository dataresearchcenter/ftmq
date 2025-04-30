from ftmq.store import get_resolver, get_store
from ftmq.util import make_proxy


def test_resolve(fixtures_path):
    resolver = get_resolver()
    resolver.begin()
    resolver.load(fixtures_path / "resolver.ijson")
    resolver.commit()

    entities = [
        {
            "id": "a",
            "schema": "LegalEntity",
            "properties": {"name": ["Name 1"]},
            "datasets": ["dataset1"],
        },
        {
            "id": "b",
            "schema": "Person",
            "properties": {"name": ["Name 2"]},
            "datasets": ["dataset2"],
        },
        {
            "id": "c",
            "schema": "Person",
            "properties": {"name": ["Name 3"]},
            "datasets": ["dataset2"],
        },
    ]

    store = get_store("memory:///", linker=resolver)
    with store.writer() as bulk:
        for entity in entities:
            bulk.add_entity(make_proxy(entity))

    store.resolve()

    catalog = store.get_catalog()
    assert catalog.names == {"dataset1", "dataset2"}
    view = store.view(catalog.get_scope())
    entities = [e for e in view.entities()]
    assert len(entities) == 2
