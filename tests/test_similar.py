from nomenklatura.db import get_metadata
from nomenklatura.xref import xref

from ftmq.similar import get_similar
from ftmq.store import get_store
from ftmq.store.base import get_resolver


def test_similar(eu_authorities, tmp_path):
    get_metadata.cache_clear()  # FIXME

    db_uri = f"sqlite:///{tmp_path}/store-similar.db"
    resolver = get_resolver(db_uri)
    store = get_store(db_uri, dataset="eu_authorities", linker=resolver)
    with store.writer() as bulk:
        for p in eu_authorities:
            bulk.add_entity(p)

    resolver.begin()
    xref(resolver, store, tmp_path / "nkix")
    resolver.commit()

    similar = get_similar("eu-authorities-rea", resolver)
    entity_id, score = next(similar)
    assert entity_id == "eu-authorities-ercea"
    assert score > 0.5

    view = store.default_view()
    similar = view.similar("eu-authorities-rea")
    proxy, score = next(similar)
    assert proxy.id == "eu-authorities-ercea"
    assert score > 0.5

    get_metadata.cache_clear()  # FIXME
