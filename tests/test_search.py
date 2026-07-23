import orjson
from anystore.io import smart_stream

from ftmq.search.logic import transform
from ftmq.search.model import EntityDocument


def test_search_transform(fixtures_path, tmp_path, things):
    doc = EntityDocument.from_entity(things[0])
    assert doc.model_dump(by_alias=True) == {
        "id": "6d03aec76fdeec8f9697d8b19954ab6fc2568bc8",
        "caption": "MLPD",
        "schema": "Organization",
        "datasets": ["donations"],
        "countries": [],
        "names": ["MLPD"],
        "text": "MLPD",
        "fingerprints": ["mlpd"],
        "temporal_start": None,
        "temporal_end": None,
        "linked_entities": [],
        "dates": [],
    }

    out = tmp_path / "transformed.json"
    transform(fixtures_path / "donations.ijson", out)
    transformed = [d for d in smart_stream(out)]
    assert len(transformed) == 474
    data = orjson.loads(transformed[0])
    assert "donations" in EntityDocument(**data).datasets
