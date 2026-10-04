from pathlib import Path

from followthemoney import StatementEntity

from ftmq.io import smart_read_proxies
from ftmq.model.stats import Collector, DatasetStats
from ftmq.store import get_store
from ftmq.util import make_entity


def test_coverage(fixtures_path: Path):
    c = Collector()
    for proxy in smart_read_proxies(fixtures_path / "donations.ijson"):
        c.collect(proxy)

    result = {
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

    assert isinstance(c.export(), DatasetStats)
    test_result = c.to_dict()
    test_result["countries"] = sorted(test_result["countries"])
    test_result["things"]["countries"] = sorted(
        test_result["things"]["countries"], key=lambda x: x["code"]
    )
    test_result["things"]["schemata"] = sorted(
        test_result["things"]["schemata"], key=lambda x: x["name"]
    )
    assert test_result == result

    proxies = smart_read_proxies(fixtures_path / "donations.ijson")
    collector = Collector()
    proxies = collector.apply(proxies)
    len_proxies = len([x for x in proxies])
    stats = collector.export()
    assert stats.entity_count > 0
    assert stats.entity_count == len_proxies
    assert stats.years == (2002, 2011)


def test_coverage_buckets(tmp_path: Path):
    # a schema is bucketed by `Thing` / `Interval` independently, as the sql
    # store does: one in neither bucket (documents) counts in `entity_count`
    # alone, one in both (`Event`) counts in each
    entities = [
        make_entity(
            {"id": f"e{i}", "schema": schema, "properties": props},
            StatementEntity,
            "test",
        )
        for i, (schema, props) in enumerate(
            [
                ("Person", {"name": ["Jane"], "country": ["de"]}),
                ("Payment", {"amount": ["1"], "date": ["2020-01-01"]}),
                ("Event", {"name": ["Summit"], "country": ["fr"]}),
                ("Page", {"bodyText": ["lorem"]}),
                ("Mention", {"name": ["Jane"]}),
                ("Similar", {"score": ["0.5"]}),
            ]
        )
    ]
    stats = Collector().collect_many(entities)
    assert stats.entity_count == 6
    assert {s.name: s.count for s in stats.things.schemata} == {
        "Person": 1,
        "Event": 1,
    }
    assert {s.name: s.count for s in stats.intervals.schemata} == {
        "Payment": 1,
        "Event": 1,
    }
    assert {c.code: c.count for c in stats.intervals.countries} == {"fr": 1}

    store = get_store(f"sqlite:///{tmp_path}/stats.db")
    with store.writer() as bulk:
        for entity in entities:
            bulk.add_entity(entity)
    assert _sorted(store.default_view().stats()) == _sorted(stats)


def _sorted(stats: DatasetStats) -> dict:
    data = stats.model_dump()
    for part in ("things", "intervals"):
        for key in ("countries", "schemata"):
            data[part][key] = sorted(data[part][key], key=str)
    return data
