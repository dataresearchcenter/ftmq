from itertools import groupby, permutations

import orjson
import pytest
from anystore.io import smart_stream_json, smart_write
from followthemoney import EntityProxy, Statement, StatementEntity, ValueEntity, model
from followthemoney.exc import InvalidData
from followthemoney.util import MEGABYTE

from ftmq.aggregate import (
    EntityPayload,
    aggregate,
    aggregate_fragments_unsafe,
    aggregate_statement_payloads,
    aggregate_statements_unsafe,
    merge,
    merge_schema,
)
from ftmq.store.fragments import get_fragments
from ftmq.util import make_dataset, make_entity


def test_aggregate():
    p1 = make_entity(
        {"id": "a", "schema": "LegalEntity", "properties": {"name": ["Jane"]}},
        ValueEntity,
    )
    p2 = make_entity(
        {"id": "a", "schema": "Person", "properties": {"name": ["Jane Doe"]}},
        ValueEntity,
    )
    assert merge(p1, p2).schema.name == "Person"
    p1.schema = model.get("Company")
    with pytest.raises(InvalidData):
        merge(p1, p2)
    assert merge(p1, p2, downgrade=True).schema.name == "LegalEntity"

    p1 = make_entity(
        {
            "id": "a",
            "schema": "Company",
            "properties": {"name": ["Jane"], "registrationNumber": ["123"]},
        },
        StatementEntity,
    )
    p2 = make_entity(
        {
            "id": "a",
            "schema": "Person",
            "properties": {"name": ["Jane Doe"], "birthDate": ["2001"]},
        },
        StatementEntity,
    )
    assert merge(p1, p2, downgrade=True).schema.name == "LegalEntity"

    # higher level aggregate function
    with pytest.raises(InvalidData):
        next(aggregate([p1, p2]))

    proxy = next(aggregate([p1, p2], downgrade=True))
    assert proxy.schema.name == "LegalEntity"


def test_aggregate_downgrade():
    def entity(schema, **props):
        return make_entity({"id": "a", "schema": schema, "properties": props})

    # the most specific common ancestor
    airplane = entity("Airplane", name=["A"])
    assert merge(airplane, entity("Vessel"), downgrade=True).schema.name == "Vehicle"
    # a later schema doesn't specialize an earlier downgrade again
    proxies = [entity("Person", name=["Jane"]), entity("Company"), entity("Person")]
    proxy = next(aggregate(proxies, downgrade=True))
    assert proxy.schema.name == "LegalEntity"
    assert proxy.get("name") == ["Jane"]


def test_merge_schema():
    assert merge_schema("Person").name == "Person"
    assert merge_schema("LegalEntity", "Person").name == "Person"
    assert merge_schema(model["Person"], "Company").name == "LegalEntity"
    assert merge_schema("Airplane", "Vessel").name == "Vehicle"
    # schemata another one extends are absorbed
    assert merge_schema("Person", "Company", "LegalEntity").name == "LegalEntity"
    assert merge_schema("Person", "Company", "Thing").name == "LegalEntity"
    # order-independent: a later schema doesn't specialize an ancestor fallback
    for schemata in permutations(("Airplane", "Person", "Vessel")):
        assert merge_schema(*schemata).name == "Thing"
    for schemata in permutations(("Person", "Company", "Organization")):
        assert merge_schema(*schemata).name == "LegalEntity"
    with pytest.raises(InvalidData):
        merge_schema("Person", "Payment")
    with pytest.raises(InvalidData):
        merge_schema("Person", "Bogus")
    with pytest.raises(InvalidData):
        merge_schema()


def _sorted_lists(data):
    # `StatementEntity` builds its lists from sets
    return {
        k: (
            {p: sorted(vs) for p, vs in v.items()}
            if k == "properties"
            else sorted(v) if isinstance(v, list) else v
        )
        for k, v in data.items()
    }


def test_aggregate_statements_unsafe(proxies):
    rows = []
    for proxy in proxies:
        for stmt in proxy.statements:
            row = stmt.to_dict()
            row.update(first_seen="2024-01-01", last_seen="2024-02-01", origin="o")
            rows.append(row)
    rows.sort(key=lambda r: r["canonical_id"])
    result = list(aggregate_statements_unsafe(rows))
    assert len(result) == len(proxies)
    dataset = make_dataset("test")
    groups = groupby(rows, key=lambda r: r["canonical_id"])
    for data, (_, group) in zip(result, groups):
        statements = [Statement.from_dict(r) for r in group]
        entity = StatementEntity.from_statements(dataset, statements)
        expected = _sorted_lists(entity.to_dict())
        assert list(data)[:3] == ["id", "caption", "schema"]
        data = _sorted_lists(data)
        # the caption is the first value of the same caption property, not the
        # one `StatementEntity` picks
        schema = model.get(data["schema"])
        prop = next((p for p in schema.caption if p in data["properties"]), None)
        captions = data["properties"][prop] if prop else [schema.label]
        assert data.pop("caption") in captions
        assert expected.pop("caption") in captions
        assert data == expected


def test_aggregate_statements_unsafe_cluster():
    def row(entity_id, schema, prop, value, **kwargs):
        return {
            "entity_id": entity_id,
            "canonical_id": "c",
            "schema": schema,
            "dataset": "test",
            "prop": prop,
            "value": value,
            **kwargs,
        }

    rows = [
        row("a", "LegalEntity", "id", "x", first_seen="2023-01-01"),
        row(
            "a",
            "LegalEntity",
            "name",
            "Jane",
            first_seen="2024-01-01",
            last_seen="2024-03-01",
            origin="o1",
        ),
        row(
            "b",
            "Person",
            "birthDate",
            "2001",
            first_seen="2024-02-01",
            last_seen="2024-02-01",
            origin="o2",
        ),
        # no canonical id: grouped by its entity id
        {**row("d", "Company", "name", "Acme"), "canonical_id": None},
    ]
    data, other = aggregate_statements_unsafe(rows)
    assert data["id"] == "c"
    assert data["schema"] == "Person"
    assert data["referents"] == ["a", "b"]
    assert data["origin"] == ["o1", "o2"]
    assert data["properties"] == {"birthDate": ["2001"], "name": ["Jane"]}
    # `first_seen` / `last_seen` over property statements, `last_change` over `id`
    assert data["first_seen"] == "2024-01-01"
    assert data["last_seen"] == "2024-03-01"
    assert data["last_change"] == "2023-01-01"
    assert other["id"] == "d"
    assert other["referents"] == []

    # conflicting schemata merge leniently, where `StatementEntity` raises
    rows = [row("a", "Person", "name", "Jane"), row("b", "Company", "name", "Acme")]
    (data,) = aggregate_statements_unsafe(rows)
    assert data["schema"] == "LegalEntity"
    rows = [row(s, s, "name", s) for s in ("Airplane", "Person", "Vessel")]
    (data,) = aggregate_statements_unsafe(rows)
    assert data["schema"] == "Thing"
    # no common ancestor
    rows = [row("a", "Person", "name", "Jane"), row("b", "Payment", "amount", "1")]
    with pytest.raises(InvalidData):
        list(aggregate_statements_unsafe(rows))
    assert list(aggregate_statements_unsafe(rows, skip_errors=True)) == []


def test_aggregate_statement_payloads():
    rows = [
        {"entity_id": "a", "canonical_id": "c", "schema": "LegalEntity",
         "dataset": "test", "prop": "id", "value": "x", "first_seen": "2023-01-01"},
        {"entity_id": "b", "canonical_id": "c", "schema": "Person", "dataset": "test",
         "prop": "name", "value": "Jane", "first_seen": "2024-02-01",
         "origin": "o", "role": "r"},
    ]  # fmt: skip
    (payload,) = aggregate_statement_payloads(rows, "test")
    assert payload.id == "c"
    assert payload.statements == rows
    # over all statements, `id` rows included
    assert payload.min_first_seen == "2023-01-01"
    assert payload.max_first_seen == "2024-02-01"
    assert payload.origins == {"o"}
    data = payload.to_dict()
    assert payload.to_dict() is data  # built once
    assert data["role"] == ["r"]
    assert data["first_seen"] == "2024-02-01"
    entity = payload.to_entity()
    assert entity.schema.name == "Person"
    assert entity.datasets == {"test"}


def test_entity_payload_from_dict(tmp_path, proxies):
    # an `entities.ftm.json` written from statement payloads reads back as is
    rows = sorted(
        (stmt.to_dict() for proxy in proxies for stmt in proxy.statements),
        key=lambda r: r["canonical_id"],
    )
    payloads = list(aggregate_statement_payloads(rows))
    uri = tmp_path / "entities.ftm.json"
    smart_write(uri, b"".join(orjson.dumps(p.to_dict()) + b"\n" for p in payloads))
    for payload, data in zip(payloads, smart_stream_json(uri), strict=True):
        loaded = EntityPayload.from_dict(data)
        assert loaded.statements == []
        assert loaded.to_dict() == payload.to_dict()
        assert loaded.origins == payload.origins
        entity = loaded.to_entity()
        assert entity.schema == payload.to_entity().schema
        assert entity.properties.keys() == payload.to_entity().properties.keys()

    # a plain FtM entity dict: list keys as lists, timestamps from its own
    data = {"id": "a", "schema": "Person", "origin": "o", "first_seen": "2024-01-01",
            "last_change": "2024-03-01", "properties": {"name": ["Jane"]}}  # fmt: skip
    payload = EntityPayload.from_dict(data, "test")
    assert payload.to_dict() == {**data, "origin": ["o"]}
    assert payload.origins == {"o"}
    assert payload.min_first_seen == "2024-01-01"
    assert payload.max_first_seen == "2024-03-01"
    assert payload.to_entity().datasets == {"test"}


def test_aggregate_statements_unsafe_caption():
    def caption(props):
        rows = [
            {
                "entity_id": "e",
                "schema": "Person",
                "dataset": "t",
                "prop": p,
                "value": v,
            }
            for p, v in props.items()
        ]
        (data,) = aggregate_statements_unsafe(rows)
        return data["caption"]

    # `email` comes after `name` in `Person.caption`
    assert caption({"name": "Jane Doe", "email": "jane@example.org"}) == "Jane Doe"
    assert caption({"email": "jane@example.org"}) == "jane@example.org"
    assert caption({"nationality": "de"}) == "Person"


def test_aggregate_statements_unsafe_statement_order():
    rows = [
        {"entity_id": "e", "schema": s, "dataset": "t", "prop": p, "value": v}
        for s, p, v in (
            ("Person", "name", "Zoe"),
            ("LegalEntity", "name", "Ann"),
            ("Person", "country", "fr"),
            ("Person", "country", "de"),
            ("LegalEntity", "email", "a@example.org"),
        )
    ]
    dicts = [
        next(aggregate_statements_unsafe(order))
        for order in (rows, rows[::-1], rows[2:] + rows[:2])
    ]
    assert dicts[0] == dicts[1] == dicts[2]
    assert dicts[0]["schema"] == "Person"
    assert list(dicts[0]["properties"]) == ["country", "email", "name"]
    assert dicts[0]["properties"]["name"] == ["Ann", "Zoe"]
    assert dicts[0]["caption"] == "Ann"


def test_aggregate_fragments_unsafe(tmp_path, eu_authorities):
    fragments = get_fragments(
        "test_aggregate", database_uri=f"sqlite:///{tmp_path}/fragments.db"
    )
    bulk = fragments.bulk()
    for proxy in eu_authorities:
        # a less specific first fragment, then one per property, half of them
        # under another origin, re-emitting the name
        name = {"name": proxy.get("name")}
        bulk.put({"id": proxy.id, "schema": "LegalEntity", "properties": name})
        for idx, (prop, values) in enumerate(proxy.properties.items()):
            data = {"id": proxy.id, "schema": proxy.schema.name}
            data["properties"] = {prop: values, **name}
            bulk.put(data, fragment=prop, origin="o" if idx % 2 else None)
    bulk.flush()
    ids = [proxy.id for proxy in eu_authorities]
    expected = [e.to_dict() for e in fragments.iterate(entity_id=ids)]
    result = list(aggregate_fragments_unsafe(fragments.fragments(entity_ids=ids)))
    assert len(result) == len(eu_authorities)
    assert result == expected
    assert {d["schema"] for d in result} == {"PublicBody"}
    assert all(list(d)[:2] == ["id", "schema"] for d in result)
    assert all(d["origin"] == ["o"] for d in result if "origin" in d)


def test_aggregate_fragments_unsafe_size_cap():
    def fragment(**props):
        return {"id": "d", "schema": "Document", "properties": props}

    fragments = [
        # the first fragment is taken as is, its size only counted
        fragment(indexText=["a" * 20 * MEGABYTE], fileName=["x.txt"]),
        fragment(indexText=["b" * 9 * MEGABYTE]),
        # past the cap: text rejected, other values kept
        fragment(indexText=["c" * 2 * MEGABYTE], title=["Title"]),
        fragment(indexText=["small", "c" * 2 * MEGABYTE], fileName=["x.txt"]),
    ]
    proxy = EntityProxy.from_dict(fragments[0])
    for data in fragments[1:]:
        proxy.merge(EntityProxy.from_dict(data))
    (result,) = aggregate_fragments_unsafe(fragments)
    assert result == proxy.to_dict()
    assert [len(v) for v in result["properties"]["indexText"]] == [
        20 * MEGABYTE,
        9 * MEGABYTE,
        5,
    ]
    assert result["properties"]["title"] == ["Title"]


def test_aggregate_fragments_unsafe_schema():
    fragments = [
        # conflicting schemata merge leniently, where `EntityProxy.merge` raises
        {"id": "a", "schema": "Person", "properties": {"name": ["Jane"]}},
        {"id": "a", "schema": "Company", "properties": {"name": ["Acme"]}},
        {"id": "a", "schema": "Person", "properties": {"birthDate": ["2001"]}},
        {"id": "b", "schema": "LegalEntity", "properties": {"name": ["Acme"]}},
        {"id": "b", "schema": "Company", "properties": {"name": ["Acme Inc."]}},
        # no common ancestor
        {"id": "c", "schema": "Person", "properties": {"name": ["Jane"]}},
        {"id": "c", "schema": "Payment", "properties": {"amount": ["1"]}},
        {"id": "c", "schema": "Person", "properties": {"name": ["J."]}},
        {"id": "d", "schema": "Person", "properties": {"name": ["Jane"]}},
    ]
    with pytest.raises(InvalidData):
        list(aggregate_fragments_unsafe(fragments))
    a, b, d = aggregate_fragments_unsafe(fragments, skip_errors=True)
    # the later `Person` fragment doesn't specialize the ancestor again
    assert a["schema"] == "LegalEntity"
    # values the merged schema lacks are kept
    assert a["properties"] == {"name": ["Jane", "Acme"], "birthDate": ["2001"]}
    assert b["schema"] == "Company"
    assert b["properties"] == {"name": ["Acme", "Acme Inc."]}
    # a single fragment: its properties as is, every other key a list
    assert d["properties"] is fragments[-1]["properties"]
    single = {"id": "e", "schema": "Person", "origin": "o", "properties": {}}
    (data,) = aggregate_fragments_unsafe([single])
    assert data == {**single, "origin": ["o"]}


def test_aggregate_fragments_unsafe_size_cap_lenient():
    # the merged `Thing` lacks `indexText`: still capped, by the fragment's schema
    fragments = [
        {
            "id": "d",
            "schema": "Document",
            "properties": {"indexText": ["a" * 29 * MEGABYTE]},
        },
        {"id": "d", "schema": "Person", "properties": {"name": ["Jane"]}},
        {
            "id": "d",
            "schema": "Document",
            "properties": {"indexText": ["b" * 2 * MEGABYTE, "small"]},
        },
    ]
    (result,) = aggregate_fragments_unsafe(fragments)
    assert result["schema"] == "Thing"
    assert [len(v) for v in result["properties"]["indexText"]] == [29 * MEGABYTE, 5]
