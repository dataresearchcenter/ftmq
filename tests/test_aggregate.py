import pytest
from followthemoney import StatementEntity, ValueEntity, model
from followthemoney.exc import InvalidData

from ftmq.aggregate import aggregate, merge
from ftmq.util import make_entity


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
