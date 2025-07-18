from anystore.util import clean_dict
from followthemoney import ValueEntity
from followthemoney.dataset.catalog import DataCatalog as NKCatalog
from followthemoney.dataset.dataset import Dataset as NKDataset

from ftmq.model import Catalog, Dataset, EntityModel
from ftmq.util import make_entity


def test_model_catalog_full(fixtures_path):
    # ftmq vs. nomenklatura

    catalog = Catalog(datasets=[Dataset(name="test")])
    assert isinstance(catalog.datasets[0], Dataset)
    assert NKDataset(catalog.datasets[0].model_dump())
    assert NKCatalog(NKDataset, clean_dict(catalog.model_dump()))
    assert len(catalog.names) == 1
    assert isinstance(catalog.names, set)
    assert "test" in catalog.names

    catalog = Catalog.from_yaml_uri(fixtures_path / "catalog.yml")
    assert catalog.name == "catalog"
    assert catalog.title == "A nice catalog"
    assert len(catalog.datasets) == 7
    ds = catalog.datasets[0]
    assert ds.name == "eu_transparency_register"
    assert ds.title == "EU Transparency Register"
    # local overwrite:
    assert ds.maintainer.name == "||)·|()"
    assert len(catalog.names) == 7


def test_model_catalog_iterate(fixtures_path):
    catalog = Catalog.from_yaml_uri(fixtures_path / "catalog_small.yml")
    tested = False
    for proxy in catalog.iterate():
        assert isinstance(proxy, ValueEntity)
        tested = True
        break
    assert tested


def test_model_proxy():
    data = {
        "id": "foo-1",
        "schema": "LegalEntity",
        "properties": {"name": ["Jane Doe"]},
    }
    entity = EntityModel(**data)
    proxy = make_entity(data, ValueEntity)
    assert entity.to_proxy() == proxy == EntityModel.from_proxy(proxy).to_proxy()

    data["properties"]["addressEntity"] = ["addr"]
    address = {
        "id": "addr",
        "schema": "Address",
    }
    adjacents = [make_entity(address, ValueEntity)]
    entity = EntityModel.from_proxy(make_entity(data, ValueEntity), adjacents=adjacents)
    assert isinstance(entity.properties["addressEntity"][0], EntityModel)
