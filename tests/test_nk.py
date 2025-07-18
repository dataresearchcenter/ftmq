"""
Test if we are still in sync with the nomenklatura data model
(aka OpenSanctions)
"""

from followthemoney import ValueEntity

from ftmq.model.dataset import Catalog


def test_nk_compatibility(fixtures_path):
    catalog = Catalog.from_yaml_uri(fixtures_path / "catalog_nk.yml")
    ds = catalog.datasets[0]
    proxies = False
    for proxy in ds.iterate():
        assert isinstance(proxy, ValueEntity)
        proxies = True
        break
    assert proxies
    assert ds.publisher.name == "Office of Foreign Assets Control"
    assert ds.maintainer.name == "OpenSanctions"

    catalog = Catalog._from_uri(
        "https://data.opensanctions.org/datasets/latest/index.json"
    )
    assert len(catalog.datasets) > 100
    for proxy in catalog.iterate():
        assert isinstance(proxy, ValueEntity)
        proxies = True
        break
    assert proxies
