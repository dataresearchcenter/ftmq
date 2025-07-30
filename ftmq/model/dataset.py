from datetime import datetime
from typing import Literal

from anystore.io import logged_items
from anystore.types import SDict
from followthemoney.dataset import DataPublisher
from followthemoney.dataset.dataset import DatasetModel as _DatasetModel
from pydantic import AnyUrl, HttpUrl
from rigour.mime.types import FTM

from ftmq.model.mixins import BaseModel
from ftmq.model.stats import DatasetStats
from ftmq.types import Entities

ContentType = Literal["documents", "structured", "mixed"]


class Dataset(BaseModel, _DatasetModel):
    maintainer: DataPublisher | None = None
    stats: DatasetStats = DatasetStats()
    git_repo: AnyUrl | None = None
    content_type: ContentType | None = None
    uri: str | None = None

    def iterate(self) -> Entities:
        from ftmq.io import smart_read_proxies  # FIXME

        for resource in self.resources:
            if resource.mime_type == FTM and resource.url:
                yield from logged_items(
                    smart_read_proxies(str(resource.url)),
                    "Read",
                    1_000,
                    uri=resource.url,
                    item_name="Proxy",
                    dataset=self.name,
                )

    def apply_stats(self, stats: DatasetStats) -> None:
        self.entity_count = stats.entity_count
        self.thing_count = stats.things.total
        self.stats = stats


def ensure_dataset(data: SDict | Dataset) -> Dataset:
    if isinstance(data, Dataset):
        return data
    return Dataset(**data)


class Catalog(BaseModel):
    name: str = "catalog"
    title: str = "Catalog"
    datasets: list[Dataset] = []
    updated_at: datetime | None = None
    description: str | None = None
    maintainer: DataPublisher | None = None
    publisher: DataPublisher | None = None
    url: HttpUrl | None = None
    uri: str | None = None
    logo_url: HttpUrl | None = None
    git_repo: AnyUrl | None = None

    def iterate(self) -> Entities:
        for dataset in self.datasets:
            yield from dataset.iterate()

    @property
    def names(self) -> set[str]:
        """Get the names of all datasets in the catalog."""
        return {d.name for d in self.datasets}
