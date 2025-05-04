from datetime import datetime
from typing import Iterable, Literal, Self, TypeVar

from anystore.io import logged_items
from nomenklatura.dataset.dataset import Dataset as NKDataset
from normality import slugify
from pydantic import AnyUrl, HttpUrl
from rigour.mime.types import FTM

from ftmq.enums import Categories, Frequencies
from ftmq.model.coverage import Coverage, DatasetStats, Schemata
from ftmq.model.mixins import BaseModel
from ftmq.types import CEGenerator, SDict
from ftmq.util import make_dataset

Frequencies = Literal[tuple(Frequencies)]
Categories = Literal[tuple(Categories)]
ContentType = Literal["documents", "structured", "mixed"]

C = TypeVar("C", bound="Catalog")
DS = TypeVar("DS", bound="Dataset")


class Publisher(BaseModel):
    name: str
    url: HttpUrl
    description: str | None = None
    country: str | None = None
    country_label: str | None = None
    official: bool = False
    logo_url: HttpUrl | None = None


class Resource(BaseModel):
    name: str
    url: AnyUrl
    title: str | None = None
    checksum: str | None = None
    timestamp: datetime | None = None
    mime_type: str | None = None
    mime_type_label: str | None = None
    size: int | None = 0


class Maintainer(BaseModel):
    """
    this is our own addition
    """

    name: str
    description: str | None = None
    country: str | None = None
    country_label: str | None = None
    url: HttpUrl | None = None
    logo_url: HttpUrl | None = None


class Dataset(BaseModel):
    # nk props
    name: str
    prefix: str | None = None
    title: str | None = None
    license: str | None = None
    summary: str | None = None
    description: str | None = None
    url: HttpUrl | None = None
    updated_at: datetime | None = None
    version: str | None = None
    category: Categories | None = None
    publisher: Publisher | None = None
    coverage: Coverage | None = None
    things: Schemata | None = None
    intervals: Schemata | None = None
    entity_count: int | None = 0
    resources: list[Resource] | None = []
    index_url: AnyUrl | None = None

    # own addition / aleph
    catalog: str | None = None
    countries: list[str] | None = []
    info_url: HttpUrl | None = None
    data_url: HttpUrl | None = None
    aleph_url: HttpUrl | None = None
    tags: list[str] | None = []
    content_type: ContentType | None = "structured"
    total_file_size: int | None = 0

    git_repo: AnyUrl | None = None
    uri: str | None = None
    maintainer: Maintainer | None = None

    def __init__(self, **data):
        data["updated_at"] = data.get("updated_at") or datetime.utcnow().replace(
            microsecond=0
        )
        super().__init__(**data)
        self.prefix = self.prefix or data.get("prefix") or slugify(self.name)
        self.coverage = self.coverage or Coverage()
        self.title = self.title or self.name.title()

    def iterate(self) -> CEGenerator:
        from ftmq.io import smart_read_proxies  # FIXME

        for resource in self.resources:
            if resource.mime_type == FTM:
                yield from logged_items(
                    smart_read_proxies(resource.url),
                    "Read",
                    1_000,
                    uri=resource.url,
                    item_name="Proxy",
                    dataset=self.name,
                )

    def apply_stats(self, stats: DatasetStats) -> None:
        self.entity_count = stats.entity_count
        self.coverage = stats.coverage
        self.things = stats.things
        self.intervals = stats.intervals


def ensure_dataset(data: SDict | Dataset) -> Dataset:
    if isinstance(data, Dataset):
        return data
    return Dataset(**data)


class Catalog(BaseModel):
    name: str | None = "default"
    title: str | None = "Catalog"
    datasets: list[Dataset] | None = []
    updated_at: datetime | None = None
    description: str | None = None
    maintainer: Maintainer | None = None
    publisher: Publisher | None = None
    url: HttpUrl | None = None
    uri: str | None = None
    logo_url: HttpUrl | None = None
    git_repo: AnyUrl | None = None

    def __init__(self, **data):
        if "name" not in data:
            data["name"] = "Catalog"
        data["datasets"] = [ensure_dataset(d) for d in data.get("datasets", [])]
        super().__init__(**data)

    def get(self, name: str) -> Dataset | None:
        for dataset in self.datasets:
            if dataset.name == name:
                return dataset

    def get_scope(self) -> NKDataset:
        # FIXME clarify
        ds = NKDataset(
            {
                "name": slugify(self.name),
                "title": self.name.title(),
                "children": self.names,
            },
        )
        ds.children = {make_dataset(n) for n in self.names}
        return ds

    def iterate(self) -> CEGenerator:
        for dataset in self.datasets:
            yield from dataset.iterate()

    @property
    def names(self) -> set[str]:
        names = set()
        for dataset in self.datasets:
            names.add(dataset.name)
        return names

    @classmethod
    def from_names(cls, names: Iterable[str]) -> Self:
        return cls(datasets=[Dataset(name=n) for n in names])
