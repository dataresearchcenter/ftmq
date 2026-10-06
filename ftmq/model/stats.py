from collections import Counter
from typing import Any, Iterable

from anystore.model import BaseModel
from followthemoney import model
from followthemoney.dataset.util import PartialDate
from pydantic import model_validator

from ftmq.types import Entities, Entity
from ftmq.util import get_country_name, get_year_from_iso


class Schema(BaseModel):
    name: str
    count: int
    label: str
    plural: str

    def __init__(self, **data):
        schema = model[data["name"]]
        data["label"] = schema.label
        data["plural"] = schema.plural
        super().__init__(**data)


class Country(BaseModel):
    code: str
    count: int
    label: str | None = None

    @model_validator(mode="before")
    @classmethod
    def clean_label(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "label" not in data:
                data["label"] = get_country_name(data["code"]) or data["code"].upper()
        return data


class Schemata(BaseModel):
    total: int = 0
    countries: list[Country] = []
    schemata: list[Schema] = []


def _schemata(schemata: Counter, countries: Counter) -> Schemata:
    return Schemata(
        schemata=[Schema(name=k, count=v) for k, v in schemata.items()],
        countries=[Country(code=k, count=v) for k, v in countries.items()],
        total=schemata.total(),
    )


class DatasetStats(BaseModel):
    things: Schemata = Schemata()
    intervals: Schemata = Schemata()
    entity_count: int = 0
    start: PartialDate | None = None
    end: PartialDate | None = None
    countries: set[str] = set()

    @property
    def years(self) -> tuple[int | None, int | None]:
        """Return the min / max year of the coverage."""
        return get_year_from_iso(self.start), get_year_from_iso(self.end)


class Collector:
    def __init__(self):
        self.entity_count = 0
        self.things = Counter()
        self.things_countries = Counter()
        self.intervals = Counter()
        self.intervals_countries = Counter()
        self.start = set()
        self.end = set()

    def collect(self, proxy: Entity) -> None:
        # the sql store's buckets: `Event` counts in both, `Page` only in the total
        self.entity_count += 1
        if proxy.schema.is_a("Thing"):
            self.things[proxy.schema.name] += 1
            for country in proxy.countries:
                self.things_countries[country] += 1
        if proxy.schema.is_a("Interval"):
            self.intervals[proxy.schema.name] += 1
            for country in proxy.countries:
                self.intervals_countries[country] += 1
        self.start.update(proxy.get("startDate", quiet=True))
        self.start.update(proxy.get("date", quiet=True))
        self.end.update(proxy.get("endDate", quiet=True))
        self.end.update(proxy.get("date", quiet=True))

    def export(self) -> DatasetStats:
        return DatasetStats(
            start=min(self.start) if self.start else None,
            end=max(self.end) if self.end else None,
            countries=set(self.things_countries) | set(self.intervals_countries),
            things=_schemata(self.things, self.things_countries),
            intervals=_schemata(self.intervals, self.intervals_countries),
            entity_count=self.entity_count,
        )

    def to_dict(self) -> dict[str, Any]:
        data = self.export()
        return data.model_dump(mode="json")

    def apply(self, proxies: Entities) -> Entities:
        """Collect coverage lazily while passing the proxies through."""
        for proxy in proxies:
            self.collect(proxy)
            yield proxy

    def collect_many(self, proxies: Entities) -> DatasetStats:
        for proxy in proxies:
            self.collect(proxy)
        return self.export()


def compile_stats(
    things: Iterable[tuple[str, int]] = (),
    intervals: Iterable[tuple[str, int]] = (),
    things_countries: Iterable[tuple[str | None, int]] = (),
    intervals_countries: Iterable[tuple[str | None, int]] = (),
    date_range: tuple[Any, Any] | None = None,
    entity_count: int | None = None,
) -> DatasetStats:
    """Compile `DatasetStats` from pre-computed `(group, count)` aggregate rows.

    Without `entity_count`, things + intervals is used, which is not a distinct
    count (schemata in both buckets count twice, those in neither not at all).
    """
    c = Collector()
    c.things = Counter(dict(things))
    c.intervals = Counter(dict(intervals))
    c.things_countries = Counter({k: v for k, v in things_countries if k is not None})
    c.intervals_countries = Counter(
        {k: v for k, v in intervals_countries if k is not None}
    )
    if entity_count is None:
        entity_count = c.things.total() + c.intervals.total()
    c.entity_count = entity_count
    stats = c.export()
    if date_range is not None:
        start, end = date_range
        if start:
            stats.start = start
        if end:
            stats.end = end
    return stats
