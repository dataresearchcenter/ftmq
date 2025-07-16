from collections import Counter
from datetime import datetime
from typing import Any

from followthemoney import model
from pydantic import BaseModel, model_validator

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
                data["label"] = (
                    get_country_name(data["code"]) or data["code"].uppercase()
                )
        return data


class Schemata(BaseModel):
    total: int = 0
    countries: list[Country] = []
    schemata: list[Schema] = []


class DatasetStats(BaseModel):
    things: Schemata = Schemata()
    intervals: Schemata = Schemata()
    entity_count: int = 0
    start: datetime | None = None
    end: datetime | None = None
    countries: set[str] = set()

    @property
    def years(self) -> tuple[int | None, int | None]:
        """
        Return min / max year extend
        """
        return get_year_from_iso(self.start), get_year_from_iso(self.end)


class Collector:
    def __init__(self):
        self.things = Counter()
        self.things_countries = Counter()
        self.intervals = Counter()
        self.intervals_countries = Counter()
        self.start = set()
        self.end = set()

    def collect(self, proxy: Entity) -> None:
        if proxy.schema.is_a("Thing"):
            self.things[proxy.schema.name] += 1
            for country in proxy.countries:
                self.things_countries[country] += 1
        else:
            self.intervals[proxy.schema.name] += 1
            for country in proxy.countries:
                self.intervals_countries[country] += 1
        self.start.update(proxy.get("startDate", quiet=True))
        self.start.update(proxy.get("date", quiet=True))
        self.end.update(proxy.get("endDate", quiet=True))
        self.end.update(proxy.get("date", quiet=True))

    def export(self) -> DatasetStats:
        start = min(self.start) if self.start else None
        end = max(self.end) if self.end else None
        countries = set(self.things_countries.keys()) | set(
            self.intervals_countries.keys()
        )
        things = Schemata(
            schemata=[Schema(name=k, count=v) for k, v in self.things.items()],
            countries=[
                Country(code=k, count=v) for k, v in self.things_countries.items()
            ],
            total=self.things.total(),
        )
        intervals = Schemata(
            schemata=[Schema(name=k, count=v) for k, v in self.intervals.items()],
            countries=[
                Country(code=k, count=v) for k, v in self.intervals_countries.items()
            ],
            total=self.intervals.total(),
        )
        return DatasetStats(
            start=start,
            end=end,
            countries=countries,
            things=things,
            intervals=intervals,
            entity_count=things.total + intervals.total,
        )

    def to_dict(self) -> dict[str, Any]:
        data = self.export()
        return data.model_dump(mode="json")

    def apply(self, proxies: Entities) -> Entities:
        """
        Generate coverage from an input stream of proxies
        This returns a generator again, so actual collection of coverage stats
        will happen if the actual generator is executed
        """
        for proxy in proxies:
            self.collect(proxy)
            yield proxy

    def collect_many(self, proxies: Entities) -> DatasetStats:
        for proxy in proxies:
            self.collect(proxy)
        return self.export()
