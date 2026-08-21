"""
Tantivy store
"""

import multiprocessing
import os
from functools import cache
from typing import Any, Iterable, cast

import tantivy
from anystore.logging import get_logger
from anystore.util import model_dump
from normality import normalize
from pydantic import ConfigDict

from ftmq.query import Query
from ftmq.search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq.search.store.base import BaseStore, get_filters

NUM_CPU = multiprocessing.cpu_count()

log = get_logger(__name__)


def or_(key: str, items: Iterable[str]) -> str:
    q = " OR ".join(f"{key}:{i}" for i in items)
    return f"({q})"


@cache
def make_schema() -> tantivy.Schema:
    schema_builder = tantivy.SchemaBuilder()
    schema_builder.add_text_field("id", tokenizer_name="raw", stored=True)
    schema_builder.add_text_field("datasets", tokenizer_name="raw", stored=True)
    schema_builder.add_text_field("schema", tokenizer_name="raw", stored=True)
    schema_builder.add_text_field("countries", tokenizer_name="raw", stored=True)
    schema_builder.add_text_field("fingerprints", tokenizer_name="raw", stored=True)
    schema_builder.add_text_field("caption", stored=True)
    schema_builder.add_text_field("names", stored=True)
    schema_builder.add_text_field("text", stored=False, tokenizer_name="en_stem")
    return schema_builder.build()


class TantivyStore(BaseStore):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    memory: bool = False
    index: tantivy.Index
    buffer: list[tantivy.Document] = []

    def __init__(self, **data: Any) -> None:
        schema = make_schema()
        if data.get("memory"):
            data["index"] = tantivy.Index(schema)
        else:
            uri = data["uri"][len("tantivy://") :]
            os.makedirs(uri, exist_ok=True)
            data["index"] = tantivy.Index(schema, uri)
        super().__init__(**data)

    def put(self, doc: EntityDocument) -> None:
        self.buffer.append(tantivy.Document(**model_dump(doc, clean=True)))
        if len(self.buffer) == 100_000:
            self.flush()

    def flush(self) -> None:
        log.info("Flushing %d items..." % len(self.buffer), store=self.uri)
        writer = self.index.writer(heap_size=15000000 * NUM_CPU, num_threads=NUM_CPU)
        for doc in self.buffer:
            writer.add_document(doc)
        writer.commit()
        writer.wait_merging_threads()
        self.index.reload()
        self.buffer = []

    def search(
        self, q: str, query: Query | None = None
    ) -> Iterable[EntitySearchResult]:
        searcher = self.index.searcher()
        t_query = self.parse_query(q, query)
        if t_query is None:
            return
        res = searcher.search(t_query)
        for score, doc_address in res.hits:
            doc = searcher.doc(doc_address)
            data: dict[str, Any] = dict(doc.to_dict())
            data["id"] = doc.get_first("id")
            data["caption"] = doc.get_first("caption")
            data["schema"] = doc.get_first("schema")
            data["names"] = data.get("names") or []
            yield EntitySearchResult(**data, score=score)

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        nq = normalize(q) or ""
        searcher = self.index.searcher()
        t_query = self.index.parse_query(f"{q}*", ["names"])
        res = searcher.search(t_query)
        for _, doc_address in res.hits:
            doc = searcher.doc(doc_address)
            for name in doc.get_all("names"):
                if (normalize(name) or "").startswith(nq):
                    yield AutocompleteResult(
                        id=cast(str, doc.get_first("id")), name=name
                    )

    def parse_query(self, q: str, query: Query | None = None) -> tantivy.Query | None:
        """Compile the search term and the query's filters into a tantivy
        query, or `None` if the filters can't match anything (an empty `in`)."""
        stmt = q
        for term in get_filters(query):
            if not term.values:
                if not term.negated:  # an empty `in` matches nothing
                    return None
                continue  # ... and excludes nothing when negated
            clause = or_(term.field, sorted(term.values))
            stmt += f" AND -{clause}" if term.negated else f" AND {clause}"
        return self.index.parse_query(stmt)
