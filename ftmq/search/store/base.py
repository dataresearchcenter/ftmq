from dataclasses import dataclass, replace
from typing import Iterable

from anystore.model import BaseModel
from banal import ensure_list

from ftmq.query import Query
from ftmq.query.exceptions import QueryError
from ftmq.query.leaves import DatasetLeaf, GroupLeaf, Leaf, SchemaLeaf, SchemataLeaf
from ftmq.query.nodes import OR, Expr
from ftmq.search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq.search.settings import Settings

settings = Settings()

# the indexed document fields a `Query` filter can address
DATASETS, SCHEMA, COUNTRIES = "datasets", "schema", "countries"


@dataclass(frozen=True)
class FilterTerm:
    """One search-index predicate: `field` holds any of `values` - or none of
    them, if `negated`. The terms of a query AND together."""

    field: str
    values: frozenset[str]
    negated: bool = False


def _leaf_term(leaf: Leaf) -> FilterTerm | None:
    """The term a single leaf compiles to, or `None` for a field the index
    doesn't hold (a property filter, an id, ...) - those are not expressible
    and are dropped, as they always have been."""
    if isinstance(leaf, DatasetLeaf):
        field, values = DATASETS, set(ensure_list(leaf.value))
    elif isinstance(leaf, SchemataLeaf):
        # an is-a filter matches every non-abstract schema below it - the
        # index holds the entity's exact schema
        field = SCHEMA
        values = set()
        for schema in leaf.schemata:
            values.add(schema.name)
            values.update(d.name for d in schema.descendants if not d.abstract)
    elif isinstance(leaf, SchemaLeaf):
        field, values = SCHEMA, set(ensure_list(leaf.value))
    elif isinstance(leaf, GroupLeaf) and leaf.key == COUNTRIES:
        field, values = COUNTRIES, set(ensure_list(leaf.value))
    else:
        return None
    comparator = str(leaf.comparator)
    if comparator in ("eq", "in"):
        return FilterTerm(field, frozenset(values))
    if comparator in ("not", "not_in"):
        return FilterTerm(field, frozenset(values), negated=True)
    raise QueryError(f"Comparator `{comparator}` is not expressible as a search filter")


def _collect(node: Expr | Leaf) -> list[FilterTerm]:
    """The ANDed terms of one node, raising for a shape the flat term list
    cannot represent."""
    if isinstance(node, Leaf):
        term = _leaf_term(node)
        return [] if term is None else [term]
    if node.connector == OR and len(node.children) > 1:
        terms = [_or_term(node)]
    else:
        terms = [t for child in node.children for t in _collect(child)]
    if node.negated:
        if not terms:  # nothing indexed under it, so nothing to negate
            return []
        if len(terms) > 1:
            raise QueryError("A negated group is not expressible as a search filter")
        return [replace(terms[0], negated=not terms[0].negated)]
    return terms


def _or_term(node: Expr) -> FilterTerm:
    """Fold a same-field OR of positive terms into one term. Anything else -
    a cross-field OR, a negated or unindexed alternative - would silently
    narrow the query, so it raises instead."""
    terms: list[FilterTerm] = []
    for child in node.children:
        child_terms = _collect(child)
        if len(child_terms) != 1 or child_terms[0].negated:
            raise QueryError("This OR is not expressible as a search filter")
        terms.append(child_terms[0])
    fields = {t.field for t in terms}
    if len(fields) > 1:
        raise QueryError("A cross-field OR is not expressible as a search filter")
    values = frozenset[str]().union(*(t.values for t in terms))
    return FilterTerm(fields.pop(), values)


def get_filters(query: Query | None) -> list[FilterTerm]:
    """Compile a query's filter tree into the flat term list a search index can
    apply.

    The index holds three filterable fields (`datasets`, `schema`,
    `countries`); a filter on anything else is dropped. A `not` / `not_in`
    comparator (or a `~` around a single condition) becomes a negated term
    instead of being read as a positive one, and a shape that cannot be
    expressed as ANDed terms - a cross-field OR, a negated group, a comparator
    like `ilike` on an indexed field - raises.

    Args:
        query: The query to compile (`None` means no filters).

    Returns:
        The terms to AND together.

    Raises:
        QueryError: If the query's filter tree is not expressible.
    """
    if query is None or query.q is None:
        return []
    return _collect(query.q)


class BaseStore(BaseModel):
    uri: str = settings.uri

    def put(self, doc: EntityDocument) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError

    def search(
        self, q: str, query: Query | None = None
    ) -> Iterable[EntitySearchResult]:
        raise NotImplementedError

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        raise NotImplementedError
