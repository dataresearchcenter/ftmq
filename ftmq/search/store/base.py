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
    """One search-index predicate: `field` holds any of `values` (none, if `negated`).

    The terms of a query AND together.
    """

    field: str
    values: frozenset[str]
    negated: bool = False


def _leaf_term(leaf: Leaf) -> FilterTerm | None:
    """The term a leaf compiles to, or `None` (dropped) for an unindexed field."""
    if isinstance(leaf, DatasetLeaf):
        field, values = DATASETS, set(ensure_list(leaf.value))
    elif isinstance(leaf, SchemataLeaf):
        # the index holds the exact schema, so is-a expands to the schemata below
        field, values = SCHEMA, leaf.names
    elif isinstance(leaf, SchemaLeaf):
        field, values = SCHEMA, set(ensure_list(leaf.value))
    elif isinstance(leaf, GroupLeaf) and leaf.key == COUNTRIES:
        field, values = COUNTRIES, set(ensure_list(leaf.value))
    else:
        return None
    comparator = leaf.comparator
    if comparator in ("eq", "in"):
        return FilterTerm(field, frozenset(values))
    if comparator in ("not", "not_in"):
        return FilterTerm(field, frozenset(values), negated=True)
    raise QueryError(f"Comparator `{comparator}` is not expressible as a search filter")


def _collect(node: Expr | Leaf) -> list[FilterTerm]:
    """The ANDed terms of a node, raising for a shape a flat term list can't hold."""
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
    """Fold a same-field OR of positive terms into one term, raising otherwise."""
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
    """Compile a query's filter tree into the flat term list a search index applies.

    Only `datasets`, `schema` and `countries` are indexed, filters on other fields
    are dropped. A `not` / `not_in` comparator (or `~` around a single condition)
    becomes a negated term.

    Args:
        query: The query to compile (`None` means no filters).

    Returns:
        The terms to AND together.

    Raises:
        QueryError: For a shape ANDed terms can't express (a cross-field OR, a
            negated group, a comparator like `ilike` on an indexed field).
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
