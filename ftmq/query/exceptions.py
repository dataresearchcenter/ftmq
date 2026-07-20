class QueryError(ValueError):
    """Raised for an invalid query: an unknown field, an invalid comparator,
    or a query that cannot be projected to the requested serialization.

    Subclasses `ValueError` so existing `except ValueError` handlers keep
    working.
    """
