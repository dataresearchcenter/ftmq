class QueryError(ValueError):
    """Raised for an invalid query.

    An unknown field or comparator, or a query the requested serialization cannot
    express. Subclasses `ValueError`.
    """
