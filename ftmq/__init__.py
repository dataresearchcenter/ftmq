from ftmq.io import (
    smart_read_proxies,
    smart_read_statements,
    smart_write_proxies,
    smart_write_statements,
)
from ftmq.query import A, C, G, M, P, Query, QueryError, Ref, Year
from ftmq.util import make_entity

__version__ = "5.0.0"
__all__ = [
    "smart_read_proxies",
    "smart_read_statements",
    "smart_write_proxies",
    "smart_write_statements",
    "Query",
    "QueryError",
    "M",
    "P",
    "G",
    "C",
    "A",
    "Ref",
    "Year",
    "make_entity",
]
