from ftmq.io import smart_read_proxies, smart_write_proxies
from ftmq.query import A, C, G, M, P, Query, QueryError, Ref, Year
from ftmq.util import make_entity

__version__ = "4.10.1"
__all__ = [
    "smart_read_proxies",
    "smart_write_proxies",
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
