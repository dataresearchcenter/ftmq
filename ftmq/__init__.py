from ftmq.io import smart_read_proxies, smart_write_proxies
from ftmq.query import C, G, M, P, Query, QueryError
from ftmq.util import make_entity

__version__ = "4.10.0"
__all__ = [
    "smart_read_proxies",
    "smart_write_proxies",
    "Query",
    "QueryError",
    "M",
    "P",
    "G",
    "C",
    "make_entity",
]
