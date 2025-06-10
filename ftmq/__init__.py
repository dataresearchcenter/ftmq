from ftmq.io import smart_read_proxies, smart_stream_proxies, smart_write_proxies
from ftmq.query import Query
from ftmq.util import make_proxy

__version__ = "0.8.6"
__all__ = [
    "smart_read_proxies",
    "smart_stream_proxies",
    "smart_write_proxies",
    "Query",
    "make_proxy",
]
