"""In-memory sqlite engines must be safe to share across threads.

Under a threaded server (granian ``mt`` / the anyio threadpool) the resolver
and every ``LakeStore`` share one process-cached ``sqlite:///:memory:`` engine;
without ``check_same_thread=False`` + ``StaticPool`` a connection opened on one
worker thread is closed on another and raises ``sqlite3.ProgrammingError``.
"""

import threading
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from ftmq.store.base import _memory_engine, get_engine, get_resolver


def _closes_across_threads(engine: Engine) -> bool:
    """Open a connection on a worker thread, close it on the main thread.

    Returns ``True`` when no error is raised (thread-safe), ``False`` when
    sqlite rejects the cross-thread close.
    """
    box: dict[str, Any] = {}

    def worker() -> None:
        conn = engine.connect()
        conn.execute(text("select 1"))
        box["conn"] = conn

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    try:
        box["conn"].close()
        return True
    except Exception:
        return False


def test_memory_engine_thread_safe() -> None:
    engine = _memory_engine()
    assert isinstance(engine.pool, StaticPool)
    assert _closes_across_threads(engine)


def test_get_engine_memory_thread_safe() -> None:
    # The LakeStore builds its (faked) engine via get_engine("sqlite:///:memory:"),
    # which must be cross-thread safe as well, and shared per url.
    engine = get_engine("sqlite:///:memory:")
    assert engine is get_engine("sqlite:///:memory:")
    assert _closes_across_threads(engine)


def test_get_resolver_default_in_memory() -> None:
    assert get_resolver() is not None
