import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
import requests
from followthemoney import StatementEntity

from ftmq.io import smart_read_proxies

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()
AUTHORITIES = "eu_authorities.ftm.json"
DONATIONS = "donations.ijson"

# --- ftmq.api test environment ---------------------------------------------
# Must be set at conftest import time: pytest collection imports test modules
# (and with them the `ftmq.api.*` module-level `Settings()` and the `Datasets`
# Literal) before any fixture runs. Both stores are file-based sqlite: the
# entity store and search store engines are then usable across the TestClient
# event-loop thread, and the shared `:memory:` engine used by other tests
# stays uncontaminated.
_API_TMP = tempfile.mkdtemp(prefix="ftmq-api-test-")
API_STORE_URI = f"sqlite:///{_API_TMP}/store.db"
API_SEARCH_URI = f"sqlite:///{_API_TMP}/search.db"
os.environ["FTMQ_API_CATALOG"] = str(FIXTURES_PATH / "api_catalog.json")
os.environ["FTMQ_API_STORE_URI"] = API_STORE_URI
os.environ["FTMQ_SEARCH_URI"] = API_SEARCH_URI


@pytest.fixture(scope="module")
def fixtures_path():
    return FIXTURES_PATH


@pytest.fixture(scope="module")
def proxies():
    proxies = []
    proxies.extend(
        smart_read_proxies(FIXTURES_PATH / AUTHORITIES, entity_type=StatementEntity)
    )
    proxies.extend(
        smart_read_proxies(FIXTURES_PATH / DONATIONS, entity_type=StatementEntity)
    )
    return proxies


@pytest.fixture(scope="module")
def eu_authorities():
    return [
        x
        for x in smart_read_proxies(
            FIXTURES_PATH / AUTHORITIES, entity_type=StatementEntity
        )
    ]


@pytest.fixture(scope="module")
def donations():
    return [
        x
        for x in smart_read_proxies(
            FIXTURES_PATH / DONATIONS, entity_type=StatementEntity
        )
    ]


@pytest.fixture(scope="module")
def things(donations):
    return [p for p in donations if p.schema.is_a("Thing")]


@pytest.fixture(scope="session")
def api_client():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from ftmq.io import smart_write_proxies
    from ftmq.search.logic import index_entities
    from ftmq.search.store import get_store as get_search_store

    # session scope: read the fixture files directly (the `proxies` fixture is
    # module-scoped)
    proxies = [
        *smart_read_proxies(FIXTURES_PATH / AUTHORITIES, entity_type=StatementEntity),
        *smart_read_proxies(FIXTURES_PATH / DONATIONS, entity_type=StatementEntity),
    ]
    smart_write_proxies(API_STORE_URI, proxies)
    # index only Things for search (intervals like `Payment` are not useful
    # fulltext results)
    things = [p for p in proxies if p.schema.is_a("Thing")]
    index_entities(things, get_search_store(uri=API_SEARCH_URI))

    # deferred import: after env setup and store population
    from ftmq.api.app import app

    return TestClient(app)


# https://pawamoy.github.io/posts/local-http-server-fake-files-testing-purposes/
def spawn_and_wait_server():
    process = subprocess.Popen(
        [sys.executable, "-m", "http.server", "-d", FIXTURES_PATH]
    )
    while True:
        try:
            requests.get("http://localhost:8000")
        except Exception:
            time.sleep(1)
        else:
            break
    return process


@pytest.fixture(scope="session", autouse=True)
def http_server():
    process = spawn_and_wait_server()
    yield process
    process.kill()
    process.wait()
    return
