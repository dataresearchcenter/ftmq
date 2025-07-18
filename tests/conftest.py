import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests
from followthemoney import StatementEntity

from ftmq.io import smart_read_proxies

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()
AUTHORITIES = "eu_authorities.ftm.json"
DONATIONS = "donations.ijson"


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
