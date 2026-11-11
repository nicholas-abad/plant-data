"""Shared fixtures: a throwaway database on a LOCAL PostgreSQL for the tests that need real SQL.

Start one with
    docker run -d --name plant-data-test-pg17 -e POSTGRES_PASSWORD=test -p 127.0.0.1:55860:5432 postgres:17
and run pytest with
    PLANT_DATA_TEST_PG_URL=postgresql+psycopg2://postgres:test@127.0.0.1:55860/postgres
Without the variable these tests skip. They never run against a remote host.
"""

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
# libpq takes the server from these too, whatever the URL's host says.
ROUTING_QUERY_KEYS = {"host", "hostaddr", "service"}
ROUTING_ENVIRONMENT = ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE")


def local_test_url(admin_url: str, environment=os.environ):
    """The parsed URL, or RuntimeError when it could reach anything but this machine."""
    url = make_url(admin_url)
    if url.host not in LOCAL_HOSTS:
        raise RuntimeError(f"PLANT_DATA_TEST_PG_URL must point at localhost, not {url.host}")
    rerouted = sorted(ROUTING_QUERY_KEYS & set(url.query)) + [k for k in ROUTING_ENVIRONMENT if environment.get(k)]
    if rerouted:
        raise RuntimeError(
            f"PLANT_DATA_TEST_PG_URL: {', '.join(rerouted)} can send the connection to another server; remove it"
        )
    return url


@pytest.fixture
def pg_url():
    """URL of a fresh, empty database (dropped afterwards) on the local test server."""
    admin_url = os.environ.get("PLANT_DATA_TEST_PG_URL")
    if not admin_url:
        pytest.skip("PLANT_DATA_TEST_PG_URL not set (a local postgres:17; see tests/conftest.py)")
    url = local_test_url(admin_url)
    name = f"plant_data_test_{uuid.uuid4().hex[:12]}"
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f"CREATE DATABASE {name}"))
    try:
        yield url.set(database=name)
    finally:
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        admin.dispose()
