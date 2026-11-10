"""The PostgreSQL tests create roles and databases: they must never reach a remote server."""

import pytest

from tests.conftest import local_test_url

LOCAL = "postgresql+psycopg2://postgres:test@127.0.0.1:55860/postgres"


def test_a_local_url_is_accepted():
    assert local_test_url(LOCAL, environment={}).host == "127.0.0.1"


@pytest.mark.parametrize(
    ("url", "environment", "named"),
    [
        ("postgresql+psycopg2://u:p@staging.example.com/db", {}, "staging.example.com"),
        # libpq prefers these over the URL's host
        (LOCAL + "?host=remote.example", {}, "host"),
        (LOCAL + "?hostaddr=203.0.113.7", {}, "hostaddr"),
        (LOCAL + "?service=production", {}, "service"),
        (LOCAL, {"PGHOSTADDR": "203.0.113.7"}, "PGHOSTADDR"),
        (LOCAL, {"PGSERVICE": "production"}, "PGSERVICE"),
    ],
)
def test_a_url_that_can_reach_another_server_is_refused(url, environment, named):
    with pytest.raises(RuntimeError, match=named):
        local_test_url(url, environment=environment)
