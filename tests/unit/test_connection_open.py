from unittest import mock

import pytest

from dbt.adapters.confluent.connections import ConfluentConnectionManager, ConfluentCredentials
from dbt.adapters.contracts.connection import Connection, ConnectionState


@pytest.fixture
def connect():
    """Patched `confluent_sql.connect`, so `open()` never touches the network."""
    with mock.patch("dbt.adapters.confluent.connections.confluent_sql.connect") as connect:
        yield connect


def open_connection(profile: dict):
    """Open a connection for `profile` (as it would appear in profiles.yml, minus `type`)."""
    credentials = ConfluentCredentials.from_dict(
        {
            "organization_id": "org-123",
            "environment_id": "env-xyz",
            "dbname": "my_db",
            "cloud_provider": "aws",
            "cloud_region": "us-west-1",
            **profile,
        }
    )
    connection = Connection(type="confluent", name="test", credentials=credentials)
    return ConfluentConnectionManager.open(connection)


class TestOpenAuthMode:
    def test_oauth_auth_is_passed_to_driver(self, connect):
        connection = open_connection({"auth": "oauth"})

        assert connect.call_args.kwargs["auth"] == "oauth"
        assert connection.state == ConnectionState.OPEN
        assert connection.handle is connect.return_value

    def test_auth_defaults_to_api_key(self, connect):
        flink_api_key = "my-key"
        flink_api_secret = "my-secret"

        open_connection({"flink_api_key": flink_api_key, "flink_api_secret": flink_api_secret})

        kwargs = connect.call_args.kwargs
        assert kwargs["auth"] == "api_key"
        assert kwargs["flink_api_key"] == flink_api_key
        assert kwargs["flink_api_secret"] == flink_api_secret


class TestOpenTimeouts:
    def test_timeouts_are_passed_to_driver_without_warning(self, connect):
        import warnings

        from confluent_sql import Timeouts

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            open_connection({"flink_api_key": "k", "flink_api_secret": "s"})

        kwargs = connect.call_args.kwargs
        assert "http_timeout_secs" not in kwargs
        timeouts = kwargs["timeouts"]
        assert timeouts.httpx_request_timeout_secs == 60
        # A results window no longer than a single request leaves no room for a retry.
        assert timeouts.statement_results_response_wallclock_secs > 60
        # Constructing Timeouts re-validates and would warn on a bad window.
        Timeouts(
            httpx_request_timeout_secs=timeouts.httpx_request_timeout_secs,
            statement_results_response_wallclock_secs=(
                timeouts.statement_results_response_wallclock_secs
            ),
        )
