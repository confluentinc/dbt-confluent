from unittest import mock

import pytest

from dbt.adapters.confluent.connections import ConfluentConnectionManager
from dbt.adapters.contracts.connection import Connection, ConnectionState
from tests.unit._helpers import make_credentials

SCOPED_FIELDS = (
    "cloud_api_key",
    "cloud_api_secret",
    "tableflow_api_key",
    "tableflow_api_secret",
    "database_kafka_cluster_id",
)


@pytest.fixture
def connect():
    """Patched `confluent_sql.connect`, so `open()` never touches the network."""
    with mock.patch("dbt.adapters.confluent.connections.confluent_sql.connect") as connect:
        yield connect


def open_connection(profile: dict):
    """Open a connection for `profile` (as it would appear in profiles.yml, minus `type`)."""
    connection = Connection(type="confluent", name="test", credentials=make_credentials(**profile))
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


class TestOpenScopedCredentials:
    @pytest.mark.parametrize(
        "scoped",
        [
            pytest.param({}, id="unset"),
            pytest.param({field: f"{field}-value" for field in SCOPED_FIELDS}, id="set"),
        ],
    )
    def test_scoped_keys_and_cluster_id_are_passed_to_driver(self, connect, scoped):
        open_connection({"flink_api_key": "my-key", "flink_api_secret": "my-secret", **scoped})

        kwargs = connect.call_args.kwargs
        for field in SCOPED_FIELDS:
            assert kwargs[field] == scoped.get(field)
