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


class TestOpenOrganizationId:
    """organization_id is optional under oauth / a Global key: omitted means "" for the driver
    to infer, while a supplied value is passed through for it to verify."""

    @pytest.mark.parametrize(
        "profile",
        [
            {"auth": "oauth"},
            {"global_api_key": "key", "global_api_secret": "secret"},
        ],
        ids=["oauth", "global_key"],
    )
    def test_omitted_is_passed_as_empty_string(self, connect, profile):
        open_connection({**profile, "organization_id": None})

        assert connect.call_args.kwargs["organization_id"] == ""

    def test_blank_from_dbt_init_is_treated_as_omitted(self, connect):
        open_connection({"auth": "oauth", "organization_id": ""})

        assert connect.call_args.kwargs["organization_id"] == ""

    @pytest.mark.parametrize(
        "profile",
        [
            {"auth": "oauth"},
            {"global_api_key": "key", "global_api_secret": "secret"},
        ],
        ids=["oauth", "global_key"],
    )
    def test_supplied_is_passed_through(self, connect, profile):
        open_connection({**profile, "organization_id": "org-456"})

        assert connect.call_args.kwargs["organization_id"] == "org-456"


class TestCredentialsOrganizationId:
    BASE = {"environment_id": "env-xyz", "dbname": "my_db", "cloud_provider": "aws"}

    def credentials(self, **extra):
        return ConfluentCredentials.from_dict(
            {**self.BASE, "cloud_region": "us-west-1", "auth": "oauth", **extra}
        )

    def test_parses_without_organization_id(self):
        assert self.credentials().organization_id is None

    def test_parses_without_organization_id_with_global_key(self):
        """The field wholly absent (not an explicit None) next to a Global key pair, the shape a
        profile written by hand for the inferring case has."""
        credentials = ConfluentCredentials.from_dict(
            {
                **self.BASE,
                "cloud_region": "us-west-1",
                "global_api_key": "key",
                "global_api_secret": "secret",
            }
        )

        assert credentials.organization_id is None
        assert credentials.auth == "api_key"

    def test_unique_field_without_organization_id(self):
        assert self.credentials().unique_field == "aws-us-west-1"

    def test_unique_field_with_organization_id(self):
        assert self.credentials(organization_id="org-123").unique_field == "aws-us-west-1-org-123"
