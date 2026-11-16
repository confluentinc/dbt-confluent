from unittest import mock

import pytest
from confluent_sql.exceptions import (
    AmbiguousClusterError,
    AmbiguousOrganizationError,
    ClusterMismatchError,
    InterfaceError,
    OperationalError,
)

from dbt.adapters.confluent.connections import ConfluentConnectionManager
from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.exceptions import FailedToConnectError
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


GLOBAL = {"global_api_key": "key", "global_api_secret": "secret"}
OMITTED = {"organization_id": None, "cloud_provider": None, "cloud_region": None}
INFERRING_PROFILES = [
    pytest.param({"auth": "oauth"}, id="oauth"),
    pytest.param(GLOBAL, id="global_key"),
]


class TestOpenInference:
    """The organization and cloud settings are optional with `auth: oauth` or a Global key: the
    omitted ones reach the driver as omitted, for it to infer; supplied ones are passed through,
    for it to verify."""

    @pytest.mark.parametrize("profile", INFERRING_PROFILES)
    def test_omitted_settings_are_passed_as_omitted(self, connect, profile):
        open_connection({**profile, **OMITTED})

        kwargs = connect.call_args.kwargs
        assert kwargs["organization_id"] == ""
        assert kwargs["cloud_provider"] is None
        assert kwargs["cloud_region"] is None
        assert kwargs["endpoint"] is None
        # What the driver infers the cloud settings from.
        assert kwargs["database"] == "my_db"

    def test_blank_organization_id_from_dbt_init_is_omitted(self, connect):
        open_connection({"auth": "oauth", "organization_id": ""})

        assert connect.call_args.kwargs["organization_id"] == ""

    @pytest.mark.parametrize("profile", INFERRING_PROFILES)
    def test_supplied_settings_are_passed_through(self, connect, profile):
        open_connection({**profile, "organization_id": "org-456"})

        kwargs = connect.call_args.kwargs
        assert kwargs["organization_id"] == "org-456"
        assert kwargs["cloud_provider"] == "aws"
        assert kwargs["cloud_region"] == "us-west-1"

    @pytest.mark.parametrize("supplied", ["cloud_provider", "cloud_region"])
    def test_half_a_cloud_pair_is_passed_for_the_driver_to_complete(self, connect, supplied):
        profile = {**GLOBAL, **OMITTED, supplied: "aws" if supplied == "cloud_provider" else "x"}

        open_connection(profile)

        kwargs = connect.call_args.kwargs
        other = "cloud_region" if supplied == "cloud_provider" else "cloud_provider"
        assert kwargs[supplied] is not None
        assert kwargs[other] is None


class TestOpenExplainsInferenceFailures:
    """The driver's inference errors name `connect()` arguments; a dbt user needs the profile
    fields. Everything else propagates untouched."""

    def failing_open(self, connect, error, profile=None):
        connect.side_effect = error
        connection = Connection(
            type="confluent",
            name="test",
            credentials=make_credentials(**(profile or {**GLOBAL, **OMITTED})),
        )
        with pytest.raises(FailedToConnectError) as exc_info:
            ConfluentConnectionManager.open(connection)
        assert exc_info.value.__cause__ is error
        assert connection.state == ConnectionState.FAIL
        assert connection.handle is None
        return str(exc_info.value)

    def test_ambiguous_organization_lists_candidates_and_names_the_field(self, connect):
        error = AmbiguousOrganizationError([("org-1", "Acme"), ("org-2", None)])

        message = self.failing_open(connect, error)

        assert "`organization_id`" in message
        assert "org-1 (Acme)" in message
        assert "org-2" in message

    def test_ambiguous_cluster_lists_candidates_and_names_the_field(self, connect):
        error = AmbiguousClusterError(
            "my_db", "env-xyz", [("lkc-1", "aws", "us-west-1"), ("lkc-2", "gcp", "us-east1")]
        )

        message = self.failing_open(connect, error)

        assert "`database_kafka_cluster_id`" in message
        assert "lkc-1 (aws, us-west-1)" in message
        assert "lkc-2 (gcp, us-east1)" in message

    def test_cluster_mismatch_names_profile_fields_not_driver_parameters(self, connect):
        error = ClusterMismatchError(
            "lkc-1",
            [("database", "my_db", "other_db"), ("cloud_region", "us-east-1", "us-west-2")],
        )

        message = self.failing_open(connect, error)

        assert "lkc-1" in message
        # The driver calls the cluster name "database"; in a profile that is `dbname`.
        assert "`dbname` is 'my_db' in your profile, but the cluster's is 'other_db'" in message
        assert (
            "`cloud_region` is 'us-east-1' in your profile, but the cluster's is 'us-west-2'"
            in (message)
        )
        assert "database" not in message.replace("database_kafka_cluster_id", "")

    @pytest.mark.parametrize(
        "error",
        [
            OperationalError("something else broke"),
            InterfaceError("Organization ID is required"),
        ],
    )
    def test_other_errors_propagate_unchanged(self, connect, error):
        connect.side_effect = error
        connection = Connection(
            type="confluent", name="test", credentials=make_credentials(**GLOBAL)
        )

        with pytest.raises(type(error)) as exc_info:
            ConfluentConnectionManager.open(connection)

        assert exc_info.value is error
        assert connection.state == ConnectionState.FAIL
