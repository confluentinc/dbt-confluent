"""`ConfluentCredentials` with the organization and cloud settings omitted, to be inferred by the
driver: what the profile accepts, rejects up front, shows in `dbt debug`, and hashes for
telemetry."""

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.confluent.connections import ConfluentCredentials

BASE = {"environment_id": "env-xyz", "dbname": "my_db"}
FLINK = {"flink_api_key": "flink-key", "flink_api_secret": "flink-secret"}
GLOBAL = {"global_api_key": "global-key", "global_api_secret": "global-secret"}
CLOUD = {"cloud_provider": "aws", "cloud_region": "us-west-1"}
ENDPOINT = {"endpoint": "https://flink.us-east-2.aws.private.confluent.cloud"}


def credentials(**profile) -> ConfluentCredentials:
    return ConfluentCredentials.from_dict({**BASE, **profile})


class TestOmissionsAccepted:
    @pytest.mark.parametrize("auth", [{"auth": "oauth"}, GLOBAL], ids=["oauth", "global_key"])
    def test_everything_inferable_may_be_omitted(self, auth):
        creds = credentials(**auth)

        assert creds.organization_id is None
        assert creds.cloud_provider is None
        assert creds.cloud_region is None
        assert creds.endpoint is None

    def test_a_global_key_beside_a_flink_key_still_infers(self):
        credentials(**GLOBAL, **FLINK)

    def test_a_profile_with_no_credentials_is_left_for_the_driver_to_reject(self):
        credentials()  # the driver says what's missing, not this check

    def test_a_flink_key_with_everything_supplied(self):
        credentials(**FLINK, **CLOUD, organization_id="org-1")

    def test_a_flink_key_with_an_endpoint_needs_no_cloud_settings(self):
        credentials(**FLINK, **ENDPOINT, organization_id="org-1")


class TestOmissionsRejectedUpFront:
    def test_flink_key_requires_organization_id(self):
        with pytest.raises(DbtRuntimeError, match="`organization_id` is required"):
            credentials(**FLINK, **CLOUD)

    def test_blank_organization_id_counts_as_omitted(self):
        with pytest.raises(DbtRuntimeError, match="`organization_id` is required"):
            credentials(**FLINK, **CLOUD, organization_id="")

    @pytest.mark.parametrize(
        "cloud",
        [{}, {"cloud_provider": "aws"}, {"cloud_region": "us-west-1"}],
        ids=["neither", "provider only", "region only"],
    )
    def test_flink_key_requires_both_cloud_settings_or_an_endpoint(self, cloud):
        with pytest.raises(DbtRuntimeError, match="`cloud_provider` and `cloud_region`"):
            credentials(**FLINK, organization_id="org-1", **cloud)

    @pytest.mark.parametrize(
        "scoped",
        [
            {"cloud_api_key": "k", "cloud_api_secret": "s"},
            {"tableflow_api_key": "k", "tableflow_api_secret": "s"},
        ],
        ids=["cloud key", "tableflow key"],
    )
    def test_scoped_keys_cannot_infer_either(self, scoped):
        with pytest.raises(DbtRuntimeError, match="`organization_id` is required"):
            credentials(**scoped, **CLOUD)

    def test_messages_point_at_the_ways_out(self):
        with pytest.raises(DbtRuntimeError) as exc_info:
            credentials(**FLINK)

        assert "auth: oauth" in str(exc_info.value)
        assert "global_api_key" in str(exc_info.value)


class TestUniqueField:
    def test_endpoint_wins(self):
        assert credentials(**GLOBAL, **ENDPOINT, **CLOUD).unique_field == ENDPOINT["endpoint"]

    def test_complete_profile(self):
        creds = credentials(**GLOBAL, **CLOUD, organization_id="org-1")

        assert creds.unique_field == "aws-us-west-1-org-1"

    def test_omitted_organization_falls_back_to_the_environment(self):
        assert credentials(**GLOBAL, **CLOUD).unique_field == "aws-us-west-1-env-xyz"

    def test_nothing_omitted_is_ever_rendered_as_none(self):
        assert credentials(auth="oauth").unique_field == "env-xyz"

    def test_different_environments_differ_when_everything_is_omitted(self):
        a = credentials(auth="oauth", environment_id="env-a")
        b = credentials(auth="oauth", environment_id="env-b")

        assert a.unique_field != b.unique_field


class TestConnectionKeys:
    def test_complete_profile_shows_everything(self):
        keys = credentials(**GLOBAL, **CLOUD, organization_id="org-1")._connection_keys()

        assert keys == (
            "organization_id",
            "database",
            "schema",
            "compute_pool_id",
            "name_prefix",
            "auth",
            "cloud_provider",
            "cloud_region",
        )

    def test_omitted_settings_are_not_shown(self):
        keys = credentials(auth="oauth")._connection_keys()

        assert keys == ("database", "schema", "compute_pool_id", "name_prefix", "auth")

    def test_endpoint_replaces_the_cloud_settings(self):
        keys = credentials(**GLOBAL, **ENDPOINT)._connection_keys()

        assert keys[-1] == "endpoint"
        assert "cloud_provider" not in keys
