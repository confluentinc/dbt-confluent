"""Which credentials can call the artifact and Tableflow control-plane routes, and the
up-front errors the adapter raises when they can't."""

from types import SimpleNamespace

import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.confluent.connections import ConfluentCredentials
from dbt.adapters.confluent.impl import ConfluentAdapter

FLINK = {"flink_api_key": "flink-key", "flink_api_secret": "flink-secret"}
GLOBAL = {"global_api_key": "global-key", "global_api_secret": "global-secret"}
CLOUD = {"cloud_api_key": "cloud-key", "cloud_api_secret": "cloud-secret"}
TABLEFLOW = {"tableflow_api_key": "tf-key", "tableflow_api_secret": "tf-secret"}
CLUSTER_ID = {"database_kafka_cluster_id": "lkc-abc123"}


def credentials(**profile) -> ConfluentCredentials:
    return ConfluentCredentials.from_dict(
        {
            "organization_id": "org-123",
            "environment_id": "env-xyz",
            "dbname": "my_db",
            "cloud_provider": "aws",
            "cloud_region": "us-west-1",
            **profile,
        }
    )


def adapter_for(profile: dict) -> ConfluentAdapter:
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter.config = SimpleNamespace(credentials=credentials(**profile))  # type: ignore[assignment]
    return adapter


@pytest.mark.parametrize(
    ("profile", "can_use"),
    [
        pytest.param(FLINK, False, id="flink key only"),
        pytest.param({**FLINK, **TABLEFLOW, **CLUSTER_ID}, False, id="tableflow key only"),
        pytest.param({**FLINK, **CLOUD}, True, id="cloud key"),
        pytest.param(GLOBAL, True, id="global key"),
        pytest.param({"auth": "oauth"}, True, id="oauth"),
    ],
)
def test_can_use_artifact_api(profile, can_use):
    assert credentials(**profile).can_use_artifact_api is can_use


@pytest.mark.parametrize(
    ("profile", "can_use"),
    [
        pytest.param(FLINK, False, id="flink key only"),
        pytest.param({**FLINK, **CLOUD, **CLUSTER_ID}, False, id="cloud key only"),
        pytest.param({**FLINK, **TABLEFLOW}, False, id="tableflow key without cluster id"),
        pytest.param(
            {**FLINK, **TABLEFLOW, **CLUSTER_ID}, True, id="tableflow key and cluster id"
        ),
        pytest.param(GLOBAL, True, id="global key"),
        pytest.param({"auth": "oauth"}, True, id="oauth"),
    ],
)
def test_can_use_tableflow(profile, can_use):
    assert credentials(**profile).can_use_tableflow is can_use


class TestRequireCredentials:
    def test_artifact_credentials_missing(self):
        with pytest.raises(CompilationError, match="cloud_api_key"):
            adapter_for(FLINK).require_artifact_credentials()

    def test_artifact_credentials_present(self):
        adapter_for({**FLINK, **CLOUD}).require_artifact_credentials()

    def test_tableflow_credentials_missing_names_every_fix(self):
        with pytest.raises(CompilationError) as exc_info:
            adapter_for({**FLINK, **TABLEFLOW}).require_tableflow_credentials()
        message = str(exc_info.value)
        assert "auth: oauth" in message
        assert "global_api_key" in message
        assert "tableflow_api_key" in message
        assert "database_kafka_cluster_id" in message

    def test_tableflow_credentials_present(self):
        adapter_for({**FLINK, **TABLEFLOW, **CLUSTER_ID}).require_tableflow_credentials()
