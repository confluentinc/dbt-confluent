"""Unit tests for ConfluentAdapter.ensure_tableflow_config.

Called on every run a model configures `tableflow`, regardless of whether
the relation was just created, already existed, or is being restarted --
there's a single rule: not enabled -> enable with the current config;
already enabled -> warn (v1 does no diffing, see #101).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from confluent_sql import AzureAdlsStorage, ByobAwsStorage, ManagedStorage, TableFormat
from confluent_sql.exceptions import (
    OperationalError,
    ProgrammingError,
    TableflowTopicAlreadyExistsError,
    TableflowTopicNotFoundError,
)
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter
from dbt.adapters.events.types import AdapterEventInfo
from tests.unit._helpers import relation as make_relation


class TestEnsureTableflowConfig:
    @pytest.fixture
    def adapter(self):
        return ConfluentAdapter.__new__(ConfluentAdapter)

    @pytest.fixture
    def handle(self):
        # Default: not enabled yet -- the common case, and the one where
        # this method is actually expected to call enable_tableflow.
        handle = MagicMock()
        handle.get_tableflow.side_effect = TableflowTopicNotFoundError(
            "not enabled", table_name="my_table"
        )
        return handle

    @pytest.fixture
    def wire_connection(self, adapter, handle):
        adapter.connections = MagicMock()
        adapter.connections.get_thread_connection.return_value = SimpleNamespace(handle=handle)
        return adapter

    @pytest.fixture
    def rel(self):
        return make_relation("my_table")

    @pytest.fixture
    def fire_event(self, monkeypatch):
        mock = MagicMock()
        monkeypatch.setattr("dbt.adapters.confluent.impl.fire_event", mock)
        return mock

    # --- config-unset no-op ---

    def test_none_config_is_a_no_op(self, wire_connection, handle):
        wire_connection.ensure_tableflow_config(make_relation("t"), None)
        handle.get_tableflow.assert_not_called()
        handle.enable_tableflow.assert_not_called()

    def test_empty_config_is_a_no_op(self, wire_connection, handle):
        wire_connection.ensure_tableflow_config(make_relation("t"), {})
        handle.get_tableflow.assert_not_called()
        handle.enable_tableflow.assert_not_called()

    # --- not enabled -> enable ---

    def test_not_enabled_enables_with_current_config(
        self, wire_connection, handle, rel, fire_event
    ):
        wire_connection.ensure_tableflow_config(
            rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        handle.enable_tableflow.assert_called_once()
        call = handle.enable_tableflow.call_args
        assert call.args[0] == "my_table"
        assert call.kwargs["tableflow_formats"] == [TableFormat.ICEBERG]
        assert call.kwargs["storage"] == ManagedStorage()
        assert call.kwargs["config"] is None
        fire_event.assert_called_once()
        event = fire_event.call_args.args[0]
        assert "my_table" in event.base_msg
        # This blocks for up to 300s by default (waiting for RUNNING), so it
        # must be visible without --debug.
        assert isinstance(event, AdapterEventInfo)

    def test_lowercase_and_multiple_formats(self, wire_connection, handle, rel):
        wire_connection.ensure_tableflow_config(
            rel, {"formats": ["iceberg", "delta"], "storage": {"kind": "Managed"}}
        )
        call = handle.enable_tableflow.call_args
        assert call.kwargs["tableflow_formats"] == [TableFormat.ICEBERG, TableFormat.DELTA]

    def test_byob_aws_storage(self, wire_connection, handle, rel):
        wire_connection.ensure_tableflow_config(
            rel,
            {
                "formats": "ICEBERG",
                "storage": {
                    "kind": "ByobAws",
                    "bucket_name": "my-bucket",
                    "provider_integration_id": "cspi-123",
                },
            },
        )
        call = handle.enable_tableflow.call_args
        assert call.kwargs["storage"] == ByobAwsStorage(
            bucket_name="my-bucket", provider_integration_id="cspi-123"
        )

    def test_azure_adls_storage(self, wire_connection, handle, rel):
        wire_connection.ensure_tableflow_config(
            rel,
            {
                "formats": "ICEBERG",
                "storage": {
                    "kind": "AzureDataLakeStorageGen2",
                    "storage_account_name": "acct",
                    "container_name": "container",
                    "provider_integration_id": "cspi-123",
                },
            },
        )
        call = handle.enable_tableflow.call_args
        assert call.kwargs["storage"] == AzureAdlsStorage(
            storage_account_name="acct",
            container_name="container",
            provider_integration_id="cspi-123",
        )

    def test_retention_and_error_handling_build_topic_config(self, wire_connection, handle, rel):
        wire_connection.ensure_tableflow_config(
            rel,
            {
                "formats": "ICEBERG",
                "storage": {"kind": "Managed"},
                "retention_ms": 604800000,
                "data_retention_ms": 0,
                "error_handling": {"mode": "LOG", "target": "my_dlq"},
            },
        )
        call = handle.enable_tableflow.call_args
        topic_config = call.kwargs["config"]
        assert topic_config.retention_ms == 604800000
        assert topic_config.data_retention_ms == 0
        assert topic_config.error_handling.mode == "LOG"
        assert topic_config.error_handling.target == "my_dlq"

    def test_no_optional_fields_passes_no_topic_config(self, wire_connection, handle, rel):
        wire_connection.ensure_tableflow_config(
            rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        assert handle.enable_tableflow.call_args.kwargs["config"] is None

    # --- already enabled -> warn, don't touch it ---

    def test_already_enabled_warns_instead_of_enabling(
        self, wire_connection, handle, rel, fire_event
    ):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = MagicMock()
        wire_connection.ensure_tableflow_config(
            rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        handle.enable_tableflow.assert_not_called()
        fire_event.assert_called_once()
        event = fire_event.call_args.args[0]
        assert "my_table" in event.base_msg
        assert "--full-refresh" in event.base_msg
        # dbt-core's default text logger doesn't color lines by level -- a
        # message must tag itself with warning_tag() to visually stand out.
        assert "WARNING" in event.base_msg

    def test_already_enabled_does_not_warn_when_config_unset(
        self, wire_connection, handle, rel, fire_event
    ):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = MagicMock()
        wire_connection.ensure_tableflow_config(rel, None)
        handle.get_tableflow.assert_not_called()
        fire_event.assert_not_called()

    # --- error wrapping ---

    def test_get_error_is_wrapped_as_dbt_database_error(self, wire_connection, handle, rel):
        err = OperationalError("gateway timeout", http_status_code=504)
        handle.get_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            wire_connection.ensure_tableflow_config(
                rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value.__cause__ is err
        handle.enable_tableflow.assert_not_called()

    def test_enable_error_is_wrapped_as_dbt_database_error(self, wire_connection, handle, rel):
        err = OperationalError("topic did not reach RUNNING within 300 seconds")
        handle.enable_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            wire_connection.ensure_tableflow_config(
                rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value.__cause__ is err
        assert "my_table" in str(exc_info.value)

    def test_already_exists_race_is_swallowed(self, wire_connection, handle, rel, fire_event):
        """Narrow race: something else enabled it between our GET and this
        call. The desired end state (enabled) already holds, so this must
        not raise."""
        handle.enable_tableflow.side_effect = TableflowTopicAlreadyExistsError(
            "already enabled", table_name="my_table"
        )
        wire_connection.ensure_tableflow_config(
            rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        # One debug event for the enable attempt, one for the swallowed race.
        assert fire_event.call_count == 2
        assert "my_table" in fire_event.call_args.args[0].base_msg

    # --- Tableflow control-plane auth errors -> actionable guidance ---

    def test_get_auth_error_names_profile_field(self, wire_connection, handle, rel):
        """A Flink-region-only profile can't resolve the Kafka cluster id
        Tableflow needs. The raw driver message points at `connect()`'s
        `database_kafka_cluster_id`, a parameter this adapter doesn't expose
        -- the wrapped error must instead name the actual profile field."""
        err = ProgrammingError(
            "Resolving the Kafka cluster id from the database name requires a global "
            "API key; alternatively pass database_kafka_cluster_id to connect()."
        )
        handle.get_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            wire_connection.ensure_tableflow_config(
                rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value.__cause__ is err
        assert "global_api_key" in str(exc_info.value)
        handle.enable_tableflow.assert_not_called()

    def test_enable_auth_error_names_profile_field(self, wire_connection, handle, rel):
        err = ProgrammingError(
            "Resolving the Kafka cluster id from the database name requires a global "
            "API key; alternatively pass database_kafka_cluster_id to connect()."
        )
        handle.enable_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            wire_connection.ensure_tableflow_config(
                rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value.__cause__ is err
        assert "global_api_key" in str(exc_info.value)

    def test_get_unrelated_programming_error_bubbles_up_unchanged(
        self, wire_connection, handle, rel
    ):
        """Not every ProgrammingError is the cluster-id auth case -- an
        unrecognized one must not be mislabeled with auth guidance."""
        err = ProgrammingError("SQL statement cannot be empty")
        handle.get_tableflow.side_effect = err
        with pytest.raises(ProgrammingError) as exc_info:
            wire_connection.ensure_tableflow_config(
                rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value is err
        handle.enable_tableflow.assert_not_called()

    def test_enable_unrelated_programming_error_bubbles_up_unchanged(
        self, wire_connection, handle, rel
    ):
        err = ProgrammingError("SQL statement cannot be empty")
        handle.enable_tableflow.side_effect = err
        with pytest.raises(ProgrammingError) as exc_info:
            wire_connection.ensure_tableflow_config(
                rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value is err
