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
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter
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
    def logger(self, monkeypatch):
        mock = MagicMock()
        monkeypatch.setattr("dbt.adapters.confluent.impl.logger", mock)
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

    def test_not_enabled_enables_with_current_config(self, wire_connection, handle, rel, logger):
        wire_connection.ensure_tableflow_config(
            rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        handle.enable_tableflow.assert_called_once()
        call = handle.enable_tableflow.call_args
        assert call.args[0] == "my_table"
        assert call.kwargs["tableflow_formats"] == [TableFormat.ICEBERG]
        assert call.kwargs["storage"] == ManagedStorage()
        assert call.kwargs["config"] is None
        # This blocks for up to 300s by default (waiting for RUNNING), so it
        # must be logged at info, not debug, to be visible without --debug.
        logger.info.assert_called_once()
        assert "my_table" in logger.info.call_args.args[0]
        logger.debug.assert_not_called()

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

    def test_already_enabled_warns_instead_of_enabling(self, wire_connection, handle, rel, logger):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = MagicMock()
        wire_connection.ensure_tableflow_config(
            rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        handle.enable_tableflow.assert_not_called()
        logger.warning.assert_called_once()
        msg = logger.warning.call_args.args[0]
        assert "my_table" in msg
        assert "--full-refresh" in msg
        # dbt-core's default text logger doesn't color lines by level -- a
        # message must tag itself with warning_tag() to visually stand out.
        assert "WARNING" in msg

    def test_already_enabled_does_not_warn_when_config_unset(
        self, wire_connection, handle, rel, logger
    ):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = MagicMock()
        wire_connection.ensure_tableflow_config(rel, None)
        handle.get_tableflow.assert_not_called()
        logger.warning.assert_not_called()

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

    def test_already_exists_race_is_swallowed(self, wire_connection, handle, rel, logger):
        """Narrow race: something else enabled it between our GET and this
        call. The desired end state (enabled) already holds, so this must
        not raise."""
        handle.enable_tableflow.side_effect = TableflowTopicAlreadyExistsError(
            "already enabled", table_name="my_table"
        )
        wire_connection.ensure_tableflow_config(
            rel, {"formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        # One info log for the enable attempt, one debug log for the swallowed race.
        logger.info.assert_called_once()
        logger.debug.assert_called_once()
        assert "my_table" in logger.debug.call_args.args[0]

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


class TestEnsureTableflowConfigMalformedConfig:
    """`tableflow` isn't validated eagerly at compile time (unlike
    distributed_by/start_mode): it's only ever applied via an API call made
    after the table already exists, so a bad value can't doom a
    --full-refresh recreate. This is the only place it's validated -- these
    cases must all raise CompilationError before ever reaching the driver.
    """

    @pytest.fixture
    def adapter(self):
        return ConfluentAdapter.__new__(ConfluentAdapter)

    @pytest.fixture
    def handle(self):
        # Not enabled yet -- reach the translation/enable path, not the
        # already-enabled warn path.
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

    @pytest.mark.parametrize(
        "bad_config, expected_substring",
        [
            (["ICEBERG"], "must be a mapping"),
            ("ICEBERG", "must be a mapping"),
            ({"storage": {"kind": "Managed"}, "bogus": 1}, "unknown key(s): bogus"),
            ({"storage": {"kind": "Managed"}}, "'tableflow.formats' is required"),
            ({"formats": [], "storage": {"kind": "Managed"}}, "'tableflow.formats' is required"),
            (
                {"formats": ["PARQUET"], "storage": {"kind": "Managed"}},
                "'tableflow.formats' is invalid",
            ),
            ({"formats": "ICEBERG"}, "'tableflow.storage' is required"),
            ({"formats": "ICEBERG", "storage": "managed"}, "'tableflow.storage' is required"),
            (
                {"formats": "ICEBERG", "storage": {"kind": "s3"}},
                "'tableflow.storage.kind' must be one of",
            ),
            (
                {"formats": "ICEBERG", "storage": {"kind": "ByobAws"}},
                "'tableflow.storage' of kind 'ByobAws' is invalid",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {
                        "kind": "Managed",
                        "bucket_name": "extra",
                    },
                },
                "'tableflow.storage' of kind 'Managed' is invalid",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {
                        "kind": "ByobAws",
                        "bucket_name": 12345,
                        "provider_integration_id": "cspi-1",
                    },
                },
                "'tableflow.storage.bucket_name' must be a string",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {
                        "kind": "AzureDataLakeStorageGen2",
                        "storage_account_name": "acct",
                        "container_name": 999,
                        "provider_integration_id": "cspi-1",
                    },
                },
                "'tableflow.storage.container_name' must be a string",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "retention_ms": -1,
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "retention_ms": True,
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "retention_ms": "not-a-number",
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "retention_ms": [1, 2, 3],
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "error_handling": "SUSPEND",
                },
                "must be a mapping with a 'mode' key",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "error_handling": {"mode": "retry"},
                },
                "'tableflow.error_handling.mode' must be one of",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "error_handling": {"mode": "SUSPEND", "target": "x"},
                },
                "'tableflow.error_handling' of mode 'SUSPEND' is invalid",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "error_handling": {"mode": "LOG", "target": 123},
                },
                "'tableflow.error_handling.target' must be a string",
            ),
            (
                {"formats": "ICEBERG", "storage": {"kind": ["Managed"]}},
                "'tableflow.storage.kind' must be one of",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "error_handling": {"mode": ["LOG"]},
                },
                "'tableflow.error_handling.mode' must be one of",
            ),
        ],
        ids=[
            "list_not_dict",
            "string_not_dict",
            "unknown_top_level_key",
            "missing_formats",
            "empty_formats",
            "unknown_format",
            "missing_storage",
            "storage_not_dict",
            "unknown_storage_kind",
            "byob_aws_missing_required_keys",
            "managed_with_extra_key",
            "byob_aws_bucket_name_not_a_string",
            "azure_adls_container_name_not_a_string",
            "negative_retention_ms",
            "bool_retention_ms",
            "non_numeric_string_retention_ms",
            "list_retention_ms",
            "error_handling_not_dict",
            "unknown_error_handling_mode",
            "target_not_allowed_outside_log",
            "target_not_a_string",
            "unhashable_storage_kind",
            "unhashable_error_handling_mode",
        ],
    )
    def test_malformed_config_raises_before_touching_driver(
        self, wire_connection, handle, rel, bad_config, expected_substring
    ):
        with pytest.raises(CompilationError) as excinfo:
            wire_connection.ensure_tableflow_config(rel, bad_config)
        assert expected_substring in str(excinfo.value), (
            f"Expected error containing {expected_substring!r}, got: {excinfo.value}"
        )
        # Validated before any driver call, including the get_tableflow check --
        # an unrelated connection/auth error there must never mask a config error.
        handle.get_tableflow.assert_not_called()
        handle.enable_tableflow.assert_not_called()

    def test_malformed_config_raises_even_when_already_enabled(self, wire_connection, handle, rel):
        """A malformed config must surface its own error rather than being
        swallowed by the already-enabled warn-and-return path."""
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = MagicMock()
        with pytest.raises(CompilationError):
            wire_connection.ensure_tableflow_config(rel, {"formats": "PARQUET"})
        handle.get_tableflow.assert_not_called()

    def test_malformed_config_raises_even_when_get_tableflow_would_error(
        self, wire_connection, handle, rel
    ):
        """A malformed config must surface its own error rather than being
        masked by an unrelated connection/auth error from get_tableflow."""
        handle.get_tableflow.side_effect = OperationalError(
            "gateway timeout", http_status_code=504
        )
        with pytest.raises(CompilationError):
            wire_connection.ensure_tableflow_config(rel, {"formats": "PARQUET"})
        handle.get_tableflow.assert_not_called()
