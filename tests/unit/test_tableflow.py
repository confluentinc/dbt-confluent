"""Unit tests for tableflow.reconcile_tableflow_config.

Called on every run a model configures `tableflow`, regardless of whether
the relation was just created, already existed, or is being restarted --
there's a single rule: not enabled -> enable with the current config;
already enabled -> diff the live config against the desired one and PATCH
only if something actually changed (see #101).
"""

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

from dbt.adapters.confluent import tableflow
from tests.unit._helpers import make_topic
from tests.unit._helpers import relation as make_relation


class TestEnsureTableflowConfig:
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
    def rel(self):
        return make_relation("my_table")

    @pytest.fixture
    def logger(self, monkeypatch):
        mock = MagicMock()
        monkeypatch.setattr("dbt.adapters.confluent.tableflow.logger", mock)
        return mock

    # --- config-unset no-op ---

    def test_none_config_is_a_no_op(self, handle):
        tableflow.reconcile_tableflow_config(handle, make_relation("t"), None)
        handle.get_tableflow.assert_not_called()
        handle.enable_tableflow.assert_not_called()

    def test_empty_config_is_a_no_op(self, handle):
        tableflow.reconcile_tableflow_config(handle, make_relation("t"), {})
        handle.get_tableflow.assert_not_called()
        handle.enable_tableflow.assert_not_called()

    # --- not enabled -> enable ---

    def test_not_enabled_enables_with_current_config(self, handle, rel, logger):
        tableflow.reconcile_tableflow_config(
            handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
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

    def test_multiple_formats(self, handle, rel):
        tableflow.reconcile_tableflow_config(
            handle, rel, {"table_formats": ["ICEBERG", "DELTA"], "storage": {"kind": "Managed"}}
        )
        call = handle.enable_tableflow.call_args
        assert call.kwargs["tableflow_formats"] == [TableFormat.ICEBERG, TableFormat.DELTA]

    def test_byob_aws_storage(self, handle, rel):
        tableflow.reconcile_tableflow_config(
            handle,
            rel,
            {
                "table_formats": "ICEBERG",
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

    def test_azure_adls_storage(self, handle, rel):
        tableflow.reconcile_tableflow_config(
            handle,
            rel,
            {
                "table_formats": "ICEBERG",
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

    def test_retention_and_error_handling_build_topic_config(self, handle, rel):
        tableflow.reconcile_tableflow_config(
            handle,
            rel,
            {
                "table_formats": "ICEBERG",
                "storage": {"kind": "Managed"},
                "config": {
                    "retention_ms": 604800000,
                    "data_retention_ms": 0,
                    "error_handling": {"mode": "LOG", "target": "my_dlq"},
                },
            },
        )
        call = handle.enable_tableflow.call_args
        topic_config = call.kwargs["config"]
        assert topic_config.retention_ms == 604800000
        assert topic_config.data_retention_ms == 0
        assert topic_config.error_handling.mode == "LOG"
        assert topic_config.error_handling.target == "my_dlq"

    def test_no_optional_fields_passes_no_topic_config(self, handle, rel):
        tableflow.reconcile_tableflow_config(
            handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        assert handle.enable_tableflow.call_args.kwargs["config"] is None

    # --- already enabled -> diff, PATCH only if something changed ---

    def test_already_enabled_matching_config_is_a_noop(self, handle, rel, logger):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(table_formats=("ICEBERG",))
        tableflow.reconcile_tableflow_config(
            handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        handle.enable_tableflow.assert_not_called()
        handle.update_tableflow.assert_not_called()
        logger.info.assert_not_called()

    # --- already enabled, storage changed -> no in-place PATCH path, so recreate (#101) ---

    def test_already_enabled_storage_changed_recreates_topic(self, handle, rel, logger):
        """`storage` is immutable via PATCH, but not actually unchangeable: disabling and
        re-enabling only ever touches the Tableflow sink, never the underlying Kafka topic
        or its data, and re-enabling backfills the full topic history -- so this is the
        right response to a storage change, not a `--full-refresh`."""
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(table_formats=("ICEBERG",))
        tableflow.reconcile_tableflow_config(
            handle,
            rel,
            {
                "table_formats": "ICEBERG",
                "storage": {
                    "kind": "ByobAws",
                    "bucket_name": "my-bucket",
                    "provider_integration_id": "cspi-123",
                },
            },
        )
        handle.disable_tableflow.assert_called_once_with("my_table")
        handle.update_tableflow.assert_not_called()
        handle.enable_tableflow.assert_called_once()
        call = handle.enable_tableflow.call_args
        assert call.args[0] == "my_table"
        assert call.kwargs["storage"] == ByobAwsStorage(
            bucket_name="my-bucket", provider_integration_id="cspi-123"
        )
        # table_formats/config are re-applied fresh from the current dbt config, same as
        # any other create -- not just whatever happened to be already running.
        assert call.kwargs["tableflow_formats"] == [TableFormat.ICEBERG]
        # One info log announcing the storage-change recreate, one for the enable itself.
        assert logger.info.call_count == 2
        assert "my_table" in logger.info.call_args.args[0]

    def test_already_enabled_storage_change_disable_race_is_tolerated(self, handle, rel):
        """Narrow race: something else already disabled Tableflow between our GET and this
        DELETE. Must not raise -- proceed straight to enabling with the desired config,
        same as if it had never been enabled."""
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(table_formats=("ICEBERG",))
        handle.disable_tableflow.side_effect = TableflowTopicNotFoundError(
            "not enabled", table_name="my_table"
        )
        tableflow.reconcile_tableflow_config(
            handle,
            rel,
            {
                "table_formats": "ICEBERG",
                "storage": {"kind": "ByobAws", "bucket_name": "b", "provider_integration_id": "i"},
            },
        )
        handle.enable_tableflow.assert_called_once()

    def test_already_enabled_matching_config_ignores_int_vs_string_type(self, handle, rel):
        """The API round-trips retention_ms as a string even though dbt
        config supplies an int -- that alone must not look like drift."""
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(config={"retention_ms": "604800000"})
        tableflow.reconcile_tableflow_config(
            handle,
            rel,
            {
                "table_formats": "ICEBERG",
                "storage": {"kind": "Managed"},
                "config": {"retention_ms": 604800000},
            },
        )
        handle.update_tableflow.assert_not_called()

    def test_already_enabled_changed_retention_sends_update(self, handle, rel):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(config={"retention_ms": "1209600000"})
        tableflow.reconcile_tableflow_config(
            handle,
            rel,
            {
                "table_formats": "ICEBERG",
                "storage": {"kind": "Managed"},
                "config": {"retention_ms": 604800000},
            },
        )
        handle.enable_tableflow.assert_not_called()
        handle.update_tableflow.assert_called_once()
        call = handle.update_tableflow.call_args
        assert call.args[0] == "my_table"
        assert call.kwargs["table_formats"] is None  # unchanged formats left alone
        # In wire form (a str), like everything else desired is built from -- not the
        # int dbt config happened to supply; nothing about what's sent changes either way.
        assert call.kwargs["config"].to_spec() == {"retention_ms": "604800000"}

    def test_already_enabled_changed_formats_sends_update(self, handle, rel):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(table_formats=("ICEBERG",))
        tableflow.reconcile_tableflow_config(
            handle, rel, {"table_formats": ["ICEBERG", "DELTA"], "storage": {"kind": "Managed"}}
        )
        call = handle.update_tableflow.call_args
        assert call.kwargs["table_formats"] == [TableFormat.ICEBERG, TableFormat.DELTA]
        assert call.kwargs["config"] is None  # unchanged config left alone

    def test_already_enabled_config_removed_leaves_existing_values(self, handle, rel):
        """`retention_ms`/`data_retention_ms`/`error_handling` are optional on the request
        but not on the resource -- the server rejects clearing them. So a `tableflow` block
        with no `config` sub-object leaves whatever's already set server-side alone, rather
        than attempting (and having rejected) an explicit delete."""
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(
            config={
                "retention_ms": "604800000",
                "data_retention_ms": "2592000000",
                "error_handling": {"mode": "LOG", "target": "my_dlq"},
            }
        )
        tableflow.reconcile_tableflow_config(
            handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        handle.update_tableflow.assert_not_called()

    def test_already_enabled_changed_error_handling_target_includes_mode(self, handle, rel):
        """The discriminated-union bug this diffing exists to avoid (#101): `error_handling`
        is a `mode` + mode-specific-fields union, and the server needs `mode` in the patch
        even when only `target` changed -- a naive per-field diff would omit it since `mode`
        itself didn't change."""
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(
            config={"error_handling": {"mode": "LOG", "target": "old_dlq"}}
        )
        tableflow.reconcile_tableflow_config(
            handle,
            rel,
            {
                "table_formats": "ICEBERG",
                "storage": {"kind": "Managed"},
                "config": {"error_handling": {"mode": "LOG", "target": "new_dlq"}},
            },
        )
        config = handle.update_tableflow.call_args.kwargs["config"]
        assert config.to_spec() == {"error_handling": {"mode": "LOG", "target": "new_dlq"}}

    def test_already_enabled_changed_error_handling_mode_sends_whole_object(self, handle, rel):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(
            config={"error_handling": {"mode": "SUSPEND"}}
        )
        tableflow.reconcile_tableflow_config(
            handle,
            rel,
            {
                "table_formats": "ICEBERG",
                "storage": {"kind": "Managed"},
                "config": {"error_handling": {"mode": "LOG", "target": "my_dlq"}},
            },
        )
        config = handle.update_tableflow.call_args.kwargs["config"]
        assert config.to_spec() == {"error_handling": {"mode": "LOG", "target": "my_dlq"}}

    def test_already_enabled_matching_error_handling_is_a_noop(self, handle, rel):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(
            config={"error_handling": {"mode": "LOG", "target": "my_dlq"}}
        )
        tableflow.reconcile_tableflow_config(
            handle,
            rel,
            {
                "table_formats": "ICEBERG",
                "storage": {"kind": "Managed"},
                "config": {"error_handling": {"mode": "LOG", "target": "my_dlq"}},
            },
        )
        handle.update_tableflow.assert_not_called()

    def test_already_enabled_update_blocks_by_default_logged_at_info(self, handle, rel, logger):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(table_formats=("ICEBERG",))
        tableflow.reconcile_tableflow_config(
            handle, rel, {"table_formats": ["ICEBERG", "DELTA"], "storage": {"kind": "Managed"}}
        )
        logger.info.assert_called_once()
        assert "my_table" in logger.info.call_args.args[0]

    def test_already_enabled_update_race_is_swallowed(self, handle, rel):
        """Narrow race: disabled concurrently between our GET and this PATCH.
        Must not raise -- the next run's GET will see it's disabled and
        enable fresh with the full desired config."""
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(table_formats=("ICEBERG",))
        handle.update_tableflow.side_effect = TableflowTopicNotFoundError(
            "not enabled", table_name="my_table"
        )
        tableflow.reconcile_tableflow_config(
            handle, rel, {"table_formats": ["ICEBERG", "DELTA"], "storage": {"kind": "Managed"}}
        )

    def test_already_enabled_does_not_call_update_when_config_unset(self, handle, rel, logger):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic()
        tableflow.reconcile_tableflow_config(handle, rel, None)
        handle.get_tableflow.assert_not_called()
        handle.update_tableflow.assert_not_called()

    def test_update_error_is_wrapped_as_dbt_database_error(self, handle, rel):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(table_formats=("ICEBERG",))
        err = OperationalError("internal server error", http_status_code=500)
        handle.update_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            tableflow.reconcile_tableflow_config(
                handle,
                rel,
                {"table_formats": ["ICEBERG", "DELTA"], "storage": {"kind": "Managed"}},
            )
        assert exc_info.value.__cause__ is err
        assert "my_table" in str(exc_info.value)

    def test_update_auth_error_names_profile_field(self, handle, rel):
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = make_topic(table_formats=("ICEBERG",))
        err = ProgrammingError(
            "Resolving the Kafka cluster id from the database name requires a global "
            "API key; alternatively pass database_kafka_cluster_id to connect()."
        )
        handle.update_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            tableflow.reconcile_tableflow_config(
                handle,
                rel,
                {"table_formats": ["ICEBERG", "DELTA"], "storage": {"kind": "Managed"}},
            )
        assert exc_info.value.__cause__ is err
        assert "global_api_key" in str(exc_info.value)

    # --- error wrapping ---

    def test_get_error_is_wrapped_as_dbt_database_error(self, handle, rel):
        err = OperationalError("gateway timeout", http_status_code=504)
        handle.get_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            tableflow.reconcile_tableflow_config(
                handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value.__cause__ is err
        handle.enable_tableflow.assert_not_called()

    def test_enable_error_is_wrapped_as_dbt_database_error(self, handle, rel):
        err = OperationalError("topic did not reach RUNNING within 300 seconds")
        handle.enable_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            tableflow.reconcile_tableflow_config(
                handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value.__cause__ is err
        assert "my_table" in str(exc_info.value)

    def test_already_exists_race_is_swallowed(self, handle, rel, logger):
        """Narrow race: something else enabled it between our GET and this
        call. The desired end state (enabled) already holds, so this must
        not raise."""
        handle.enable_tableflow.side_effect = TableflowTopicAlreadyExistsError(
            "already enabled", table_name="my_table"
        )
        tableflow.reconcile_tableflow_config(
            handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
        )
        logger.info.assert_called_once()

    # --- Tableflow control-plane auth errors -> actionable guidance ---

    def test_get_auth_error_names_profile_field(self, handle, rel):
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
            tableflow.reconcile_tableflow_config(
                handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value.__cause__ is err
        assert "global_api_key" in str(exc_info.value)
        handle.enable_tableflow.assert_not_called()

    def test_enable_auth_error_names_profile_field(self, handle, rel):
        err = ProgrammingError(
            "Resolving the Kafka cluster id from the database name requires a global "
            "API key; alternatively pass database_kafka_cluster_id to connect()."
        )
        handle.enable_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            tableflow.reconcile_tableflow_config(
                handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value.__cause__ is err
        assert "global_api_key" in str(exc_info.value)

    def test_get_unrelated_programming_error_bubbles_up_unchanged(self, handle, rel):
        """Not every ProgrammingError is the cluster-id auth case -- an
        unrecognized one must not be mislabeled with auth guidance."""
        err = ProgrammingError("SQL statement cannot be empty")
        handle.get_tableflow.side_effect = err
        with pytest.raises(ProgrammingError) as exc_info:
            tableflow.reconcile_tableflow_config(
                handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
            )
        assert exc_info.value is err
        handle.enable_tableflow.assert_not_called()

    def test_enable_unrelated_programming_error_bubbles_up_unchanged(self, handle, rel):
        err = ProgrammingError("SQL statement cannot be empty")
        handle.enable_tableflow.side_effect = err
        with pytest.raises(ProgrammingError) as exc_info:
            tableflow.reconcile_tableflow_config(
                handle, rel, {"table_formats": "ICEBERG", "storage": {"kind": "Managed"}}
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
    def handle(self):
        # Not enabled yet -- reach the translation/enable path, not the
        # already-enabled warn path.
        handle = MagicMock()
        handle.get_tableflow.side_effect = TableflowTopicNotFoundError(
            "not enabled", table_name="my_table"
        )
        return handle

    @pytest.fixture
    def rel(self):
        return make_relation("my_table")

    @pytest.mark.parametrize(
        "bad_config, expected_substring",
        [
            (["ICEBERG"], "must be a mapping"),
            ("ICEBERG", "must be a mapping"),
            ({"storage": {"kind": "Managed"}, "bogus": 1}, "unknown key(s): bogus"),
            ({"storage": {"kind": "Managed"}}, "'tableflow.table_formats' is required"),
            (
                {"table_formats": [], "storage": {"kind": "Managed"}},
                "'tableflow.table_formats' is required",
            ),
            (
                {"table_formats": ["PARQUET"], "storage": {"kind": "Managed"}},
                "'tableflow.table_formats' is invalid",
            ),
            (
                {"table_formats": "iceberg", "storage": {"kind": "Managed"}},
                "'tableflow.table_formats' is invalid",
            ),
            ({"table_formats": "ICEBERG"}, "'tableflow.storage' is required"),
            (
                {"table_formats": "ICEBERG", "storage": "managed"},
                "'tableflow.storage' is required",
            ),
            (
                {"table_formats": "ICEBERG", "storage": {"kind": "s3"}},
                "'tableflow.storage.kind' must be one of",
            ),
            (
                {"table_formats": "ICEBERG", "storage": {"kind": "ByobAws"}},
                "'tableflow.storage' of kind 'ByobAws' is invalid",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {
                        "kind": "Managed",
                        "bucket_name": "extra",
                    },
                },
                "'tableflow.storage' of kind 'Managed' is invalid",
            ),
            (
                {
                    "table_formats": "ICEBERG",
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
                    "table_formats": "ICEBERG",
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
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": {"retention_ms": -1},
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": {"retention_ms": True},
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": {"retention_ms": "not-a-number"},
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": {"retention_ms": [1, 2, 3]},
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": {"error_handling": "SUSPEND"},
                },
                "must be a mapping with a 'mode' key",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": {"error_handling": {"mode": "retry"}},
                },
                "'tableflow.config.error_handling.mode' must be one of",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": {"error_handling": {"mode": "SUSPEND", "target": "x"}},
                },
                "'tableflow.config.error_handling' of mode 'SUSPEND' is invalid",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": {"error_handling": {"mode": "LOG", "target": 123}},
                },
                "'tableflow.config.error_handling.target' must be a string",
            ),
            (
                {"table_formats": "ICEBERG", "storage": {"kind": ["Managed"]}},
                "'tableflow.storage.kind' must be one of",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": {"error_handling": {"mode": ["LOG"]}},
                },
                "'tableflow.config.error_handling.mode' must be one of",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": ["not", "dict"],
                },
                "'tableflow.config' must be a mapping",
            ),
            (
                {
                    "table_formats": "ICEBERG",
                    "storage": {"kind": "Managed"},
                    "config": {"retention_ms": 1, "bogus": 1},
                },
                "'tableflow.config' has unknown key(s): bogus",
            ),
        ],
        ids=[
            "list_not_dict",
            "string_not_dict",
            "unknown_top_level_key",
            "missing_formats",
            "empty_formats",
            "unknown_format",
            "lowercase_format_no_longer_accepted",
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
            "config_not_dict",
            "unknown_config_key",
        ],
    )
    def test_malformed_config_raises_before_touching_driver(
        self, handle, rel, bad_config, expected_substring
    ):
        with pytest.raises(CompilationError) as excinfo:
            tableflow.reconcile_tableflow_config(handle, rel, bad_config)
        assert expected_substring in str(excinfo.value), (
            f"Expected error containing {expected_substring!r}, got: {excinfo.value}"
        )
        # Validated before any driver call, including the get_tableflow check --
        # an unrelated connection/auth error there must never mask a config error.
        handle.get_tableflow.assert_not_called()
        handle.enable_tableflow.assert_not_called()

    def test_malformed_config_raises_even_when_already_enabled(self, handle, rel):
        """A malformed config must surface its own error rather than being
        swallowed by the already-enabled warn-and-return path."""
        handle.get_tableflow.side_effect = None
        handle.get_tableflow.return_value = MagicMock()
        with pytest.raises(CompilationError):
            tableflow.reconcile_tableflow_config(handle, rel, {"table_formats": "PARQUET"})
        handle.get_tableflow.assert_not_called()

    def test_malformed_config_raises_even_when_get_tableflow_would_error(self, handle, rel):
        """A malformed config must surface its own error rather than being
        masked by an unrelated connection/auth error from get_tableflow."""
        handle.get_tableflow.side_effect = OperationalError(
            "gateway timeout", http_status_code=504
        )
        with pytest.raises(CompilationError):
            tableflow.reconcile_tableflow_config(handle, rel, {"table_formats": "PARQUET"})
        handle.get_tableflow.assert_not_called()
