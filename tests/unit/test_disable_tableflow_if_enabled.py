"""Unit tests for ConfluentAdapter.disable_tableflow_if_enabled.

Called before dropping a relation so the drop doesn't race an active
Tableflow materialization (confluent_sql's own recommendation). This method
itself checks only live state (GET, then DELETE), not dbt config -- so
within it, a table Tableflow was enabled on outside of dbt is covered the
same as one dbt enabled. Its caller (`disable_old_tableflow_before_drop` in
helpers.sql) is what decides whether to call it at all, gated on the
*current* model's `tableflow` config -- see that macro's docstring for why.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from confluent_sql.exceptions import (
    OperationalError,
    ProgrammingError,
    TableflowTopicNotFoundError,
)
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter
from tests.unit._helpers import relation as make_relation


class TestDisableTableflowIfEnabled:
    @pytest.fixture
    def adapter(self):
        return ConfluentAdapter.__new__(ConfluentAdapter)

    @pytest.fixture
    def handle(self):
        return MagicMock()

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

    def test_disables_when_enabled(self, wire_connection, handle, rel, logger):
        handle.get_tableflow.return_value = MagicMock()
        wire_connection.disable_tableflow_if_enabled(rel)
        handle.disable_tableflow.assert_called_once_with("my_table")
        logger.info.assert_called_once()
        assert "my_table" in logger.info.call_args.args[0]

    def test_no_op_when_not_enabled(self, wire_connection, handle, rel, logger):
        handle.get_tableflow.side_effect = TableflowTopicNotFoundError(
            "not enabled", table_name="my_table"
        )
        wire_connection.disable_tableflow_if_enabled(rel)
        handle.disable_tableflow.assert_not_called()
        logger.info.assert_not_called()

    def test_checks_before_disabling(self, wire_connection, handle, rel):
        """A GET-then-DELETE order, not a blind DELETE -- a blind DELETE would
        404 (raise TableflowTopicNotFoundError) for the common case where
        Tableflow was never enabled."""
        handle.get_tableflow.return_value = MagicMock()
        wire_connection.disable_tableflow_if_enabled(rel)
        assert handle.method_calls[0][0] == "get_tableflow"
        assert handle.method_calls[1][0] == "disable_tableflow"

    def test_get_error_is_wrapped_as_dbt_database_error(self, wire_connection, handle, rel):
        """Bypasses ConfluentConnectionManager.exception_handler (no SQL
        statement/cursor involved), so it must wrap confluent_sql errors
        itself instead of leaking a raw driver exception."""
        err = OperationalError("gateway timeout", http_status_code=504)
        handle.get_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            wire_connection.disable_tableflow_if_enabled(rel)
        assert exc_info.value.__cause__ is err
        handle.disable_tableflow.assert_not_called()

    def test_disable_error_is_wrapped_as_dbt_database_error(self, wire_connection, handle, rel):
        handle.get_tableflow.return_value = MagicMock()
        err = OperationalError("internal server error", http_status_code=500)
        handle.disable_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            wire_connection.disable_tableflow_if_enabled(rel)
        assert exc_info.value.__cause__ is err

    def test_disable_404_race_is_not_a_failure(self, wire_connection, handle, rel):
        """The GET and the DELETE are two separate calls -- if Tableflow gets
        disabled concurrently (another run, manual intervention) in between,
        the DELETE 404s too. That's the desired end state arriving a
        different way, not an error, so it must not raise."""
        handle.get_tableflow.return_value = MagicMock()
        handle.disable_tableflow.side_effect = TableflowTopicNotFoundError(
            "not enabled", table_name="my_table"
        )
        wire_connection.disable_tableflow_if_enabled(rel)

    # --- Tableflow control-plane auth errors -> actionable guidance ---

    def test_get_auth_error_names_profile_field(self, wire_connection, handle, rel):
        err = ProgrammingError(
            "Resolving the Kafka cluster id from the database name requires a global "
            "API key; alternatively pass database_kafka_cluster_id to connect()."
        )
        handle.get_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            wire_connection.disable_tableflow_if_enabled(rel)
        assert exc_info.value.__cause__ is err
        assert "global_api_key" in str(exc_info.value)
        handle.disable_tableflow.assert_not_called()

    def test_disable_auth_error_names_profile_field(self, wire_connection, handle, rel):
        handle.get_tableflow.return_value = MagicMock()
        err = ProgrammingError(
            "Resolving the Kafka cluster id from the database name requires a global "
            "API key; alternatively pass database_kafka_cluster_id to connect()."
        )
        handle.disable_tableflow.side_effect = err
        with pytest.raises(DbtDatabaseError) as exc_info:
            wire_connection.disable_tableflow_if_enabled(rel)
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
            wire_connection.disable_tableflow_if_enabled(rel)
        assert exc_info.value is err
        handle.disable_tableflow.assert_not_called()

    def test_disable_unrelated_programming_error_bubbles_up_unchanged(
        self, wire_connection, handle, rel
    ):
        handle.get_tableflow.return_value = MagicMock()
        err = ProgrammingError("SQL statement cannot be empty")
        handle.disable_tableflow.side_effect = err
        with pytest.raises(ProgrammingError) as exc_info:
            wire_connection.disable_tableflow_if_enabled(rel)
        assert exc_info.value is err
