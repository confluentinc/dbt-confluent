"""Unit tests for ConfluentAdapter.delete_statement.

The adapter calls confluent-sql's delete unconditionally and catches the
403 that compute-pool-scoped FlinkDeveloper roles return for a missing
statement (Confluent Cloud hides existence across pool boundaries — see
GH #58). The caller indicates whether 403 should be loud (expect_exists=True,
default) or quiet (expect_exists=False, used for orphan-cleanup paths).
"""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from confluent_sql.exceptions import OperationalError

from dbt.adapters.confluent.impl import ConfluentAdapter


class TestDeleteStatement:
    @pytest.fixture
    def adapter(self):
        return ConfluentAdapter.__new__(ConfluentAdapter)

    @pytest.fixture
    def handle(self):
        return MagicMock()

    @pytest.fixture
    def conn(self, handle):
        return SimpleNamespace(handle=handle)

    @pytest.fixture
    def wire_connection(self, adapter, conn):
        adapter.connections = MagicMock()
        adapter.connections.get_thread_connection.return_value = conn
        return adapter

    def test_successful_delete_calls_handle(self, wire_connection, handle):
        wire_connection.delete_statement("dbt-some-stmt")
        handle.delete_statement.assert_called_once_with("dbt-some-stmt")

    def test_403_with_expect_exists_emits_warning(self, wire_connection, handle):
        handle.delete_statement.side_effect = OperationalError(
            "error sending request '403'", http_status_code=403
        )
        with patch("dbt.adapters.confluent.impl.fire_event") as fire_event:
            wire_connection.delete_statement("dbt-some-stmt", expect_exists=True)

        fire_event.assert_called_once()
        event = fire_event.call_args.args[0]
        assert "403" in event.base_msg
        assert "dbt-some-stmt" in event.base_msg

    def test_403_with_expect_exists_default_emits_warning(self, wire_connection, handle):
        """Default is expect_exists=True, so a 403 still warns."""
        handle.delete_statement.side_effect = OperationalError(
            "error sending request '403'", http_status_code=403
        )
        with patch("dbt.adapters.confluent.impl.fire_event") as fire_event:
            wire_connection.delete_statement("dbt-some-stmt")

        fire_event.assert_called_once()

    def test_403_without_expect_exists_logs_debug_no_warning(
        self, wire_connection, handle, caplog
    ):
        handle.delete_statement.side_effect = OperationalError(
            "error sending request '403'", http_status_code=403
        )
        with (
            patch("dbt.adapters.confluent.impl.fire_event") as fire_event,
            caplog.at_level(logging.DEBUG, logger="dbt.adapters.confluent.impl"),
        ):
            wire_connection.delete_statement("dbt-some-stmt", expect_exists=False)

        fire_event.assert_not_called()
        assert any(
            "dbt-some-stmt" in rec.message and "opportunistic" in rec.message
            for rec in caplog.records
        )

    def test_non_403_operational_error_reraises(self, wire_connection, handle):
        err = OperationalError("internal server error", http_status_code=500)
        handle.delete_statement.side_effect = err
        with pytest.raises(OperationalError) as exc_info:
            wire_connection.delete_statement("dbt-some-stmt")
        assert exc_info.value is err

    def test_operational_error_without_status_reraises(self, wire_connection, handle):
        """An OperationalError with no http_status_code (e.g. raised by older
        confluent-sql code paths) is not a 403 and must propagate."""
        handle.delete_statement.side_effect = OperationalError("some bare error")
        with pytest.raises(OperationalError):
            wire_connection.delete_statement("dbt-some-stmt")
