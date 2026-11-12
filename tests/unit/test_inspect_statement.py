"""Unit tests for ConfluentAdapter.inspect_statement.

Returns the statement's needs_restart decision and the SQL Confluent stores for it, from one
lookup. decide_action (Jinja) uses both values: needs_restart to know whether to re-submit the
INSERT, and sql for the logic drift check.
"""

from unittest.mock import MagicMock, patch

import pytest
from confluent_sql.exceptions import OperationalError, StatementNotFoundError
from confluent_sql.statement import Phase

from dbt.adapters.confluent.impl import ConfluentAdapter

TERMINAL_PHASES = [Phase.COMPLETED, Phase.STOPPED, Phase.FAILED, Phase.DELETED]
HEALTHY_PHASES = [Phase.PENDING, Phase.RUNNING, Phase.DEGRADED, Phase.STOPPING, Phase.DELETING]


def _adapter() -> ConfluentAdapter:
    """A real adapter instance with a mocked connection handle, so the real
    _handle_pool_scoped_403 runs on the 403/non-403 paths."""
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter.connections = MagicMock()
    return adapter


def _handle(adapter: ConfluentAdapter) -> MagicMock:
    return adapter.connections.get_thread_handle.return_value


def inspect(adapter: ConfluentAdapter, name: str = "any-name") -> dict:
    return adapter.inspect_statement(name)


class TestInspectStatement:
    def test_missing_statement_needs_restart_without_sql(self):
        adapter = _adapter()
        _handle(adapter).get_statement.side_effect = StatementNotFoundError(
            statement_name="any-name", message="not found"
        )
        assert inspect(adapter) == {"needs_restart": True, "sql": None}

    def test_403_needs_restart_without_sql_and_warns(self):
        """Pool-scoped roles return 403 for a missing / cross-pool statement;
        treat it as missing (restart) and emit a warning."""
        adapter = _adapter()
        _handle(adapter).get_statement.side_effect = OperationalError(
            "error sending request '403'", http_status_code=403
        )
        with patch("dbt.adapters.confluent.impl.logger") as logger:
            assert inspect(adapter) == {"needs_restart": True, "sql": None}
        logger.warning.assert_called_once()
        assert "403" in logger.warning.call_args.args[0]

    def test_non_403_operational_error_reraises(self):
        """A non-403 API error is a real failure and must propagate."""
        adapter = _adapter()
        _handle(adapter).get_statement.side_effect = OperationalError(
            "internal server error", http_status_code=500
        )
        with pytest.raises(OperationalError):
            inspect(adapter)

    @pytest.mark.parametrize("phase", TERMINAL_PHASES)
    def test_terminal_statement_needs_restart_with_sql(self, phase: Phase):
        adapter = _adapter()
        statement = MagicMock()
        statement.phase = phase
        statement.spec = {"statement": "SELECT 1"}
        _handle(adapter).get_statement.return_value = statement
        assert inspect(adapter) == {"needs_restart": True, "sql": "SELECT 1"}

    @pytest.mark.parametrize("phase", HEALTHY_PHASES)
    def test_healthy_statement_returns_sql(self, phase: Phase):
        adapter = _adapter()
        statement = MagicMock()
        statement.phase = phase
        statement.spec = {"statement": "SELECT 1"}
        _handle(adapter).get_statement.return_value = statement
        assert inspect(adapter) == {"needs_restart": False, "sql": "SELECT 1"}

    def test_spec_without_statement_returns_none(self):
        """spec {} on RUNNING -> sql None, no exception."""
        adapter = _adapter()
        statement = MagicMock()
        statement.phase = Phase.RUNNING
        statement.spec = {}
        _handle(adapter).get_statement.return_value = statement
        assert inspect(adapter) == {"needs_restart": False, "sql": None}
