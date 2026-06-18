"""Unit tests for ConfluentAdapter.statement_needs_restart.

This is the decision input for the streaming-statement recovery path in
`decide_action` (Jinja): True means re-submit the long-running INSERT.
Terminal phases and a missing statement (404, or 403 under pool-scoped
roles, which return 403 instead of 404 for a missing / cross-pool
statement) return True; healthy phases — including in-flight transitions —
return False.
"""

from unittest.mock import MagicMock, patch

import pytest
from confluent_sql.exceptions import OperationalError, StatementNotFoundError
from confluent_sql.statement import Phase

from dbt.adapters.confluent.impl import ConfluentAdapter


def _adapter() -> ConfluentAdapter:
    """A real adapter instance with a mocked connection handle, so the real
    _handle_pool_scoped_403 runs on the 403/non-403 paths."""
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter.connections = MagicMock()
    return adapter


def _handle(adapter: ConfluentAdapter) -> MagicMock:
    return adapter.connections.get_thread_connection.return_value.handle


def _adapter_with_phase(phase: Phase | None) -> ConfluentAdapter:
    """Adapter whose get_statement returns a statement in `phase`,
    or raises StatementNotFoundError when phase is None."""
    adapter = _adapter()
    if phase is None:
        _handle(adapter).get_statement.side_effect = StatementNotFoundError(
            statement_name="any-name", message="not found"
        )
    else:
        statement = MagicMock()
        statement.phase = phase
        _handle(adapter).get_statement.return_value = statement
    return adapter


def needs_restart(adapter: ConfluentAdapter, name: str = "any-name") -> bool:
    return adapter.statement_needs_restart(name)


class TestStatementNeedsRestart:
    def test_missing_statement_needs_restart(self):
        assert needs_restart(_adapter_with_phase(None)) is True

    @pytest.mark.parametrize(
        "phase", [Phase.COMPLETED, Phase.STOPPED, Phase.FAILED, Phase.DELETED]
    )
    def test_terminal_phases_need_restart(self, phase: Phase):
        assert needs_restart(_adapter_with_phase(phase)) is True

    @pytest.mark.parametrize(
        "phase",
        [Phase.PENDING, Phase.RUNNING, Phase.DEGRADED, Phase.STOPPING, Phase.DELETING],
    )
    def test_healthy_and_inflight_phases_do_not_restart(self, phase: Phase):
        assert needs_restart(_adapter_with_phase(phase)) is False

    def test_403_needs_restart_and_warns(self):
        """Pool-scoped roles return 403 for a missing / cross-pool statement;
        treat it as missing (restart) and emit a warning."""
        adapter = _adapter()
        _handle(adapter).get_statement.side_effect = OperationalError(
            "error sending request '403'", http_status_code=403
        )
        with patch("dbt.adapters.confluent.impl.fire_event") as fire_event:
            assert needs_restart(adapter) is True
        fire_event.assert_called_once()
        assert "403" in fire_event.call_args.args[0].base_msg

    def test_non_403_operational_error_reraises(self):
        """A non-403 API error is a real failure and must propagate."""
        adapter = _adapter()
        _handle(adapter).get_statement.side_effect = OperationalError(
            "internal server error", http_status_code=500
        )
        with pytest.raises(OperationalError):
            needs_restart(adapter)
