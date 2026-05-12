"""Unit tests for ConfluentAdapter.classify_existing_statement.

The classifier is the decision input for the streaming-statement
recovery path in `decide_action` (Jinja). Terminal phases trigger a
restart; healthy phases (including in-flight transitions) skip; a
404 from the API also triggers a restart.
"""

from unittest.mock import MagicMock

import pytest
from confluent_sql.exceptions import StatementNotFoundError
from confluent_sql.statement import Phase

from dbt.adapters.confluent.impl import ConfluentAdapter


def _adapter_with_phase(phase: Phase | None) -> MagicMock:
    """Build a mock adapter that returns a statement in `phase`,
    or raises StatementNotFoundError when phase is None."""
    adapter = MagicMock()
    handle = adapter.connections.get_thread_connection.return_value.handle
    if phase is None:
        handle.get_statement.side_effect = StatementNotFoundError(
            statement_name="any-name", message="not found"
        )
    else:
        statement = MagicMock()
        statement.phase = phase
        handle.get_statement.return_value = statement
    return adapter


def classify(adapter: MagicMock, name: str = "any-name") -> str:
    return ConfluentAdapter.classify_existing_statement(adapter, name)


class TestClassifyExistingStatement:
    def test_missing_statement_returns_missing(self):
        assert classify(_adapter_with_phase(None)) == "missing"

    @pytest.mark.parametrize(
        "phase", [Phase.COMPLETED, Phase.STOPPED, Phase.FAILED, Phase.DELETED]
    )
    def test_terminal_phases_return_terminal(self, phase: Phase):
        assert classify(_adapter_with_phase(phase)) == "terminal"

    @pytest.mark.parametrize(
        "phase",
        [Phase.PENDING, Phase.RUNNING, Phase.DEGRADED, Phase.STOPPING, Phase.DELETING],
    )
    def test_healthy_and_inflight_phases_return_healthy(self, phase: Phase):
        assert classify(_adapter_with_phase(phase)) == "healthy"
