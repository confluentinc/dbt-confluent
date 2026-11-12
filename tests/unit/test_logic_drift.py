"""Unit tests for ConfluentAdapter.check_logic_drift."""

from unittest.mock import MagicMock

import pytest

from dbt.adapters.confluent.impl import ConfluentAdapter
from dbt.exceptions import CompilationError
from tests.unit._helpers import relation
from tests.unit.test_statement_sql import M, _new_shape


def _adapter() -> ConfluentAdapter:
    """A real adapter instance with a mocked connection handle."""
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter.connections = MagicMock()
    return adapter


class TestCheckLogicDrift:
    def test_matching_sql_passes(self):
        adapter = _adapter()
        adapter.check_logic_drift(relation("my_model"), "dbt-p-my_model", _new_shape(M), M)

    def test_changed_sql_raises_with_advice(self):
        adapter = _adapter()
        with pytest.raises(CompilationError) as exc:
            adapter.check_logic_drift(
                relation("my_model"), "dbt-p-my_model", _new_shape(M), M + " and x = 1"
            )
        message = str(exc.value)
        assert "SQL logic drift detected" in message
        assert "my_model" in message and "'dbt-p-my_model'" in message
        assert "--full-refresh" in message and "on_schema_drift='ignore'" in message
