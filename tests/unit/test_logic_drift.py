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
        stored_statement = _new_shape(M)
        with pytest.raises(CompilationError) as exc:
            adapter.check_logic_drift(
                relation("my_model"), "dbt-p-my_model", stored_statement, M + " and x = 1"
            )
        message = str(exc.value)
        assert "SQL logic drift detected" in message
        assert "my_model" in message and "'dbt-p-my_model'" in message
        assert "The running statement's SQL:" in message and "amount > 100" in message
        assert "The model's current SQL:" in message and "and x = 1" in message
        assert "--full-refresh" in message
        assert "stop or delete the statement and re-run" in message
        assert "on_schema_drift='ignore'" not in message

    def test_long_sql_is_cut(self):
        adapter = _adapter()
        long_model_sql = "select " + "x, " * 5000 + "y from t"
        stored_statement = _new_shape("select a from t")
        with pytest.raises(CompilationError) as exc:
            adapter.check_logic_drift(
                relation("my_model"), "dbt-p-my_model", stored_statement, long_model_sql
            )
        message = str(exc.value)
        assert "more characters" in message
        assert len(message) < len(long_model_sql) + 2000
