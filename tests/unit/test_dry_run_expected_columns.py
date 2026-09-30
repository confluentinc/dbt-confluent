"""Unit tests for ConfluentAdapter.get_expected_columns_from_dry_run (GH-118).

The connection manager is mocked, but the schema it returns is a real confluent-sql Schema
parsed from dry-run-shaped JSON, so the adapter sees exactly the objects the driver would hand
it.
"""

from unittest.mock import MagicMock

import pytest
from confluent_sql.statement import Schema
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter
from tests.unit._helpers import relation

ID = {"name": "id", "type": {"type": "BIGINT", "nullable": False}}
PRICE = {
    "name": "price",
    "type": {"type": "DECIMAL", "precision": 10, "scale": 2, "nullable": True},
}
VARIANT = {"name": "v", "type": {"type": "VARIANT", "nullable": True}}
MODEL = relation("my_model")


@pytest.fixture
def adapter():
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)  # bypass __init__
    adapter.connections = MagicMock()
    return adapter


@pytest.fixture
def debug_log(mocker):
    return mocker.patch("dbt.adapters.confluent.impl.logger").debug


def _wire(adapter, columns):
    """Make dry_run_schema return a schema with `columns` (None = no schema)."""
    schema = None if columns is None else Schema.from_response({"columns": columns})
    adapter.connections.dry_run_schema.return_value = schema


def test_dry_runs_the_wrapped_select(adapter):
    _wire(adapter, [ID])

    adapter.get_expected_columns_from_dry_run(
        MODEL, "select id from src", execution_mode="streaming_query", compute_pool_id="lfcp-1"
    )

    # The same wrapped SELECT the temp-table CTAS validates.
    adapter.connections.dry_run_schema.assert_called_once_with(
        "SELECT * FROM (\nselect id from src\n) WHERE FALSE",
        execution_mode="streaming_query",
        compute_pool_id="lfcp-1",
    )


def test_mode_and_pool_default_to_none(adapter):
    _wire(adapter, [ID])
    adapter.get_expected_columns_from_dry_run(MODEL, "select id from src")
    _, kwargs = adapter.connections.dry_run_schema.call_args
    assert kwargs == {"execution_mode": None, "compute_pool_id": None}


def test_renders_columns_in_query_order(adapter):
    _wire(adapter, [PRICE, ID])

    columns = adapter.get_expected_columns_from_dry_run(MODEL, "select price, id from src")

    assert columns == {"price": "DECIMAL(10, 2)", "id": "BIGINT"}
    assert list(columns) == ["price", "id"]


def test_unverified_type_returns_none(adapter, debug_log):
    """None tells check_for_schema_drift to use the temp-table fallback. The debug line names
    the model and the column, since text logs carry no node info with threads > 1."""
    _wire(adapter, [ID, VARIANT])

    assert adapter.get_expected_columns_from_dry_run(MODEL, "select id, v from src") is None

    (message,), _ = debug_log.call_args
    assert str(MODEL) in message
    assert "'v'" in message
    assert "VARIANT" in message


@pytest.mark.parametrize("columns", [None, []], ids=["no-schema", "no-columns"])
def test_missing_schema_returns_none(adapter, debug_log, columns):
    """A dry-run schema arrives in the synchronous POST response, so an empty one is
    deterministic: fall back rather than raise with retry advice that can't help."""
    _wire(adapter, columns)

    assert adapter.get_expected_columns_from_dry_run(MODEL, "select id from src") is None

    (message,), _ = debug_log.call_args
    assert str(MODEL) in message
    assert "retry" not in message.lower()


def test_duplicate_column_names_return_none(adapter, debug_log):
    """A CTAS rejects duplicate output names, but a dict would silently collapse them."""
    _wire(adapter, [ID, {"name": "id", "type": {"type": "INTEGER", "nullable": True}}, PRICE])

    assert adapter.get_expected_columns_from_dry_run(MODEL, "select id, id, price") is None

    (message,), _ = debug_log.call_args
    assert str(MODEL) in message
    assert "'id'" in message


def test_dry_run_error_propagates(adapter):
    """dry_run_schema raises DbtDatabaseError for invalid SQL; the resolver must not swallow
    it (the old temp-table CTAS failed the same way)."""
    adapter.connections.dry_run_schema.side_effect = DbtDatabaseError(
        "SQL validation failed. Column 'nope' not found in any table"
    )
    with pytest.raises(DbtDatabaseError, match="Column 'nope' not found"):
        adapter.get_expected_columns_from_dry_run(MODEL, "select nope from src")
