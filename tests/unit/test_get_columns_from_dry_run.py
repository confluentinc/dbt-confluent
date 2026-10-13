"""Unit tests for ConfluentAdapter.get_columns_from_dry_run.

The connection manager is mocked, but the schemas it returns are real confluent-sql Schemas
parsed from dry-run-shaped JSON, so the adapter sees exactly the objects the driver would hand
it.
"""

from unittest.mock import MagicMock, call

import pytest
from confluent_sql.statement import Schema
from confluent_sql.types import ColumnTypeDefinition
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter, DryRunColumns
from tests.unit._helpers import drift_catalog_row, make_drift_catalog, relation

ID = {"name": "id", "type": {"type": "BIGINT", "nullable": False}}
PRICE = {
    "name": "price",
    "type": {"type": "DECIMAL", "precision": 10, "scale": 2, "nullable": True},
}
MODEL = relation("my_model")
SELECT_EXISTING = "SELECT * FROM `env-1`.`cluster-a`.`my_model`"
# get_drift_catalog's result for a plain existing table: it has columns, and isn't materialized.
CATALOG = make_drift_catalog(
    [
        drift_catalog_row(
            section="COLUMNS", table_name="my_model", col_name="id", data_type="BIGINT"
        )
    ]
)


@pytest.fixture
def adapter():
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)  # bypass __init__
    adapter.connections = MagicMock()
    return adapter


@pytest.fixture
def debug_log(mocker):
    return mocker.patch("dbt.adapters.confluent.impl.logger").debug


def _schema(columns):
    return None if columns is None else Schema.from_response({"columns": columns})


def _wire(adapter, expected, existing=None):
    """dry_run_schema answers the model's SELECT with `expected` columns, then the existing
    table's SELECT * with `existing` (None = no schema)."""
    adapter.connections.dry_run_schema.side_effect = [_schema(expected), _schema(existing)]


def _type(column):
    return ColumnTypeDefinition.from_response(column["type"])


def test_dry_runs_the_model_select_then_the_existing_table(adapter):
    _wire(adapter, [ID], [ID])

    adapter.get_columns_from_dry_run(
        MODEL,
        CATALOG,
        "select id from src",
        execution_mode="streaming_query",
        compute_pool_id="lfcp-1",
    )

    # The model's SELECT as written (no wrapper), then the existing table, in the same mode
    # and on the same pool.
    assert adapter.connections.dry_run_schema.call_args_list == [
        call("select id from src", execution_mode="streaming_query", compute_pool_id="lfcp-1"),
        call(SELECT_EXISTING, execution_mode="streaming_query", compute_pool_id="lfcp-1"),
    ]


def test_mode_and_pool_default_to_none(adapter):
    _wire(adapter, [ID], [ID])
    adapter.get_columns_from_dry_run(MODEL, CATALOG, "select id from src")
    for _, kwargs in adapter.connections.dry_run_schema.call_args_list:
        assert kwargs == {"execution_mode": None, "compute_pool_id": None}


def test_returns_both_sides_in_query_order_with_raw_types(adapter):
    """The driver's types come back untouched, nullability included: check_schema_drift
    decides what to compare."""
    existing_price = {**PRICE, "type": {**PRICE["type"], "nullable": False}}
    _wire(adapter, [PRICE, ID], [ID, existing_price])

    columns = adapter.get_columns_from_dry_run(MODEL, CATALOG, "select price, id from src")

    assert columns == DryRunColumns(
        existing={"id": _type(ID), "price": _type(existing_price)},
        expected={"price": _type(PRICE), "id": _type(ID)},
    )
    assert list(columns.expected) == ["price", "id"]
    assert list(columns.existing) == ["id", "price"]


@pytest.mark.parametrize("columns", [None, []], ids=["no-schema", "no-columns"])
def test_model_without_schema_returns_none(adapter, debug_log, columns):
    """A dry-run schema arrives in the synchronous submission response, so an empty one is
    deterministic: fall back rather than raise with retry advice that can't help. The
    existing table isn't dry-run. The debug line names the model, since text logs carry no
    node info with threads > 1."""
    _wire(adapter, columns)

    assert adapter.get_columns_from_dry_run(MODEL, CATALOG, "select id from src") is None

    adapter.connections.dry_run_schema.assert_called_once()
    (message,), _ = debug_log.call_args
    assert str(MODEL) in message
    assert "the model's SELECT" in message
    assert "retry" not in message.lower()


def test_existing_without_schema_returns_none(adapter, debug_log):
    _wire(adapter, [ID], [])

    assert adapter.get_columns_from_dry_run(MODEL, CATALOG, "select id from src") is None

    (message,), _ = debug_log.call_args
    assert str(MODEL) in message
    assert "the existing table" in message


def test_duplicate_column_names_return_none(adapter, debug_log):
    """A CTAS rejects duplicate output names, but a dict would silently collapse them."""
    _wire(adapter, [ID, {"name": "id", "type": {"type": "INTEGER", "nullable": True}}, PRICE])

    assert adapter.get_columns_from_dry_run(MODEL, CATALOG, "select id, id, price") is None

    adapter.connections.dry_run_schema.assert_called_once()
    (message,), _ = debug_log.call_args
    assert str(MODEL) in message
    assert "'id'" in message


@pytest.mark.parametrize("failing", [0, 1], ids=["model", "existing"])
def test_dry_run_error_propagates(adapter, failing):
    """dry_run_schema raises DbtDatabaseError for invalid SQL, or for an existing table dropped
    mid-run; the resolver must not swallow it (the old temp-table CTAS failed the same way)."""
    answers = [_schema([ID]), _schema([ID])]
    answers[failing] = DbtDatabaseError("Dry-run failed: Column 'nope' not found in any table")
    adapter.connections.dry_run_schema.side_effect = answers

    with pytest.raises(DbtDatabaseError, match="Column 'nope' not found"):
        adapter.get_columns_from_dry_run(MODEL, CATALOG, "select nope from src")


def test_materialized_table_raises_before_any_dry_run(adapter):
    """The catalog runs first, so a Flink materialized table gets its dedicated error without
    paying for the dry-runs."""
    catalog = make_drift_catalog(
        [
            drift_catalog_row(
                section="COLUMNS", table_name="my_model", col_name="id", data_type="BIGINT"
            ),
            drift_catalog_row(
                section="TABLES", table_name="my_model", is_distributed="NO", is_materialized="YES"
            ),
        ]
    )

    with pytest.raises(CompilationError, match="exists as a Flink materialized table"):
        adapter.get_columns_from_dry_run(MODEL, catalog, "select id from src")

    adapter.connections.dry_run_schema.assert_not_called()


def test_unreadable_existing_table_raises_before_any_dry_run(adapter):
    """No COLUMNS rows for the existing table (metadata lag, or dropped outside dbt) gets the
    catalog's "existing schema" error, not a dry-run failure."""
    with pytest.raises(DbtDatabaseError, match="could not introspect the existing schema"):
        adapter.get_columns_from_dry_run(MODEL, make_drift_catalog([]), "select id from src")

    adapter.connections.dry_run_schema.assert_not_called()
