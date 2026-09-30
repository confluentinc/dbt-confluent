"""Unit tests for ConfluentConnectionManager.dry_run_schema (GH-118).

add_query is replaced per test, so nothing is submitted. The cursor is a mock, but the schema
it hands back is a real confluent-sql Schema parsed from dry-run-shaped JSON.
"""

from unittest.mock import MagicMock, PropertyMock

import confluent_sql
import pytest
from confluent_sql.statement import Schema
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.connections import ConfluentConnectionManager

SCHEMA = Schema.from_response(
    {"columns": [{"name": "id", "type": {"type": "BIGINT", "nullable": False}}]}
)
COMMENT = '/* {"node_id": "model.proj.my_model"} */\n'


@pytest.fixture
def manager():
    """A manager with no profile or connection. The query header is set the way dbt's
    set_query_header would, so the real `_add_query_comment` runs."""
    manager = ConfluentConnectionManager.__new__(ConfluentConnectionManager)  # bypass __init__
    manager.query_header = MagicMock()
    manager.query_header.add.side_effect = lambda sql: f"{COMMENT}{sql}"
    manager.add_query = MagicMock()
    return manager


def _cursor(manager, schema):
    cursor = MagicMock()
    cursor.statement.schema = schema
    manager.add_query.return_value = (MagicMock(), cursor)
    return cursor


def test_submits_commented_sql_as_hidden_dry_run(manager):
    cursor = _cursor(manager, SCHEMA)

    schema = manager.dry_run_schema(
        "SELECT 1", execution_mode="snapshot", compute_pool_id="lfcp-1"
    )

    assert schema is SCHEMA
    args, kwargs = manager.add_query.call_args
    # The dry-run carries dbt's query comment, like the temp-table CTAS it replaces.
    assert args == (f"{COMMENT}SELECT 1",)
    assert kwargs == {
        "auto_begin": False,
        "execution_mode": "snapshot",
        "hidden": True,
        "compute_pool_id": "lfcp-1",
        "statement_properties": {"sql.dry-run": "true"},
    }
    cursor.close.assert_called_once()


def test_mode_and_pool_default_to_none(manager):
    """None lets add_query fall back to the profile's execution mode and default pool."""
    _cursor(manager, SCHEMA)
    manager.dry_run_schema("SELECT 1")
    _, kwargs = manager.add_query.call_args
    assert kwargs["execution_mode"] is None
    assert kwargs["compute_pool_id"] is None


def test_no_result_schema_returns_none(manager):
    """A DDL dry-run reports `schema: {}`, which the driver parses as None."""
    cursor = _cursor(manager, None)
    assert manager.dry_run_schema("CREATE TABLE t (id INT)") is None
    cursor.close.assert_called_once()


def test_driver_error_reading_schema_becomes_database_error(manager):
    """A driver error while reading the schema must not escape as a raw confluent_sql
    exception, and the cursor is still closed."""
    cursor = MagicMock()
    type(cursor).statement = PropertyMock(
        side_effect=confluent_sql.InterfaceError("Statement traits are not available")
    )
    manager.add_query.return_value = (MagicMock(), cursor)

    with pytest.raises(DbtDatabaseError, match="traits are not available"):
        manager.dry_run_schema("SELECT 1")
    cursor.close.assert_called_once()


def test_submission_error_propagates(manager):
    """add_query's exception_handler already turns a FAILED dry-run (invalid SQL) into a
    DbtDatabaseError carrying the server's detail."""
    manager.add_query.side_effect = DbtDatabaseError(
        "SQL validation failed. Column 'nope' not found in any table"
    )
    with pytest.raises(DbtDatabaseError, match="Column 'nope' not found"):
        manager.dry_run_schema("SELECT nope FROM src")
