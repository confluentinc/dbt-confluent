"""Unit tests for ConfluentConnectionManager.dry_run_schema (GH-118).

The thread connection's handle is an autospec of confluent_sql.Connection, so a call that
doesn't match the driver's dry_run_statement signature fails here, and nothing is submitted.
The Statement it hands back is a real confluent-sql Statement parsed from a dry-run-shaped
response.
"""

import json
import re
from unittest.mock import MagicMock, create_autospec

import confluent_sql
import pytest
from confluent_sql.execution_mode import ExecutionMode
from confluent_sql.statement import Statement
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from dbt.adapters.confluent.connections import ConfluentConnectionManager
from dbt.adapters.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus

COLUMNS = [{"name": "id", "type": {"type": "BIGINT", "nullable": False}}]
COMMENT = '/* {"node_id": "model.proj.my_model"} */\n'


def dry_run_response(sql_kind: str = "SELECT", columns: list | None = COLUMNS) -> dict:
    """A dry-run submission response, shaped like Flink's. A DDL dry-run reports `schema: {}`."""
    return {
        "name": "dbapi-8f0c6d4e-1b7a-4c1e-9d2f-3a5b6c7d8e9f",
        "metadata": {"uid": ""},
        "spec": {"statement": "SELECT 1", "properties": {"sql.dry-run": "true"}},
        "status": {
            "phase": "COMPLETED",
            "traits": {
                "is_append_only": True,
                "is_bounded": True,
                "sql_kind": sql_kind,
                "schema": {"columns": columns} if columns is not None else {},
            },
        },
    }


@pytest.fixture
def events(monkeypatch):
    fire_event = MagicMock()
    monkeypatch.setattr("dbt.adapters.confluent.connections.fire_event", fire_event)
    return fire_event


@pytest.fixture
def handle():
    handle = create_autospec(confluent_sql.Connection, instance=True)
    handle.dry_run_statement.return_value = Statement.from_response(handle, dry_run_response())
    return handle


@pytest.fixture
def manager(handle, events):
    """A manager with no profile. The query header is set the way dbt's set_query_header
    would, so the real `_add_query_comment` runs, and the real `get_thread_handle` reads the
    mocked thread connection's handle."""
    connection = MagicMock()
    connection.name = "model.proj.my_model"
    connection.credentials.execution_mode = "streaming_query"
    connection.handle = handle
    manager = ConfluentConnectionManager.__new__(ConfluentConnectionManager)  # bypass __init__
    manager.query_header = MagicMock()
    manager.query_header.add.side_effect = lambda sql: f"{COMMENT}{sql}"
    manager.get_thread_connection = MagicMock(return_value=connection)
    return manager


def test_dry_runs_commented_sql_in_the_given_mode(manager, handle):
    schema = manager.dry_run_schema("SELECT 1", execution_mode="snapshot")

    assert [(column.name, column.type.type) for column in schema.columns] == [("id", "BIGINT")]
    # The dry-run carries dbt's query comment, like the temp-table CTAS it replaces.
    handle.dry_run_statement.assert_called_once_with(
        f"{COMMENT}SELECT 1", mode=ExecutionMode.SNAPSHOT, compute_pool_id=None
    )


def test_mode_and_pool_default_to_the_connection(manager, handle):
    manager.dry_run_schema("SELECT 1")
    _, kwargs = handle.dry_run_statement.call_args
    assert kwargs == {"mode": ExecutionMode.STREAMING_QUERY, "compute_pool_id": None}


def test_compute_pool_is_passed_through(manager, handle):
    manager.dry_run_schema("SELECT 1", compute_pool_id="lfcp-1")
    _, kwargs = handle.dry_run_statement.call_args
    assert kwargs["compute_pool_id"] == "lfcp-1"


def test_no_result_schema_returns_none(manager, handle):
    """A DDL dry-run reports `schema: {}`, which the driver parses as None."""
    handle.dry_run_statement.return_value = Statement.from_response(
        handle, dry_run_response(sql_kind="CREATE_TABLE", columns=None)
    )
    assert manager.dry_run_schema("CREATE TABLE t (id INT)") is None


@pytest.mark.parametrize(
    "message",
    [
        # Flink rejects the statement (invalid SQL).
        "Dry-run failed: Column 'nope' not found in any table",
        # The request fails. Nothing is retried, so a 429 or 5xx fails the check.
        "error sending request '429' - Too Many Requests",
        # The response isn't final, e.g. an exhausted compute pool.
        "Dry-run came back in non-terminal phase PENDING; a dry-run is expected to be answered"
        " in full by the submission response.",
    ],
)
def test_driver_errors_become_database_errors(manager, handle, message):
    """Every confluent-sql error surfaces as a DbtDatabaseError carrying the driver's text."""
    handle.dry_run_statement.side_effect = confluent_sql.OperationalError(message)
    with pytest.raises(DbtDatabaseError, match=re.escape(message)):
        manager.dry_run_schema("SELECT nope FROM src")
    handle.dry_run_statement.assert_called_once()


def test_other_errors_become_runtime_errors(manager, handle):
    """An error the driver doesn't wrap, such as a response body that isn't JSON, surfaces as
    a DbtRuntimeError (DbtDatabaseError is a subclass, so check the exact type)."""
    handle.dry_run_statement.side_effect = json.JSONDecodeError("Expecting value", "", 0)
    with pytest.raises(DbtRuntimeError, match="Expecting value") as excinfo:
        manager.dry_run_schema("SELECT 1")
    assert excinfo.type is DbtRuntimeError


def test_fires_the_add_query_events(manager, events):
    manager.dry_run_schema("SELECT 1")
    fired = [call.args[0] for call in events.call_args_list]
    assert [type(event) for event in fired] == [ConnectionUsed, SQLQuery, SQLQueryStatus]
    assert fired[1].sql == f"{COMMENT}SELECT 1"
    assert fired[2].status == "Phase.COMPLETED"
