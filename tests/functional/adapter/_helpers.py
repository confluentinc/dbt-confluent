"""Shared helpers for functional adapter tests.

Underscore-prefixed so pytest does not treat it as a test module.
"""

import time
import uuid
from typing import Literal, TypedDict

import agate
import pytest
from confluent_sql import ArtifactNotFoundError, Connection
from confluent_sql.exceptions import OperationalError, StatementNotFoundError
from confluent_sql.execution_mode import ExecutionMode

from dbt.adapters.confluent.utils import fetch_from_cursor


def wait_for_absent(adapter, name, timeout=60):
    """Block until `name` is fully gone (get_statement reports it missing).

    adapter.delete_statement() does not await async teardown — the production
    restart path tolerates the lingering name via add_query's 409-retry on
    CREATE. Tests that re-submit the same name through the raw cursor (no such
    retry) must wait for the name to actually free before planting, or they race
    the teardown and hit a 409 Conflict.

    "Missing" is a 404 (StatementNotFoundError) or, under compute-pool-scoped
    roles, a 403 — the same condition the adapter treats as missing — so we
    accept both rather than spinning to timeout on the 403.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with adapter.connection_named("absence_poll"):
            conn = adapter.connections.get_thread_connection()
            try:
                conn.handle.get_statement(name)
            except StatementNotFoundError:
                return
            except OperationalError as e:
                if getattr(e, "http_status_code", None) == 403:
                    return
                raise
        time.sleep(2)
    raise AssertionError(f"Statement {name} was not freed within {timeout}s of deletion")


def wait_for_terminal(adapter, name, timeout=30):
    """Block until `name` reaches a terminal phase (COMPLETED/STOPPED/FAILED/DELETED)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with adapter.connection_named("phase_poll"):
            conn = adapter.connections.get_thread_connection()
            stmt = conn.handle.get_statement(name)
            if stmt.phase.is_terminal:
                return stmt
        time.sleep(2)
    raise AssertionError(f"Statement {name} did not reach terminal state in {timeout}s")


def wait_for_running(adapter, name, timeout=60):
    """Block until `name` reaches RUNNING. Fails fast if it reaches a terminal
    phase instead.

    The adapter classifies *any* non-terminal phase as healthy/adoptable
    (PENDING, RUNNING, DEGRADED, STOPPING, DELETING), so RUNNING is stricter
    than strictly necessary. We wait for it anyway as a deterministic
    "stably healthy" signal: a faker-backed INSERT reaches RUNNING within
    seconds, and waiting avoids racing a transient PENDING that could still
    flip to a terminal phase before the assertion runs."""
    deadline = time.monotonic() + timeout
    last_phase = None
    while time.monotonic() < deadline:
        with adapter.connection_named("running_poll"):
            conn = adapter.connections.get_thread_connection()
            stmt = conn.handle.get_statement(name)
        last_phase = stmt.phase
        if stmt.phase.name == "RUNNING":
            return stmt
        if stmt.phase.is_terminal:
            raise AssertionError(f"Statement {name} reached terminal phase {stmt.phase}")
        time.sleep(2)
    raise AssertionError(
        f"Statement {name} never reached RUNNING in {timeout}s (last: {last_phase})"
    )


def relation(project, name):
    """Build a Relation for a model that lives in the test project's schema."""
    return project.adapter.Relation.create(identifier=name)


def snapshot_query(project, sql: str, fetch: Literal["one", "all"]) -> agate.Table:
    """Run `sql` in Confluent's SNAPSHOT execution mode and return its result.

    project.run_sql defaults to STREAMING_QUERY (see
    ConfluentAdapter.run_sql_for_tests), which can hand back a preliminary
    changelog result before the underlying query has actually finished
    processing -- a real risk for assertions on an exact row count or exact
    row set, which would then see a false negative. SNAPSHOT mode instead
    runs the query to completion and returns its final, bounded result.
    """
    with project.adapter.connection_named("snapshot_query"):
        conn = project.adapter.connections.get_thread_connection()
        # closing_cursor auto-closes on exit (mode kept explicit even though
        # SNAPSHOT is its default) -- unlike a bare cursor.execute, it doesn't
        # attach the test profile's statement_label or statement_name_prefix
        # on its own:
        # - Without the label, the sweep can't find these
        #   polling statements at normal teardown.
        # - Without the prefix, the driver defaults to its own
        #   "dbapi-{uuid}" name (see confluent_sql.connection), which
        #   isolation.sweep_test_resources' name-prefix filter doesn't match --
        #   so a crashed run (no normal teardown) would leave these unswept
        #   until Confluent's ~30-day purge.
        with conn.handle.closing_cursor(mode=ExecutionMode.SNAPSHOT) as cursor:
            cursor.execute(
                sql,
                statement_name=f"{conn.credentials.statement_name_prefix}{uuid.uuid4()}",
                statement_labels=[conn.credentials.statement_label],
            )
            if fetch == "one":
                return fetch_from_cursor(cursor, limit=1)
            elif fetch == "all":
                return fetch_from_cursor(cursor)
            raise ValueError(f"fetch must be 'one' or 'all', got: {fetch!r}")


def wait_for_snapshot_rows(
    project, sql: str, min_rows: int = 1, timeout: int = 30, interval: int = 2
) -> agate.Table:
    """Poll `sql` via snapshot_query until it returns at least `min_rows` rows."""
    deadline = time.monotonic() + timeout
    rows = snapshot_query(project, sql, fetch="all")
    while len(rows) < min_rows and time.monotonic() < deadline:
        time.sleep(interval)
        rows = snapshot_query(project, sql, fetch="all")
    return rows


def get_result_by_name(results, name):
    """Extract a specific result by node name from run results."""
    for result in results:
        if result.node.name == name:
            return result
    return None


def assert_drift_error(results, name):
    """Assert that a specific result failed with a drift detection error."""
    result = get_result_by_name(results, name)
    assert result is not None, f"{name} not found in results"
    assert result.status.name == "Error", (
        f"{name} expected status 'Error' but got '{result.status.name}'"
    )
    assert "drift detected" in result.message.lower(), (
        f"{name} error was not a drift error: {result.message}"
    )


def assert_distribution_drift_error(results, name):
    """Assert that a specific result failed with a distribution-drift error.

    With the new collect-and-raise format the wrapper says "Schema drift
    detected" once and each violation appears as a bullet line ("  - ...");
    distribution violations always start with the literal "distribution" prefix
    ("distribution: ...", "distribution columns: ...", "distribution buckets: ...").
    We match on the "- distribution" bullet prefix so a relation whose name
    happens to contain "distribution" can't produce a false positive.
    """
    result = get_result_by_name(results, name)
    assert result is not None, f"{name} not found in results"
    assert result.status.name == "Error", (
        f"{name} expected status 'Error' but got '{result.status.name}'"
    )
    msg_lower = result.message.lower()
    assert "schema drift detected" in msg_lower, (
        f"{name} error was not a schema drift error: {result.message}"
    )
    assert "- distribution" in msg_lower, (
        f"{name} schema drift error did not include a distribution violation: {result.message}"
    )


def assert_tables_absent(project, *names):
    """Assert none of `names` exist in the test schema. Used to prove
    post_model_hook cleanup: temp tables (schema-check, unit-test fixtures)
    are dropped by the hook, not inline, so a leftover means the hook didn't
    run or didn't drop. Exact names rather than a prefix scan on purpose: the
    shared test schema still holds UUID-suffixed temp tables leaked by
    pre-deterministic-naming adapter versions, which a prefix match would
    false-positive on."""
    rows = project.run_sql("show tables", fetch="all")
    existing = {row[0] for row in rows}
    leaked = sorted(set(names) & existing)
    assert not leaked, f"Temp tables leaked past post_model_hook cleanup: {leaked}"


def drop_materialized_table_if_exists(project, name, attempts=16, interval=10):
    """Drop a materialized table, raising if it can't be dropped.

    MTs need the dedicated `DROP MATERIALIZED TABLE IF EXISTS` — a regular
    DROP TABLE phantom-drops them (see ConfluentAdapter.drop_relation). Waits
    out the transient rejection ("being modified" / "Could not execute
    DropTable") while a prior CREATE OR ALTER is still establishing, and
    raises the last error once the attempts run out.
    """
    for i in range(attempts):
        try:
            project.run_sql(f"drop materialized table if exists `{name}`")
            return
        except Exception as e:
            msg = str(e).lower()
            if i < attempts - 1 and ("being modified" in msg or "could not execute" in msg):
                time.sleep(interval)
                continue
            raise


def drop_any_relation(project, name):
    """Drop `name` whatever it currently is, raising if it can't be dropped.

    Used by teardown in test classes where the name's kind depends on how far
    the test got — e.g. the MT switch-guard test ends with either a regular
    table (guard-error path) or a materialized table (--full-refresh path).
    DROP MATERIALIZED TABLE IF EXISTS goes first and is a no-op on an absent or
    regular table; if it raises, so does this, so the DROP TABLE that follows
    can never phantom-drop an MT (see ConfluentAdapter.drop_relation). DROP
    TABLE IF EXISTS then handles the absent/regular/inferred cases (and
    deletes the table's topic).
    """
    drop_materialized_table_if_exists(project, name)
    project.run_sql(f"drop table if exists `{name}`")


class SubmittedStatement(TypedDict):
    """A statement recorded by capture_submitted_statement_properties."""

    name: str
    sql: str
    properties: dict[str, str | int | bool]
    is_dry_run: bool


def capture_submitted_statement_properties(
    monkeypatch: pytest.MonkeyPatch,
) -> list[SubmittedStatement]:
    """Patch ConfluentConnectionManager.add_query and confluent_sql.Connection.dry_run_statement
    to record every statement the caller's `dbt run` submits, in order, as a dict with keys
    `name` (the statement name; a dry-run's is `dbapi-<uuid>`), `sql` (as submitted, query
    comment included), `properties` (as the server echoed them back) and `is_dry_run` (the
    driver's `Statement.is_dry_run`).

    Some statements are reaped (deleted server-side) the instant they
    complete -- e.g. materialized_table's DDL, submitted under a per-run name
    and deleted by the driver as soon as the CREATE OR ALTER finishes (see
    materialized_table.sql) -- so a post-hoc get_statement() lookup after
    `dbt run` returns would always 404. A `sql.dry-run` statement is never
    stored at all, and is captured from `dry_run_statement`. Capturing here,
    at add_query's return (after the statement is submitted but before the
    caller's execute() wrapper closes the cursor and triggers that deletion),
    is the only way to observe them. A statement whose submission raises is
    not recorded.

    Returns the list that accumulates as the run proceeds; inspect it after
    `run_dbt`.
    """
    from dbt.adapters.confluent.connections import ConfluentConnectionManager

    captured: list[SubmittedStatement] = []
    original_add_query = ConfluentConnectionManager.add_query

    def add_query_and_capture(self, sql, *args, **kwargs):
        connection, cursor = original_add_query(self, sql, *args, **kwargs)
        captured.append(
            {
                "name": cursor.statement.name,
                "sql": sql,
                "properties": dict(cursor.statement.properties),
                "is_dry_run": cursor.statement.is_dry_run,
            }
        )
        return connection, cursor

    monkeypatch.setattr(ConfluentConnectionManager, "add_query", add_query_and_capture)

    original_dry_run_statement = Connection.dry_run_statement

    def dry_run_statement_and_capture(self, statement_text, *args, **kwargs):
        statement = original_dry_run_statement(self, statement_text, *args, **kwargs)
        captured.append(
            {
                "name": statement.name,
                "sql": statement_text,
                "properties": dict(statement.properties),
                "is_dry_run": statement.is_dry_run,
            }
        )
        return statement

    monkeypatch.setattr(Connection, "dry_run_statement", dry_run_statement_and_capture)
    return captured
