"""Shared helpers for functional adapter tests.

Underscore-prefixed so pytest does not treat it as a test module.
"""

import time

from confluent_sql.exceptions import OperationalError, StatementNotFoundError


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
    """Block until `name` reaches RUNNING so the adapter classifies it as healthy
    (adoptable). Fails fast if it reaches a terminal phase instead."""
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


def delete_statements_by_label(project, label):
    """Delete every Flink statement carrying `label`, freeing compute-pool
    resources. The adapter can't drop schemas, so test teardown is statement-
    and table-scoped instead. No-op if `label` is falsy."""
    if not label:
        return
    with project.adapter.connection_named("cleanup"):
        conn = project.adapter.connections.get_thread_connection()
        for statement in conn.handle.list_statements(label=label):
            # Use the adapter helper so a missing statement / pool-scoped 403 is
            # swallowed rather than failing teardown. Deletion is async and not
            # awaited here; that's fine for cleanup, which doesn't reuse names.
            project.adapter.delete_statement(statement.name)


def drop_tables(project, *names):
    """Drop each named table if it exists. Pairs with delete_statements_by_label
    so teardown removes both the statements and the relations they created."""
    for name in names:
        project.run_sql(f"drop table if exists {name}")
