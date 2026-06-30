"""Shared helpers for functional adapter tests.

Underscore-prefixed so pytest does not treat it as a test module.
"""

import time


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


def drop_materialized_table(project, name, attempts=16, interval=10):
    """Best-effort drop of a materialized table; returns True if it's gone.

    A materialized table can't be dropped with `DROP TABLE` ("not a regular
    table"), so teardown for MT models must use `DROP MATERIALIZED TABLE`. This
    waits out the transient state ("being modified" / "Could not execute
    DropTable") that occurs while a prior CREATE OR ALTER is still establishing.

    Ordering matters: an MT stays tied to its defining statement, so the table
    must be dropped *before* any statement is deleted — deleting the statement
    while the table still exists orphans (wedges) it. Callers should therefore
    only delete statements when this returns True.
    """
    for i in range(attempts):
        try:
            project.run_sql(f"drop materialized table `{name}`")
            return True
        except Exception as e:
            msg = str(e).lower()
            if "does not exist" in msg:
                return True
            if i < attempts - 1 and ("being modified" in msg or "could not execute" in msg):
                time.sleep(interval)
                continue
            return False  # give up; do NOT delete statements (would wedge it)
