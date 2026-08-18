"""Shared helpers for functional adapter tests.

Underscore-prefixed so pytest does not treat it as a test module.
"""

import time
from datetime import datetime, timezone

from confluent_sql.exceptions import OperationalError, StatementNotFoundError

# Leftovers younger than this may belong to a session still running on the
# shared cluster (a full suite run takes minutes; two hours is a generous
# margin), so the sweepers below leave them for a later sweep.
SWEEP_MIN_AGE_SECONDS = 2 * 60 * 60


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


def drop_tables(project, *names):
    """Drop each named table if it exists. Pairs with delete_statements_by_label
    so teardown removes both the statements and the relations they created."""
    for name in names:
        project.run_sql(f"drop table if exists {name}")


def drop_materialized_table(project, name, attempts=16, interval=10):
    """Best-effort drop of a materialized table; returns True if it's gone.

    MTs need the dedicated `DROP MATERIALIZED TABLE IF EXISTS` — a regular
    DROP TABLE phantom-drops them (see ConfluentAdapter.drop_relation). Waits
    out the transient rejection ("being modified" / "Could not execute
    DropTable") while a prior CREATE OR ALTER is still establishing.

    Callers gate statement deletion on the return value: if the drop kept
    failing, leave the statements too and let the next run's teardown (or a
    manual sweep) clean up the lot.
    """
    for i in range(attempts):
        try:
            project.run_sql(f"drop materialized table if exists `{name}`")
            return True
        except Exception as e:
            msg = str(e).lower()
            if i < attempts - 1 and ("being modified" in msg or "could not execute" in msg):
                time.sleep(interval)
                continue
            return False  # give up; caller leaves statements for a later sweep


def drop_any_relation(project, name):
    """Drop `name` whatever it currently is; returns True if it's gone.

    Used by teardown in test classes where the name's kind depends on how far
    the test got — e.g. the MT switch-guard test ends with either a regular
    table (guard-error path) or a materialized table (--full-refresh path).
    The IS_MATERIALIZED pre-check routes MTs to DROP MATERIALIZED TABLE — a
    regular DROP TABLE would phantom-drop them without raising (see
    ConfluentAdapter.drop_relation); DROP TABLE IF EXISTS handles the
    absent/regular/inferred cases (and deletes the table's topic).

    Note this only removes the *catalog* entry promptly: the backing Kafka
    topic is deleted asynchronously and can linger long after, and Schema
    Registry subjects are not deleted at all. Callers must not recreate the
    same relation name afterwards — the MT tests use per-session unique
    names for exactly that reason.
    """
    rows = project.run_sql(
        "select IS_MATERIALIZED from INFORMATION_SCHEMA.`TABLES` "
        f"where TABLE_SCHEMA = '{project.test_schema}' and TABLE_NAME = '{name}'",
        fetch="all",
    )
    if rows and str(rows[0][0]).upper() == "YES":
        return drop_materialized_table(project, name)
    try:
        project.run_sql(f"drop table if exists `{name}`")
        return True
    except Exception:
        return False  # give up; a later session's sweep reclaims the leftovers


def sweep_stale_test_relations(project, pattern, current_tag, min_age=SWEEP_MIN_AGE_SECONDS):
    """Reclaim relations leaked by previous test sessions.

    Test relation names carry a reserved prefix plus a hex epoch-seconds
    session tag (see test_materialized_table.py) and are never reused across
    sessions, so a failed teardown or a hard-killed run would otherwise leak
    its relations forever. This scans SHOW TABLES for names matching
    `pattern` — which must be anchored, cover only the reserved name shape,
    and expose a `tag` group — and drops every match that is not this
    session's and is older than `min_age` (younger ones may belong to a
    concurrently running session).

    Only catalog entries are visible here: a lingering topic whose entry
    hasn't resurfaced yet is untouchable by design and gets reclaimed by a
    later sweep once it resurfaces (or finishes deleting on its own).
    """
    now = time.time()
    for row in project.run_sql("show tables", fetch="all"):
        match = pattern.match(row[0])
        if not match or match.group("tag") == current_tag:
            continue
        if now - int(match.group("tag"), 16) < min_age:
            continue
        drop_any_relation(project, row[0])


def capture_submitted_statement_properties(monkeypatch):
    """Patch ConfluentConnectionManager.add_query to record every submitted
    statement's properties, keyed by its (sanitized) statement name, as the
    caller's `dbt run` executes.

    Some statements are reaped (deleted server-side) the instant they
    complete -- e.g. materialized_table's DDL, submitted under a per-run name
    and deleted by the driver as soon as the CREATE OR ALTER finishes (see
    materialized_table.sql) -- so a post-hoc get_statement() lookup after
    `dbt run` returns would always 404. Capturing properties here, at
    add_query's return (after the statement is submitted but before the
    caller's execute() wrapper closes the cursor and triggers that deletion),
    is the only way to observe them.

    Returns the dict that accumulates {statement_name: properties} as the
    run proceeds; call after `run_dbt` and filter by name substring to find
    the model(s) of interest.
    """
    from dbt.adapters.confluent.connections import ConfluentConnectionManager

    captured: dict[str, dict] = {}
    original_add_query = ConfluentConnectionManager.add_query

    def add_query_and_capture(self, sql, *args, **kwargs):
        connection, cursor = original_add_query(self, sql, *args, **kwargs)
        captured[cursor.statement.name] = dict(cursor.statement.properties)
        return connection, cursor

    monkeypatch.setattr(ConfluentConnectionManager, "add_query", add_query_and_capture)
    return captured


def sweep_stale_test_statements(
    project, prefix="dbt-adapter-test-", min_age=SWEEP_MIN_AGE_SECONDS
):
    """Delete statements leaked by previous test sessions.

    Statement labels are fresh UUIDs per test class, so a crashed run's
    statements are only discoverable by the suite-reserved
    `statement_name_prefix` every test profile sets. Deletes every statement
    under that prefix older than `min_age` (creation time from statement
    metadata; statements without one are left alone). Terminal statements
    are auto-purged by Confluent after 30 days anyway — the ones that matter
    here are leaked RUNNING statements, which hold compute-pool resources.
    """
    cutoff = datetime.now(timezone.utc).timestamp() - min_age
    with project.adapter.connection_named("sweep"):
        conn = project.adapter.connections.get_thread_connection()
        # name_contains is a server-side substring filter; anchor client-side.
        for statement in conn.handle.list_statements(name_contains=prefix):
            if not statement.name.startswith(prefix):
                continue
            created_at = statement.metadata.get("created_at")
            if not created_at:
                continue
            if datetime.fromisoformat(created_at).timestamp() >= cutoff:
                continue
            # The adapter helper swallows missing-statement / pool-scoped 403s.
            project.adapter.delete_statement(statement.name)
