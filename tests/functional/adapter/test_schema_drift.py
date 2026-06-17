"""Tests for schema drift detection.

When a table already exists and --full-refresh is not set, the adapter should:
- Skip re-creation if the table matches the model definition
- Raise an error if there's column drift (name changes, additions, removals)
- Raise an error if there's WITH options drift
- Raise an error if there's distributed_by drift (added, removed, columns or
  bucket count changed)
- Allow skipping drift detection with on_schema_drift='ignore'

Scope note: the drift *detection logic* (columns added/removed/renamed/type,
WITH options, distribution columns/buckets, partitioning, collect-and-raise)
is exhaustively unit-tested in tests/unit/test_check_schema_drift.py, which
runs in milliseconds against hand-built catalogs. The functional tests here
exist only to prove the end-to-end wiring against a live Confluent backend:
that each materialization fires the drift check, that get_drift_catalog reads
real INFORMATION_SCHEMA output, and that the skip / error / full-refresh /
ignore behaviors hold. We therefore keep ONE representative drift case per
materialization rather than re-verifying every drift kind through a (slow)
live run — the kind-by-kind discrimination is the unit tests' job.
"""

import pytest

from dbt.tests.util import run_dbt, set_model_file
from tests.functional.adapter._helpers import (
    assert_distribution_drift_error,
    assert_drift_error,
    get_result_by_name,
    relation,
)
from tests.functional.adapter.fixtures import ClassScopedCleanup, ConfluentFixtures

# -- Shared faker source feeding the table / streaming_table models --
#
# Intentionally UNBOUNDED (no `number-of-rows`). The streaming_table that reads
# from this source is recoverable: on a re-run, decide_action classifies its
# long-running INSERT and restarts it if the statement is missing or terminal.
# A bounded faker source makes that INSERT *complete* (reach a terminal phase),
# which would turn a no-change second run into a restart and break
# TestSchemaDriftDetection.test_second_run_skips. Keeping the source unbounded
# keeps the INSERT RUNNING/healthy across re-runs, matching production, so the
# second run skips. The autouse ConfluentFixtures.clean_up deletes every
# statement by label after each test, so an unbounded source leaks no compute.
SOURCE_FOR_DRIFT = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
    }
) }}
`order_id` BIGINT,
`price` DECIMAL(10, 2),
`order_time` TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,
PRIMARY KEY(`order_id`) NOT ENFORCED
"""

# -- Table (CTAS) models --

TABLE_MODEL = """
{{ config(materialized='table') }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

TABLE_MODEL_EXTRA_COLUMN = """
{{ config(materialized='table') }}
select order_id, price, order_time, order_id as duplicate_col from {{ ref('source_for_drift') }}
"""

# -- Streaming table models --

STREAMING_TABLE_MODEL = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

STREAMING_TABLE_MODEL_EXTRA_COLUMN = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
) }}
select order_id, price, order_time, order_id as duplicate_col from {{ ref('source_for_drift') }}
"""

STREAMING_TABLE_MODELS_YML = """
models:
  - name: my_streaming_table
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
          - type: primary_key
            expression: "not enforced"
      - name: price
        data_type: decimal(10,2)
      - name: order_time
        data_type: timestamp(3)
"""

# -- Streaming source models --

STREAMING_SOURCE_MODEL = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
    }
) }}
`id` BIGINT,
`value` STRING
"""

STREAMING_SOURCE_MODEL_EXTRA_COLUMN = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
    }
) }}
`id` BIGINT,
`value` STRING,
`extra_column` STRING
"""


# ---------------------------------------------------------------------------
# Drift detection wiring — one shared setup across all three materializations
# ---------------------------------------------------------------------------


class TestSchemaDriftDetection(ConfluentFixtures):
    """End-to-end drift wiring for table, streaming_table and streaming_source.

    All three materializations are created once in a single class-scoped
    full-refresh, then each drift-error test mutates one model file and runs
    again. A drift error does not recreate the table, so the backend stays at
    its baseline and the tests are independent of one another (each also resets
    its model file in a finally for clean failure output).

    Per-kind discrimination (renamed/removed/type/options) is covered by the
    unit tests; here a single added-column drift per materialization proves the
    check is wired and reaches check_schema_drift through the live catalog query.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "models": {"+schema": unique_schema},
            "seeds": {"+schema": unique_schema},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_for_drift.sql": SOURCE_FOR_DRIFT,
            "my_table.sql": TABLE_MODEL,
            "my_streaming_table.sql": STREAMING_TABLE_MODEL,
            "my_source.sql": STREAMING_SOURCE_MODEL,
            "models.yml": STREAMING_TABLE_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists my_table")
        project.run_sql("drop table if exists my_streaming_table")
        project.run_sql("drop table if exists my_source")

    def test_second_run_skips(self, project):
        """A second run with no changes must skip every model, not drift.

        This is the shared no-drift path for all three materializations.
        """
        results = run_dbt(["run"])
        assert len(results) == 4
        for r in results:
            assert r.message == "SKIP", f"{r.node.name} was not skipped (message: {r.message})"

    def test_column_drift_detected(self, project):
        """Every materialization drifted at once, asserted in a single run.

        A `dbt run` drift-checks *every* model in the project (each pays a
        temp-table create + catalog query), so the per-run cost is paid whether
        one model drifts or all three. We therefore mutate all three and assert
        each raises, rather than spending three separate runs for no extra
        coverage — the per-kind detection logic is unit-tested.
        """
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_EXTRA_COLUMN)
        set_model_file(
            project, relation(project, "my_streaming_table"), STREAMING_TABLE_MODEL_EXTRA_COLUMN
        )
        set_model_file(
            project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_EXTRA_COLUMN
        )
        try:
            result = run_dbt(["run"], expect_pass=False)
            assert_drift_error(result, "my_table")
            assert_drift_error(result, "my_streaming_table")
            assert_drift_error(result, "my_source")
        finally:
            set_model_file(project, relation(project, "my_table"), TABLE_MODEL)
            set_model_file(project, relation(project, "my_streaming_table"), STREAMING_TABLE_MODEL)
            set_model_file(project, relation(project, "my_source"), STREAMING_SOURCE_MODEL)


class TestTableFullRefreshFixesDrift(ConfluentFixtures):
    """After detecting drift, --full-refresh should succeed.

    Separated because --full-refresh recreates the table with a different
    schema, which would invalidate the shared-setup drift tests above.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "models": {"+schema": unique_schema},
            "seeds": {"+schema": unique_schema},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_for_drift.sql": SOURCE_FOR_DRIFT,
            "my_table.sql": TABLE_MODEL,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists my_table")

    def test_full_refresh_fixes_drift(self, project):
        """After detecting drift, --full-refresh should succeed."""
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_EXTRA_COLUMN)
        result = run_dbt(["run", "--full-refresh"])
        assert len(result) == 2
        for r in result:
            assert r.status.name == "Success", f"{r.node.name} failed: {r.status}"


# ---------------------------------------------------------------------------
# on_schema_drift='ignore' tests
# ---------------------------------------------------------------------------

TABLE_MODEL_IGNORE_DRIFT = """
{{ config(
    materialized='table',
    on_schema_drift='ignore'
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

TABLE_MODEL_IGNORE_DRIFT_CHANGED = """
{{ config(
    materialized='table',
    on_schema_drift='ignore'
) }}
select order_id, price from {{ ref('source_for_drift') }}
"""


class TestIgnoreSchemaDrift(ConfluentFixtures):
    """When on_schema_drift='ignore', drift should not be detected or cause errors.

    Tested on the cheap CTAS table only: on_schema_drift='ignore' short-circuits
    decide_action *before* any drift check runs (helpers.sql), so the
    behavior is identical for every materialization and the drift kind (columns
    vs options) is irrelevant — the check never fires. No need to pay for a
    live streaming_table to re-prove the same branch.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "models": {"+schema": unique_schema},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_for_drift.sql": SOURCE_FOR_DRIFT,
            "my_table.sql": TABLE_MODEL_IGNORE_DRIFT,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists my_table")

    def test_table_with_column_drift_ignored(self, project):
        """With on_schema_drift='ignore', column drift should not cause an error."""
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_IGNORE_DRIFT_CHANGED)
        result = run_dbt(["run"])
        # All models should succeed (skip)
        assert len(result) == 2  # source + my_table
        for r in result:
            assert r.status.name == "Success"
        # my_table should have been skipped
        my_table_result = get_result_by_name(result, "my_table")
        assert my_table_result.message == "SKIP"


# -- streaming_table restart under on_schema_drift='ignore' --

STREAMING_TABLE_MODEL_IGNORE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
    on_schema_drift='ignore',
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

STREAMING_TABLE_MODEL_IGNORE_COLUMN_DROPPED = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
    on_schema_drift='ignore',
) }}
select order_id, price from {{ ref('source_for_drift') }}
"""


class TestStreamingRestartUnderIgnore(ClassScopedCleanup):
    """on_schema_drift='ignore' suppresses *benign* drift, not load-bearing
    column drift on the restart path.

    The skip path (healthy statement) short-circuits before any drift check, so
    'ignore' is total there — that's covered by TestIgnoreSchemaDrift on the
    cheap CTAS table. But streaming_table is recoverable: when its INSERT is
    missing or terminal, the materialization re-submits the INSERT against the
    *existing* table. If the model's columns have actually drifted, that INSERT
    would fail at Flink with a cryptic "Different number of columns" error. So
    the restart path runs a columns-only drift check even under 'ignore' and
    surfaces a clear dbt drift error instead.
    """

    TABLES = ["source_for_drift", "my_streaming_table"]

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "models": {"+schema": unique_schema},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_for_drift.sql": SOURCE_FOR_DRIFT,
            "my_streaming_table.sql": STREAMING_TABLE_MODEL_IGNORE,
            "models.yml": STREAMING_TABLE_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def first_run(self, project):
        # Build the table and its live INSERT once; class_clean_up tears down.
        run_dbt(["run", "--full-refresh"])
        yield

    def test_restart_under_ignore_raises_on_column_drift(self, project, dbt_profile_data):
        """Regression: missing INSERT + drifted columns under 'ignore' must
        surface a drift error, not a cryptic Flink failure on resubmit."""
        # Force the missing-statement state deterministically so decide_action
        # takes the restart branch on the next run.
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        with project.adapter.connection_named("force_missing"):
            conn = project.adapter.connections.get_thread_connection()
            for stmt in conn.handle.list_statements(label=label):
                project.adapter.delete_statement(stmt.name)

        set_model_file(
            project,
            relation(project, "my_streaming_table"),
            STREAMING_TABLE_MODEL_IGNORE_COLUMN_DROPPED,
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_streaming_table")


class TestInvalidOnSchemaChange(ConfluentFixtures):
    """Invalid on_schema_drift values should raise a clear error."""

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "models": {"+schema": unique_schema},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_for_drift.sql": SOURCE_FOR_DRIFT,
            "my_table.sql": """
{{ config(
    materialized='table',
    on_schema_drift='append_new_columns'
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
""",
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists my_table")

    def test_invalid_on_schema_drift_value(self, project):
        """Invalid on_schema_drift value should raise an error."""
        result = run_dbt(["run"], expect_pass=False)
        my_table_result = get_result_by_name(result, "my_table")
        assert my_table_result is not None
        assert my_table_result.status.name == "Error"
        assert "Invalid value for on_schema_drift" in my_table_result.message
        assert "append_new_columns" in my_table_result.message
        assert "Expected 'ignore' or 'fail'" in my_table_result.message


# ---------------------------------------------------------------------------
# distributed_by drift — one shared setup across all three materializations
# ---------------------------------------------------------------------------

# -- table (CTAS) --
DIST_CTAS_BASELINE = """
{{ config(
    materialized='table',
    distributed_by={'columns': ['order_id'], 'buckets': 4},
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

DIST_CTAS_DIFFERENT_COLUMN = """
{{ config(
    materialized='table',
    distributed_by={'columns': ['price'], 'buckets': 4},
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

# -- streaming_source --
DIST_SOURCE_BASELINE = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
        'changelog.mode': 'append',
    },
    distributed_by={'columns': ['order_id'], 'buckets': 3},
) }}
order_id BIGINT NOT NULL,
price DECIMAL(10, 2),
order_time TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
"""

DIST_SOURCE_DIFFERENT_COLUMN = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
        'changelog.mode': 'append',
    },
    distributed_by={'columns': ['price'], 'buckets': 3},
) }}
order_id BIGINT NOT NULL,
price DECIMAL(10, 2),
order_time TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
"""


class TestDistributedBySchemaDrift(ConfluentFixtures):
    """End-to-end `distributed_by` drift wiring on the table (CTAS) and
    streaming_source materializations.

    One class-scoped full-refresh creates a distributed baseline for each; the
    drift test then mutates both distribution column lists and asserts the
    check fires. streaming_table is intentionally omitted here: its drift path
    is identical to the CTAS table's (both are `has_select_query=true`), and a
    live streaming_table costs ~2 min of compute-pool provisioning per setup —
    its end-to-end drift wiring is proven once in TestSchemaDriftDetection.

    A passing test_second_run_skips implicitly proves the DISTRIBUTED BY clause
    rendered correctly — otherwise the unchanged second run would itself drift.
    The column-vs-bucket discrimination (and the detection logic generally) is
    covered by the unit tests, so one changed-column case is enough here.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "models": {"+schema": unique_schema},
            "seeds": {"+schema": unique_schema},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_for_drift.sql": SOURCE_FOR_DRIFT,
            "dist_drift_ctas.sql": DIST_CTAS_BASELINE,
            "dist_drift_source.sql": DIST_SOURCE_BASELINE,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists dist_drift_ctas")
        project.run_sql("drop table if exists dist_drift_source")

    def test_second_run_skips(self, project):
        """Re-running with the same distribution must skip cleanly for both
        materializations — also proves both DISTRIBUTED BY clauses rendered."""
        results = run_dbt(["run"])
        assert len(results) == 3
        for r in results:
            assert r.message == "SKIP", f"{r.node.name} was not skipped (message: {r.message})"

    def test_distribution_drift_detected(self, project):
        """Both materializations' distribution drifted at once, in a single run
        (see TestSchemaDriftDetection.test_column_drift_detected for why one run
        rather than two)."""
        set_model_file(project, relation(project, "dist_drift_ctas"), DIST_CTAS_DIFFERENT_COLUMN)
        set_model_file(
            project, relation(project, "dist_drift_source"), DIST_SOURCE_DIFFERENT_COLUMN
        )
        try:
            result = run_dbt(["run"], expect_pass=False)
            assert_distribution_drift_error(result, "dist_drift_ctas")
            assert_distribution_drift_error(result, "dist_drift_source")
        finally:
            set_model_file(project, relation(project, "dist_drift_ctas"), DIST_CTAS_BASELINE)
            set_model_file(project, relation(project, "dist_drift_source"), DIST_SOURCE_BASELINE)


# -- Intentional gap: auto-assigned distribution does not trigger drift --

NO_DIST_BUT_PK_MODEL = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

NO_DIST_BUT_PK_MODELS_YML = """
models:
  - name: no_dist_but_pk_table
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
          - type: primary_key
            expression: "not enforced"
      - name: price
        data_type: decimal(10,2)
      - name: order_time
        data_type: timestamp(3)
"""


class TestDistributedByDefaultIsNotChecked(ConfluentFixtures):
    """Locks in the documented intentional gap: when the user does NOT set
    `distributed_by`, the adapter must NOT compare against the distribution
    Confluent auto-assigned (typically derived from the primary key). Otherwise
    every existing PK-bearing model would falsely fire drift on every re-run.

    Setup creates a streaming_table with a PK (so Confluent auto-distributes
    by `order_id`) but no `distributed_by` config. A second run must skip.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "models": {"+schema": unique_schema},
            "seeds": {"+schema": unique_schema},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_for_drift.sql": SOURCE_FOR_DRIFT,
            "no_dist_but_pk_table.sql": NO_DIST_BUT_PK_MODEL,
            "models.yml": NO_DIST_BUT_PK_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists no_dist_but_pk_table")

    def test_auto_assigned_distribution_does_not_drift(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        for r in results:
            assert r.message == "SKIP", (
                f"{r.node.name} unexpectedly fired drift on auto-assigned distribution: "
                f"{r.message}"
            )
