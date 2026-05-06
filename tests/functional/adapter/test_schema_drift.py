"""Tests for schema drift detection.

When a table already exists and --full-refresh is not set, the adapter should:
- Skip re-creation if the table matches the model definition
- Raise an error if there's column drift (name changes, additions, removals)
- Raise an error if there's WITH options drift
- Raise an error if there's distributed_by drift (added, removed, columns or
  bucket count changed)
- Allow skipping drift detection with on_schema_drift='ignore'
"""

import pytest

from dbt.tests.util import run_dbt, set_model_file
from tests.functional.adapter._helpers import (
    assert_distribution_drift_error,
    assert_drift_error,
    get_result_by_name,
    relation,
)
from tests.functional.adapter.fixtures import ConfluentFixtures

# -- Table (CTAS) models --

TABLE_MODEL = """
{{ config(materialized='table') }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

TABLE_MODEL_EXTRA_COLUMN = """
{{ config(materialized='table') }}
select order_id, price, order_time, order_id as duplicate_col from {{ ref('source_for_drift') }}
"""

TABLE_MODEL_REMOVED_COLUMN = """
{{ config(materialized='table') }}
select order_id, price from {{ ref('source_for_drift') }}
"""

TABLE_MODEL_RENAMED_COLUMN = """
{{ config(materialized='table') }}
select order_id as id, price, order_time from {{ ref('source_for_drift') }}
"""

TABLE_MODEL_REORDERED_COLUMNS = """
{{ config(materialized='table') }}
select price, order_id, order_time from {{ ref('source_for_drift') }}
"""

TABLE_MODEL_TYPE_CHANGE = """
{{ config(materialized='table') }}
select
  cast(order_id as int) as order_id,
  cast(price as decimal(10, 3)) as price,
  order_time
from {{ ref('source_for_drift') }}
"""

# -- Streaming table models --

STREAMING_TABLE_MODEL = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

STREAMING_TABLE_MODEL_DIFFERENT_OPTIONS = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
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

STREAMING_TABLE_MODEL_TYPE_CHANGE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
) }}
select
  cast(order_id as int) as order_id,
  price,
  order_time
from {{ ref('source_for_drift') }}
"""

STREAMING_TABLE_MODEL_REMOVED_COLUMN = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
) }}
select order_id, price from {{ ref('source_for_drift') }}
"""

STREAMING_TABLE_MODEL_RENAMED_COLUMN = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
) }}
select order_id as id, price, order_time from {{ ref('source_for_drift') }}
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

SOURCE_FOR_DRIFT = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
    }
) }}
`order_id` BIGINT,
`price` DECIMAL(10, 2),
`order_time` TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,
PRIMARY KEY(`order_id`) NOT ENFORCED
"""

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

STREAMING_SOURCE_MODEL_DIFFERENT_OPTIONS = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '10',
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

STREAMING_SOURCE_MODEL_REMOVED_COLUMN = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
    }
) }}
`id` BIGINT
"""

STREAMING_SOURCE_MODEL_RENAMED_COLUMN = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
    }
) }}
`id` BIGINT,
`name` STRING
"""

STREAMING_SOURCE_MODEL_TYPE_CHANGE = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
    }
) }}
`id` INT,
`value` STRING
"""


# ---------------------------------------------------------------------------
# Table (CTAS) drift tests
# ---------------------------------------------------------------------------


class TestTableSchemaDrift(ConfluentFixtures):
    """Table schema drift detection tests.

    Creates source_for_drift + my_table once, then runs drift checks against
    the unchanged table.  Drift-error tests don't modify the table, so they
    can all share the same class-scoped setup.
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

    def test_second_run_skips(self, project):
        """Second run without --full-refresh should skip, not fail."""
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL)
        results = run_dbt(["run"])
        assert len(results) == 2
        for r in results:
            assert r.message == "SKIP", f"{r.node.name} was not skipped (message: {r.message})"

    def test_extra_column_detected(self, project):
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_EXTRA_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_table")

    def test_removed_column_detected(self, project):
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_REMOVED_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_table")

    def test_renamed_column_detected(self, project):
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_RENAMED_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_table")

    def test_reordered_columns_not_detected(self, project):
        """Column reordering is not considered drift — order doesn't matter for Kafka tables."""
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_REORDERED_COLUMNS)
        result = run_dbt(["run"])
        assert len(result) == 2
        for r in result:
            assert r.message == "SKIP", f"{r.node.name} was not skipped (message: {r.message})"

    def test_type_change_detected(self, project):
        """Changing column data types should raise an error without --full-refresh."""
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_TYPE_CHANGE)
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_table")


class TestTableFullRefreshFixesDrift(ConfluentFixtures):
    """After detecting drift, --full-refresh should succeed.

    Separated because --full-refresh recreates the table with a different
    schema, which would invalidate other drift tests sharing the same setup.
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
# Streaming table drift tests
# ---------------------------------------------------------------------------


class TestStreamingTableSchemaDrift(ConfluentFixtures):
    """Streaming table schema and options drift detection tests.

    Creates source_for_drift + my_streaming_table once, then runs drift
    checks.  Drift-error tests don't modify the table.
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
            "my_streaming_table.sql": STREAMING_TABLE_MODEL,
            "models.yml": STREAMING_TABLE_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists my_streaming_table")

    def test_second_run_skips(self, project):
        set_model_file(project, relation(project, "my_streaming_table"), STREAMING_TABLE_MODEL)
        results = run_dbt(["run"])
        assert len(results) == 2
        for r in results:
            assert r.message == "SKIP", f"{r.node.name} was not skipped (message: {r.message})"

    def test_changed_options_detected(self, project):
        set_model_file(
            project,
            relation(project, "my_streaming_table"),
            STREAMING_TABLE_MODEL_DIFFERENT_OPTIONS,
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_streaming_table")

    def test_column_drift_detected(self, project):
        set_model_file(
            project, relation(project, "my_streaming_table"), STREAMING_TABLE_MODEL_EXTRA_COLUMN
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_streaming_table")

    def test_type_change_detected(self, project):
        """Changing column data types should raise an error without --full-refresh."""
        set_model_file(
            project, relation(project, "my_streaming_table"), STREAMING_TABLE_MODEL_TYPE_CHANGE
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_streaming_table")

    def test_removed_and_renamed_columns_detected(self, project):
        """Removing or renaming a column both raise drift errors. Bundled
        because each variant takes the same path through the drift check
        and a separate test would double the (slow) class fixture cost."""
        set_model_file(
            project,
            relation(project, "my_streaming_table"),
            STREAMING_TABLE_MODEL_REMOVED_COLUMN,
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_streaming_table")

        set_model_file(
            project,
            relation(project, "my_streaming_table"),
            STREAMING_TABLE_MODEL_RENAMED_COLUMN,
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_streaming_table")


# ---------------------------------------------------------------------------
# Streaming source drift tests
# ---------------------------------------------------------------------------


class TestStreamingSourceSchemaDrift(ConfluentFixtures):
    """Streaming source schema and options drift detection tests.

    Creates my_source once, then runs drift checks.  Drift-error tests
    don't modify the table.
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
            "my_source.sql": STREAMING_SOURCE_MODEL,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists my_source")

    def test_second_run_skips(self, project):
        set_model_file(project, relation(project, "my_source"), STREAMING_SOURCE_MODEL)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].message == "SKIP", (
            f"{results[0].node.name} was not skipped (message: {results[0].message})"
        )

    def test_changed_options_detected(self, project):
        set_model_file(
            project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_DIFFERENT_OPTIONS
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_source")

    def test_extra_column_detected(self, project):
        """Adding a column should raise an error without --full-refresh."""
        set_model_file(
            project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_EXTRA_COLUMN
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_source")

    def test_removed_column_detected(self, project):
        """Removing a column should raise an error without --full-refresh."""
        set_model_file(
            project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_REMOVED_COLUMN
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_source")

    def test_renamed_column_detected(self, project):
        """Renaming a column should raise an error without --full-refresh."""
        set_model_file(
            project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_RENAMED_COLUMN
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_source")

    def test_type_change_detected(self, project):
        """Changing column data types should raise an error without --full-refresh."""
        set_model_file(project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_TYPE_CHANGE)
        result = run_dbt(["run"], expect_pass=False)
        assert_drift_error(result, "my_source")


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

STREAMING_TABLE_MODEL_IGNORE_DRIFT = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
    on_schema_drift='ignore'
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

STREAMING_TABLE_MODEL_IGNORE_DRIFT_CHANGED = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
    on_schema_drift='ignore'
) }}
select order_id, price from {{ ref('source_for_drift') }}
"""


class TestIgnoreSchemaDrift(ConfluentFixtures):
    """When on_schema_drift='ignore', drift should not be detected or cause errors."""

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
            "my_streaming_table.sql": STREAMING_TABLE_MODEL_IGNORE_DRIFT,
            "models.yml": STREAMING_TABLE_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists my_table")
        project.run_sql("drop table if exists my_streaming_table")

    def test_table_with_column_drift_ignored(self, project):
        """With on_schema_drift='ignore', column drift should not cause an error."""
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_IGNORE_DRIFT_CHANGED)
        result = run_dbt(["run"])
        # All models should succeed (skip)
        assert len(result) == 3  # source + my_table + my_streaming_table
        for r in result:
            assert r.status.name == "Success"
        # my_table should have been skipped
        my_table_result = get_result_by_name(result, "my_table")
        assert my_table_result.message == "SKIP"

    def test_streaming_table_with_options_drift_ignored(self, project):
        """With on_schema_drift='ignore', WITH options drift should not cause an error."""
        set_model_file(
            project,
            relation(project, "my_streaming_table"),
            STREAMING_TABLE_MODEL_IGNORE_DRIFT_CHANGED,
        )
        result = run_dbt(["run"])
        # All models should succeed (skip)
        assert len(result) == 3
        for r in result:
            assert r.status.name == "Success"
        # my_streaming_table should have been skipped
        streaming_result = get_result_by_name(result, "my_streaming_table")
        assert streaming_result.message == "SKIP"


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
# distributed_by drift tests
# ---------------------------------------------------------------------------

DIST_TABLE_BASELINE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
    distributed_by={'columns': ['order_id'], 'buckets': 4},
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

DIST_TABLE_DIFFERENT_COLUMN = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
    distributed_by={'columns': ['price'], 'buckets': 4},
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

DIST_TABLE_DIFFERENT_BUCKETS = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
    distributed_by={'columns': ['order_id'], 'buckets': 8},
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""

DIST_TABLE_MODELS_YML = """
models:
  - name: dist_drift_table
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


class TestDistributedBySchemaDrift(ConfluentFixtures):
    """Drift tests for the `distributed_by` config.

    Creates source_for_drift + dist_drift_table once with a baseline
    distribution, then runs drift checks against the unchanged table.
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
            "dist_drift_table.sql": DIST_TABLE_BASELINE,
            "models.yml": DIST_TABLE_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists dist_drift_table")

    def test_second_run_skips(self, project):
        """Re-running with the same distribution should skip cleanly."""
        set_model_file(project, relation(project, "dist_drift_table"), DIST_TABLE_BASELINE)
        results = run_dbt(["run"])
        assert len(results) == 2
        for r in results:
            assert r.message == "SKIP", f"{r.node.name} was not skipped (message: {r.message})"

    def test_changed_columns_detected(self, project):
        """Changing the distribution column list should raise a drift error."""
        set_model_file(project, relation(project, "dist_drift_table"), DIST_TABLE_DIFFERENT_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_distribution_drift_error(result, "dist_drift_table")

    def test_changed_buckets_detected(self, project):
        """Changing the bucket count should raise a drift error."""
        set_model_file(
            project, relation(project, "dist_drift_table"), DIST_TABLE_DIFFERENT_BUCKETS
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_distribution_drift_error(result, "dist_drift_table")


# -- CTAS table drift --

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

DIST_CTAS_DIFFERENT_BUCKETS = """
{{ config(
    materialized='table',
    distributed_by={'columns': ['order_id'], 'buckets': 8},
) }}
select order_id, price, order_time from {{ ref('source_for_drift') }}
"""


class TestDistributedByOnTableDrift(ConfluentFixtures):
    """Drift coverage for `distributed_by` on the `table` (CTAS) materialization.

    The class fixture creates source_for_drift + dist_drift_ctas with the
    baseline distribution; tests then mutate the model file and assert that
    the drift check fires. A successful baseline (test_second_run_skips)
    implicitly proves the DISTRIBUTED BY clause was rendered correctly —
    otherwise the second run would fire drift.
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
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists dist_drift_ctas")

    def test_second_run_skips(self, project):
        """Re-running with the same distribution should skip cleanly."""
        set_model_file(project, relation(project, "dist_drift_ctas"), DIST_CTAS_BASELINE)
        results = run_dbt(["run"])
        assert len(results) == 2
        for r in results:
            assert r.message == "SKIP", f"{r.node.name} was not skipped (message: {r.message})"

    def test_changed_columns_detected(self, project):
        set_model_file(project, relation(project, "dist_drift_ctas"), DIST_CTAS_DIFFERENT_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_distribution_drift_error(result, "dist_drift_ctas")

    def test_changed_buckets_detected(self, project):
        set_model_file(project, relation(project, "dist_drift_ctas"), DIST_CTAS_DIFFERENT_BUCKETS)
        result = run_dbt(["run"], expect_pass=False)
        assert_distribution_drift_error(result, "dist_drift_ctas")


# -- Streaming source drift --

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

DIST_SOURCE_DIFFERENT_BUCKETS = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
        'changelog.mode': 'append',
    },
    distributed_by={'columns': ['order_id'], 'buckets': 5},
) }}
order_id BIGINT NOT NULL,
price DECIMAL(10, 2),
order_time TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
"""


class TestDistributedByOnStreamingSourceDrift(ConfluentFixtures):
    """Drift coverage for `distributed_by` on the `streaming_source`
    materialization. See TestDistributedByOnTableDrift for rationale."""

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "models": {"+schema": unique_schema},
            "seeds": {"+schema": unique_schema},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dist_drift_source.sql": DIST_SOURCE_BASELINE,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists dist_drift_source")

    def test_second_run_skips(self, project):
        """Re-running with the same distribution should skip cleanly."""
        set_model_file(project, relation(project, "dist_drift_source"), DIST_SOURCE_BASELINE)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].message == "SKIP", (
            f"{results[0].node.name} was not skipped (message: {results[0].message})"
        )

    def test_changed_columns_detected(self, project):
        set_model_file(
            project, relation(project, "dist_drift_source"), DIST_SOURCE_DIFFERENT_COLUMN
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_distribution_drift_error(result, "dist_drift_source")

    def test_changed_buckets_detected(self, project):
        set_model_file(
            project, relation(project, "dist_drift_source"), DIST_SOURCE_DIFFERENT_BUCKETS
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_distribution_drift_error(result, "dist_drift_source")


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
