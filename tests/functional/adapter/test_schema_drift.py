"""Tests for schema drift detection.

When a table already exists and --full-refresh is not set, the adapter should:
- Skip re-creation if the table matches the model definition
- Raise an error if there's column drift (name changes, additions, removals)
- Raise an error if there's WITH options drift
- Allow skipping drift detection with on_schema_drift='ignore'
"""

import pytest

from dbt.tests.util import run_dbt, set_model_file
from tests.functional.adapter.fixtures import ConfluentFixtures


def relation(project, name):
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
