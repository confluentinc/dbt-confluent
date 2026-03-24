"""Tests for schema drift detection.

When a table already exists and --full-refresh is not set, the adapter should:
- Skip re-creation if the table matches the model definition
- Raise an error if there's column drift (name changes, additions, removals)
- Raise an error if there's WITH options drift
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


def assert_result_has_status(results, name, expected_status):
    """Assert that a specific result has the expected status."""
    result = get_result_by_name(results, name)
    assert result is not None, f"{name} not found in results"
    assert result.status == expected_status, f"{name} expected status '{expected_status}' but got '{result.status}'"

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


class TestTableIdempotent(ConfluentFixtures):
    """A table model should be idempotent: two consecutive runs without --full-refresh should succeed."""

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
        # First run with --full-refresh to create everything
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists my_table")

    def test_second_run_skips(self, project):
        """Second run without --full-refresh should skip, not fail."""
        results = run_dbt(["run"])
        assert len(results) == 2
        # Verify both models were skipped
        for r in results:
            assert r.message == "SKIP", f"{r.node.name} was not skipped (message: {r.message})"


class TestTableColumnDrift(ConfluentFixtures):
    """Changing columns in a table model should raise an error without --full-refresh."""

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

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, project):
        # Reset model file before each test in case a previous test modified it
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL)
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists my_table")

    def test_extra_column_detected(self, project):
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_EXTRA_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_table", "error")

    def test_removed_column_detected(self, project):
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_REMOVED_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_table", "error")

    def test_renamed_column_detected(self, project):
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_RENAMED_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_table", "error")

    def test_reordered_columns_not_detected(self, project):
        """Column reordering is not considered drift — order doesn't matter for Kafka tables."""
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_REORDERED_COLUMNS)
        result = run_dbt(["run"])
        assert len(result) == 2
        # Verify both models were skipped (no drift detected)
        for r in result:
            assert r.message == "SKIP", f"{r.node.name} was not skipped (message: {r.message})"

    def test_full_refresh_fixes_drift(self, project):
        """After detecting drift, --full-refresh should succeed."""
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_EXTRA_COLUMN)
        result = run_dbt(["run", "--full-refresh"])
        assert len(result) == 2
        # Verify both models succeeded
        for r in result:
            assert r.status.name == "Success", f"{r.node.name} failed: {r.status}"

    def test_type_change_detected(self, project):
        """Changing column data types should raise an error without --full-refresh."""
        set_model_file(project, relation(project, "my_table"), TABLE_MODEL_TYPE_CHANGE)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_table", "error")


class TestStreamingTableIdempotent(ConfluentFixtures):
    """A streaming_table model should be idempotent."""

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
        results = run_dbt(["run"])
        assert len(results) == 2
        # Verify both models were skipped
        for r in results:
            assert r.message == "SKIP", f"{r.node.name} was not skipped (message: {r.message})"


class TestStreamingTableOptionsDrift(ConfluentFixtures):
    """Changing WITH options in a streaming_table should raise an error."""

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

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, project):
        # Reset model file before each test in case a previous test modified it
        set_model_file(project, relation(project, "my_streaming_table"), STREAMING_TABLE_MODEL)
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists source_for_drift")
        project.run_sql("drop table if exists my_streaming_table")

    def test_changed_options_detected(self, project):
        set_model_file(project, relation(project, "my_streaming_table"), STREAMING_TABLE_MODEL_DIFFERENT_OPTIONS)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_streaming_table", "error")

    def test_column_drift_detected(self, project):
        set_model_file(project, relation(project, "my_streaming_table"), STREAMING_TABLE_MODEL_EXTRA_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_streaming_table", "error")

    def test_type_change_detected(self, project):
        """Changing column data types should raise an error without --full-refresh."""
        set_model_file(project, relation(project, "my_streaming_table"), STREAMING_TABLE_MODEL_TYPE_CHANGE)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_streaming_table", "error")


class TestStreamingSourceIdempotent(ConfluentFixtures):
    """A streaming_source model should be idempotent."""

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
        results = run_dbt(["run"])
        assert len(results) == 1
        # Verify the model was skipped
        assert results[0].message == "SKIP", f"{results[0].node.name} was not skipped (message: {results[0].message})"


class TestStreamingSourceOptionsDrift(ConfluentFixtures):
    """Changing WITH options or columns in a streaming_source should raise an error."""

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

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, project):
        run_dbt(["run", "--full-refresh"])
        yield
        project.run_sql("drop table if exists my_source")

    def test_changed_options_detected(self, project):
        set_model_file(project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_DIFFERENT_OPTIONS)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_source", "error")

    def test_extra_column_detected(self, project):
        """Adding a column should raise an error without --full-refresh."""
        set_model_file(project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_EXTRA_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_source", "error")

    def test_removed_column_detected(self, project):
        """Removing a column should raise an error without --full-refresh."""
        set_model_file(project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_REMOVED_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_source", "error")

    def test_renamed_column_detected(self, project):
        """Renaming a column should raise an error without --full-refresh."""
        set_model_file(project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_RENAMED_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_source", "error")

    def test_type_change_detected(self, project):
        """Changing column data types should raise an error without --full-refresh."""
        set_model_file(project, relation(project, "my_source"), STREAMING_SOURCE_MODEL_TYPE_CHANGE)
        result = run_dbt(["run"], expect_pass=False)
        assert_result_has_status(result, "my_source", "error")
