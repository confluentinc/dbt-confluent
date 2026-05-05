"""Tests for specific regressions and bug fixes."""

import pytest

from dbt.tests.util import run_dbt, set_model_file
from tests.functional.adapter.fixtures import ConfluentFixtures
from tests.functional.adapter.test_schema_drift import (
    assert_distribution_drift_error,
    get_result_by_name,
    relation,
)

# Test for issue with window functions producing NOT NULL constraints
# in FULL_DATA_TYPE which break CAST expressions in get_empty_schema_sql

SOURCE_FOR_WINDOW_TEST = """
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

STREAMING_TABLE_WITH_WINDOW_FUNCTIONS = """
{{ config(
    materialized='streaming_table',
    contract={'enforced': true}
) }}
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY window_start, window_end
                          ORDER BY total_price DESC) AS rownum
    FROM (
        SELECT window_start,
               window_end,
               order_id,
               SUM(price) AS total_price,
               COUNT(*) AS cnt
        FROM TABLE(TUMBLE(TABLE {{ ref('source_for_window_test') }},
                         DESCRIPTOR(order_time),
                         INTERVAL '10' MINUTES))
        GROUP BY window_start, window_end, order_id
    )
)
WHERE rownum <= 5
"""

WINDOW_TABLE_MODELS_YML = """
models:
  - name: window_table
    columns:
      - name: window_start
        data_type: TIMESTAMP(3) NOT NULL
      - name: window_end
        data_type: TIMESTAMP(3) NOT NULL
      - name: order_id
        data_type: BIGINT NOT NULL
      - name: total_price
        data_type: DECIMAL(38,2) NOT NULL
      - name: cnt
        data_type: BIGINT NOT NULL
      - name: rownum
        data_type: BIGINT NOT NULL
"""


class TestWindowFunctionConstraintsInDataType(ConfluentFixtures):
    """Test validation of malformed YAML with constraints in data_type field.

    Users might mistakenly put constraint keywords (NOT NULL, VIRTUAL, etc.)
    directly in the data_type field instead of using the constraints section.
    This test ensures we catch this early and provide a clear, actionable error.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "models": {"+schema": unique_schema},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_for_window_test.sql": SOURCE_FOR_WINDOW_TEST,
            "window_table.sql": STREAMING_TABLE_WITH_WINDOW_FUNCTIONS,
            "models.yml": WINDOW_TABLE_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        yield
        project.run_sql("drop table if exists source_for_window_test")
        project.run_sql("drop table if exists window_table")

    def test_constraint_in_data_type_error(self, project):
        """Should error with clear message when constraints are in data_type field."""
        results = run_dbt(["run", "--full-refresh"], expect_pass=False)

        # Should have 1 success (source) and 1 error (window_table)
        assert len(results) == 2

        # Find the window_table result
        window_table_result = None
        for res in results:
            if res.node.name == "window_table":
                window_table_result = res
                break

        assert window_table_result is not None, "window_table not found in results"
        assert window_table_result.status.name == "Error", "Expected window_table to fail"

        # Check error message contains key guidance
        assert "constraint keyword in the data_type field" in window_table_result.message
        assert "cannot appear in data type definitions" in window_table_result.message
        assert "constraints" in window_table_result.message.lower()


# Regression for #31 — model-level (multi-column) PRIMARY KEY constraints used to
# render as `primary key NOT ENFORCED (col1, col2)`, which Flink rejects.
# Flink requires `PRIMARY KEY (col1, col2) NOT ENFORCED`.
PK_SOURCE = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
        'changelog.mode': 'append',
    }
) }}
order_id BIGINT NOT NULL,
customer_id BIGINT NOT NULL,
price DECIMAL(10, 2),
order_time TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
"""

PK_STREAMING_TABLE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
) }}
select order_id, customer_id, price from {{ ref('pk_source') }}
"""

PK_MODELS_YML = """
models:
  - name: pk_streaming_table
    constraints:
      - type: primary_key
        columns: [order_id, customer_id]
        expression: "NOT ENFORCED"
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
      - name: customer_id
        data_type: bigint
        constraints:
          - type: not_null
      - name: price
        data_type: decimal(10,2)
"""


class TestModelLevelPrimaryKey(ConfluentFixtures):
    """Regression test for #31: a model-level PRIMARY KEY constraint with
    multiple columns must render with the column list immediately after
    `PRIMARY KEY` and the expression (`NOT ENFORCED`) at the end. Otherwise
    Flink rejects the CREATE TABLE statement with a parse error."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "pk_source.sql": PK_SOURCE,
            "pk_streaming_table.sql": PK_STREAMING_TABLE,
            "models.yml": PK_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        yield
        project.run_sql("drop table if exists pk_streaming_table")
        project.run_sql("drop table if exists pk_source")

    def test_model_level_primary_key_renders_correctly(self, project):
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results), (
            "dbt run failed — model-level PRIMARY KEY likely rendered in the wrong order"
        )


# Regression for #34 — Flink's `DISTRIBUTED BY HASH(col) INTO N BUCKETS` clause
# belongs between the column list and the WITH clause. Triage confirmed there
# is no way to express it through the existing API (a custom model-level
# constraint renders inside the column parens, which Flink rejects). This test
# pins the new `distributed_by` config that solves the gap.
DIST_SOURCE = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
        'changelog.mode': 'append',
    }
) }}
order_id BIGINT NOT NULL,
price DECIMAL(10, 2),
order_time TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
"""

DIST_STREAMING_TABLE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
    distributed_by={'columns': ['order_id'], 'buckets': 4},
) }}
select order_id, price from {{ ref('dist_source') }}
"""

DIST_MODELS_YML = """
models:
  - name: dist_streaming_table
    columns:
      - name: order_id
        data_type: bigint
      - name: price
        data_type: decimal(10,2)
"""

# Malformed distributed_by configs — exercised by
# TestDistributedByHash.test_invalid_distributed_by_configs_raise to confirm
# validate_distributed_by_config raises a clear compile error for each shape.
DIST_INVALID_NOT_MAPPING = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
    distributed_by=['order_id'],
) }}
select order_id, price from {{ ref('dist_source') }}
"""

DIST_INVALID_EMPTY_COLUMNS = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
    distributed_by={'columns': []},
) }}
select order_id, price from {{ ref('dist_source') }}
"""

DIST_INVALID_STRING_COLUMNS = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
    distributed_by={'columns': 'order_id'},
) }}
select order_id, price from {{ ref('dist_source') }}
"""

DIST_INVALID_NON_STRING_ENTRY = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
    distributed_by={'columns': [42]},
) }}
select order_id, price from {{ ref('dist_source') }}
"""

DIST_INVALID_BACKTICK_IN_NAME = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
    distributed_by={'columns': ['foo`bar']},
) }}
select order_id, price from {{ ref('dist_source') }}
"""


class TestDistributedByHash(ConfluentFixtures):
    """Regression for #34: a `distributed_by` config on a streaming_table must
    emit `DISTRIBUTED BY HASH(...) INTO N BUCKETS` between the column list
    and the WITH clause, so Flink applies the requested distribution."""

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Override ConfluentFixtures default (which sets `+full_refresh: True`):
        # the validator test relies on the materialization wrapper running on a
        # second `dbt run` against an already-existing table, which a forced
        # full-refresh would short-circuit.
        return {"models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dist_source.sql": DIST_SOURCE,
            "dist_streaming_table.sql": DIST_STREAMING_TABLE,
            "models.yml": DIST_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        yield
        project.run_sql("drop table if exists dist_streaming_table")
        project.run_sql("drop table if exists dist_source")

    def test_distributed_by_hash_clause_in_ddl(self, project):
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results), "dbt run failed"

        ddl_rows = project.run_sql("SHOW CREATE TABLE dist_streaming_table", fetch="all")
        ddl = ddl_rows[0][0]
        assert "DISTRIBUTED BY HASH(`order_id`) INTO 4 BUCKETS" in ddl, (
            f"DISTRIBUTED BY clause missing or malformed in created table DDL:\n{ddl}"
        )

    def test_invalid_distributed_by_configs_raise(self, project):
        """Bundled: every malformed `distributed_by` shape must raise a clear
        compile error. Single method to avoid five fixture spin-ups. Uses
        `dbt run` because `dbt compile` does not invoke the materialization
        wrapper — the validator only fires during run. The validator runs at
        the top of the materialization (before any Confluent calls), so each
        invalid config short-circuits before any DDL is attempted."""
        cases = [
            (DIST_INVALID_NOT_MAPPING, "must be a mapping"),
            (DIST_INVALID_EMPTY_COLUMNS, "non-empty 'columns' list"),
            (DIST_INVALID_STRING_COLUMNS, "non-empty 'columns' list"),
            (DIST_INVALID_NON_STRING_ENTRY, "non-empty strings"),
            (DIST_INVALID_BACKTICK_IN_NAME, "backtick"),
        ]
        try:
            for model_sql, expected_msg in cases:
                set_model_file(project, relation(project, "dist_streaming_table"), model_sql)
                result = run_dbt(["run"], expect_pass=False)
                node = get_result_by_name(result, "dist_streaming_table")
                assert node is not None, (
                    f"dist_streaming_table not in run results for case '{expected_msg}'"
                )
                assert node.status.name == "Error", (
                    f"Expected Error for case '{expected_msg}', got '{node.status.name}'"
                )
                assert expected_msg.lower() in node.message.lower(), (
                    f"Expected error containing {expected_msg!r}, got: {node.message}"
                )
        finally:
            # Reset model to baseline so the rendering test (if it runs after
            # this one) sees a valid model.
            set_model_file(
                project, relation(project, "dist_streaming_table"), DIST_STREAMING_TABLE
            )


# Same regression as #34, but covering the CTAS `table` materialization path.
DIST_TABLE_MODEL = """
{{ config(
    materialized='table',
    distributed_by={'columns': ['order_id'], 'buckets': 2},
) }}
select order_id, price from {{ ref('dist_source') }}
"""

DIST_TABLE_MODEL_DIFFERENT_COLUMN = """
{{ config(
    materialized='table',
    distributed_by={'columns': ['price'], 'buckets': 2},
) }}
select order_id, price from {{ ref('dist_source') }}
"""

DIST_TABLE_MODEL_DIFFERENT_BUCKETS = """
{{ config(
    materialized='table',
    distributed_by={'columns': ['order_id'], 'buckets': 6},
) }}
select order_id, price from {{ ref('dist_source') }}
"""

DIST_TABLE_MODELS_YML = """
models:
  - name: dist_table
    columns:
      - name: order_id
        data_type: bigint
      - name: price
        data_type: decimal(10,2)
"""


class TestDistributedByHashOnTable(ConfluentFixtures):
    """Regression for #34: a `distributed_by` config on a `table` (CTAS) must
    emit `DISTRIBUTED BY HASH(...) INTO N BUCKETS` between the column list
    and the AS clause; and changes to that distribution must surface as
    drift errors when re-running without `--full-refresh`."""

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Override the `+full_refresh: True` default from ConfluentFixtures —
        # otherwise the drift assertions below would always force a recreate
        # and never reach the drift check.
        return {"models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dist_source.sql": DIST_SOURCE,
            "dist_table.sql": DIST_TABLE_MODEL,
            "models.yml": DIST_TABLE_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        yield
        project.run_sql("drop table if exists dist_table")
        project.run_sql("drop table if exists dist_source")

    def test_distributed_by_renders_and_drift_detected(self, project):
        """Bundled: rendering check (#34) + drift coverage for the CTAS path.
        Bundled because both share the same slow class-scoped table creation;
        once the table exists, additional drift assertions are cheap."""
        # Rendering check (#34): clause must appear in the created DDL
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results), "dbt run failed"

        ddl_rows = project.run_sql("SHOW CREATE TABLE dist_table", fetch="all")
        ddl = ddl_rows[0][0]
        assert "DISTRIBUTED BY HASH(`order_id`) INTO 2 BUCKETS" in ddl, (
            f"DISTRIBUTED BY clause missing or malformed in created table DDL:\n{ddl}"
        )

        # Drift coverage: changing the distribution column must raise
        set_model_file(project, relation(project, "dist_table"), DIST_TABLE_MODEL_DIFFERENT_COLUMN)
        result = run_dbt(["run"], expect_pass=False)
        assert_distribution_drift_error(result, "dist_table")

        # Drift coverage: changing the bucket count must raise
        set_model_file(
            project, relation(project, "dist_table"), DIST_TABLE_MODEL_DIFFERENT_BUCKETS
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_distribution_drift_error(result, "dist_table")


# Same regression as #34, but covering the `streaming_source` materialization.
DIST_SOURCE_WITH_DIST = """
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

DIST_SOURCE_WITH_DIST_DIFFERENT_COLUMN = """
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

DIST_SOURCE_WITH_DIST_DIFFERENT_BUCKETS = """
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


class TestDistributedByHashOnStreamingSource(ConfluentFixtures):
    """Regression for #34: a `distributed_by` config on a `streaming_source`
    must emit `DISTRIBUTED BY HASH(...) INTO N BUCKETS` between the column
    definitions and the WITH clause; and changes to that distribution must
    surface as drift errors when re-running without `--full-refresh`."""

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Override the `+full_refresh: True` default from ConfluentFixtures —
        # otherwise the drift assertions below would always force a recreate
        # and never reach the drift check.
        return {"models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dist_source_distributed.sql": DIST_SOURCE_WITH_DIST,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        yield
        project.run_sql("drop table if exists dist_source_distributed")

    def test_distributed_by_renders_and_drift_detected(self, project):
        """Bundled: rendering check (#34) + drift coverage for the streaming
        source path. Bundled because both share the same slow class-scoped
        table creation; once the table exists, additional drift assertions
        are cheap."""
        # Rendering check (#34)
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results), "dbt run failed"

        ddl_rows = project.run_sql("SHOW CREATE TABLE dist_source_distributed", fetch="all")
        ddl = ddl_rows[0][0]
        assert "DISTRIBUTED BY HASH(`order_id`) INTO 3 BUCKETS" in ddl, (
            f"DISTRIBUTED BY clause missing or malformed in created table DDL:\n{ddl}"
        )

        # Drift coverage: changing the distribution column must raise
        set_model_file(
            project,
            relation(project, "dist_source_distributed"),
            DIST_SOURCE_WITH_DIST_DIFFERENT_COLUMN,
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_distribution_drift_error(result, "dist_source_distributed")

        # Drift coverage: changing the bucket count must raise
        set_model_file(
            project,
            relation(project, "dist_source_distributed"),
            DIST_SOURCE_WITH_DIST_DIFFERENT_BUCKETS,
        )
        result = run_dbt(["run"], expect_pass=False)
        assert_distribution_drift_error(result, "dist_source_distributed")
