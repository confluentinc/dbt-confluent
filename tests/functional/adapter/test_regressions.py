"""Tests for specific regressions and bug fixes."""

import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter.fixtures import ConfluentFixtures


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
        for r in results:
            if r.node.name == "window_table":
                window_table_result = r
                break

        assert window_table_result is not None, "window_table not found in results"
        assert window_table_result.status.name == "Error", "Expected window_table to fail"

        # Check error message contains key guidance
        assert "constraint keywords in the data_type field" in window_table_result.message
        assert "constraints must be specified separately" in window_table_result.message
        assert "constraints:" in window_table_result.message
