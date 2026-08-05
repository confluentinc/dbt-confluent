"""Tests for the `table` materialization."""

import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter._helpers import get_result_by_name
from tests.functional.adapter.fixtures import ConfluentFixtures

SIMPLE_TABLE = """
{{ config(materialized='table') }}
select 1 as id, 'a' as name
"""

SIMPLE_TABLE_MODELS_YML = """
models:
  - name: simple_table
    columns:
      - name: id
        data_type: int
      - name: name
        data_type: string
"""


class TestTableMaterialization(ConfluentFixtures):
    NAME = "table_basic"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_table.sql": SIMPLE_TABLE,
            "models.yml": SIMPLE_TABLE_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        yield
        project.run_sql("drop table if exists simple_table")

    def test_ctas_completes_in_snapshot_mode(self, project):
        """Regression for #77: the CTAS statement must run in snapshot mode,
        not streaming. A streaming-mode CTAS never reaches a terminal phase
        on its own; get_response() reports the statement's terminal phase as
        the result message, so this is checked directly from the run result
        rather than a post-run statement lookup (which would race the
        adapter's own post-execute cleanup of completed statements)."""
        results = run_dbt(["run"])
        result = get_result_by_name(results, "simple_table")
        assert result is not None, "simple_table not found in run results"
        assert result.message == "Phase.COMPLETED", (
            "simple_table's CTAS did not complete on its own "
            f"(message: {result.message}) -- likely submitted as a streaming query"
        )
