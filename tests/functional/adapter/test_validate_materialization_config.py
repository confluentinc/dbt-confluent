"""Functional test for validate_materialization_config (config validation).

Scope note: the validation logic itself (which key is supported on which
materialization, error aggregation, the ignore_unsupported_config override)
is exhaustively unit-tested in tests/unit/test_validate_materialization_config.py.
This test exists only to prove the end-to-end wiring: that a real
materialization actually calls the validator before doing anything else, that
the resulting CompilationError surfaces through a real `dbt run`, and that
the override lets the run through. One representative case is enough --
kind-by-kind coverage is the unit tests' job.

The override case uses `materialized='view'` rather than `table`: a view is
pure metadata registration (no query execution), so it's the cheapest real
materialization that still proves the override works end to end, without
waiting on a CTAS statement to reach a terminal phase.
"""

import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter._helpers import get_result_by_name
from tests.functional.adapter.fixtures import ConfluentFixtures

BAD_CONFIG_TABLE = """
{{ config(
    materialized='table',
    statement_properties={'sql.tables.scan.idle-timeout': '30 s'},
) }}
select 1 as id
"""

OVERRIDDEN_CONFIG_VIEW = """
{{ config(
    materialized='view',
    statement_properties={'sql.tables.scan.idle-timeout': '30 s'},
    ignore_unsupported_config=['statement_properties'],
) }}
select 1 as id
"""

MODELS_YML = """
models:
  - name: bad_config_table
    columns:
      - name: id
        data_type: int
"""


class TestValidateMaterializationConfig(ConfluentFixtures):
    """Both models build in a single `dbt run` -- one is expected to fail,
    one to succeed -- rather than two separate runs for no extra coverage
    (same reasoning as TestSchemaDriftDetection in test_schema_drift.py)."""

    NAME = "validateconfig"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_config_table.sql": BAD_CONFIG_TABLE,
            "overridden_config_view.sql": OVERRIDDEN_CONFIG_VIEW,
            "models.yml": MODELS_YML,
        }

    @pytest.fixture(scope="class")
    def run_dbt_results(self, project):
        return run_dbt(["run"], expect_pass=False)

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        yield
        project.run_sql("drop table if exists bad_config_table")
        project.run_sql("drop view if exists overridden_config_view")

    def test_unsupported_config_fails_the_run(self, project, run_dbt_results):
        result = get_result_by_name(run_dbt_results, "bad_config_table")
        assert result is not None, "bad_config_table not found in results"
        assert result.status.name == "Error", (
            f"Expected status 'Error' but got '{result.status.name}'"
        )
        assert "statement_properties" in result.message
        assert "table" in result.message

    def test_ignore_unsupported_config_lets_the_run_through(self, project, run_dbt_results):
        result = get_result_by_name(run_dbt_results, "overridden_config_view")
        assert result is not None, "overridden_config_view not found in results"
        assert result.status.name == "Success", (
            f"Expected a successful run but got status '{result.status.name}': {result.message}"
        )
