"""Functional tests for the `function` materialization's wiring.

The pure logic (config validation, state diffing, action planning) is unit-tested in
tests/unit/test_functions.py. These prove the Jinja wiring end to end through a real `dbt run`:
that the behavior flag gates the materialization, and that the generic config validator runs.
Neither needs an uploaded artifact, since both fail before any `CREATE FUNCTION` is submitted.
"""

import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter._helpers import get_result_by_name
from tests.functional.adapter.fixtures import ConfluentFixtures

FLAG = "enable_experimental_function_materialization"

# Valid per `validate_function_config`, so any failure comes from what's under test.
FUNCTION_SQL = """
{{ config(
    materialized='function',
    language='java',
    artifact_id='cfa-doesnotmatter',
    class='com.example.Unused',
) }}
"""

# `tableflow` is a dbt-confluent key that a function doesn't consume.
UNSUPPORTED_CONFIG_FUNCTION_SQL = """
{{ config(
    materialized='function',
    language='java',
    artifact_id='cfa-doesnotmatter',
    class='com.example.Unused',
    tableflow={'formats': 'ICEBERG', 'storage': {'kind': 'Managed'}},
) }}
"""


def run_failing_function() -> str:
    """`dbt run`, expecting the `udf` function node to fail; returns its error message."""
    results = run_dbt(["run"], expect_pass=False)
    result = get_result_by_name(results, "udf")
    assert result is not None, "udf not found in results"
    assert result.status.name == "Error", f"Expected 'Error' but got '{result.status.name}'"
    return result.message


class TestFunctionRequiresBehaviorFlag(ConfluentFixtures):
    NAME = "functionflagoff"

    @pytest.fixture(scope="class")
    def functions(self):
        return {"udf.sql": FUNCTION_SQL}

    def test_disabled_by_default(self, project):
        assert FLAG in run_failing_function()


class TestFunctionValidatesMaterializationConfig(ConfluentFixtures):
    NAME = "functionvalidateconfig"

    @pytest.fixture(scope="class")
    def project_config_update(self, project_config_update):
        return {**project_config_update, "flags": {FLAG: True}}

    @pytest.fixture(scope="class")
    def functions(self):
        return {"udf.sql": UNSUPPORTED_CONFIG_FUNCTION_SQL}

    def test_unsupported_config_fails_the_run(self, project):
        message = run_failing_function()
        assert "tableflow" in message and "not supported" in message
