"""Functional tests for the lesser-used scoped credentials.

One project holding a Tableflow model and a function (uploaded from a local package, so its
run calls the artifact API). Each test runs it under a different credential set -- one dbt
target per set, picked with `--target` -- and asserts whether the model works or is refused
up front, per MATERIALIZATIONS.md#tableflow and #uploading-the-artifact:

| credentials                                      | Tableflow | function |
| ------------------------------------------------ | --------- | -------- |
| Flink key only                                   | refused   | refused  |
| Flink key + Cloud key                            | refused   | works    |
| Flink key + Tableflow key + database_kafka_...id | works     | refused  |

The unit tests cover every combination; these show the driver accepts the scoped keys and that
a refusal happens before any DDL. (A Global key and `auth: oauth` also work for both; neither
is a case here. The existing test suites for those features exercise global API keys extensively,
and oauth is tested separately - this test is specifically about the lesser used creds).

Needs the Flink key (the base of every scoped target), the Global key (it is the `default`
target, used for cleanup), a Cloud key and a Tableflow key, and the Kafka cluster id -- see the README's testing section for the variables.
"""

import os
from typing import NamedTuple
from uuid import uuid4

import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter._helpers import (
    capture_submitted_statement_properties,
    get_result_by_name,
)
from tests.functional.adapter.isolation import physical_name
from tests.functional.adapter.test_function import (
    EXAMPLE_UDF_CLASS,
    PACKAGE_FILE,
    FunctionFixtures,
    function_sql,
    write_package,
)
from tests.functional.adapter.test_tableflow import (
    _disable_tableflow_best_effort as disable_tableflow_best_effort,
)
from tests.functional.adapter.test_tableflow import _get_tableflow as get_tableflow

REQUIRED_ENV = (
    "CONFLUENT_FLINK_API_KEY",
    "CONFLUENT_FLINK_API_SECRET",
    "CONFLUENT_GLOBAL_API_KEY",
    "CONFLUENT_GLOBAL_API_SECRET",
    "CONFLUENT_CLOUD_API_KEY",
    "CONFLUENT_CLOUD_API_SECRET",
    "CONFLUENT_TABLEFLOW_API_KEY",
    "CONFLUENT_TABLEFLOW_API_SECRET",
    "CONFLUENT_KAFKA_CLUSTER_ID",
)
_missing = [name for name in REQUIRED_ENV if not os.getenv(name)]

pytestmark = pytest.mark.skipif(bool(_missing), reason=f"Needs {', '.join(_missing)}")

TABLE = "tf_creds_table"
FUNCTION = "tf_creds_function"

TABLEFLOW_MODEL = f"""
{{{{ config(
    materialized='table',
    tableflow={{'table_formats': 'ICEBERG', 'storage': {{'kind': 'Managed'}}}},
) }}}}
select 1 as id, 'a' as name
"""

# Substrings of the errors that refuse a model, from `require_tableflow_credentials` and
# `require_artifact_credentials`.
TABLEFLOW_REFUSED = "`tableflow` needs credentials"
FUNCTION_REFUSED = "needs credentials that can use the artifact API"


class Expected(NamedTuple):
    """Whether each model works under a credential set, or is refused up front."""

    tableflow_works: bool
    function_works: bool


# dbt target name -> expectations: the table in the module docstring.
CASES = {
    "flink_key": Expected(tableflow_works=False, function_works=False),
    "cloud_key": Expected(tableflow_works=False, function_works=True),
    "tableflow_key": Expected(tableflow_works=True, function_works=False),
}


@pytest.fixture
def submitted(monkeypatch):
    """Every statement the test's `dbt build` submits, in order."""
    return capture_submitted_statement_properties(monkeypatch)


def key_fields(prefix: str) -> dict[str, str]:
    """The profile's `<prefix>_api_key`/`_secret`, from `CONFLUENT_<PREFIX>_API_KEY`/`_SECRET`."""
    env = f"CONFLUENT_{prefix.upper()}_API"
    return {
        f"{prefix}_api_key": os.environ[f"{env}_KEY"],
        f"{prefix}_api_secret": os.environ[f"{env}_SECRET"],
    }


class TestScopedCredentials(FunctionFixtures):
    """Each credential set against the same Tableflow model and function."""

    NAME = "scopedcredentials"
    FUNCTIONS = [FUNCTION]

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, statement_name_prefix):
        """A dbt target per credential set; each is the standard test target with only its
        credentials swapped. `default` has the Global key, which `project.run_sql` and the
        class teardown use (see `disable_tableflow`)."""
        base = {k: v for k, v in dbt_profile_target.items() if "_api_" not in k}
        base |= {
            "statement_name_prefix": statement_name_prefix,
            "statement_label": f"dbt-test-{uuid4()}",
        }
        flink_key = key_fields("flink")
        credentials = {
            "flink_key": flink_key,
            "cloud_key": {**flink_key, **key_fields("cloud")},
            "tableflow_key": {
                **flink_key,
                **key_fields("tableflow"),
                "database_kafka_cluster_id": os.environ["CONFLUENT_KAFKA_CLUSTER_ID"],
            },
            "default": key_fields("global"),
        }
        outputs = {target: {**base, **fields} for target, fields in credentials.items()}
        return {"test": {"outputs": outputs, "target": "default"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {f"{TABLE}.sql": TABLEFLOW_MODEL}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            f"{FUNCTION}.sql": function_sql(
                artifact_id=None, class_name=EXAMPLE_UDF_CLASS, language="python"
            ).replace("artifact_id=None", f"artifact_path={PACKAGE_FILE!r}")
        }

    @pytest.fixture(scope="class", autouse=True)
    def package(self, project):
        write_package(project, "v1")

    @pytest.fixture(scope="class", autouse=True)
    def disable_tableflow(self, project, class_clean_up):
        # Tears down before `class_clean_up` drops the relations, so Tableflow is disabled first.
        yield
        # `project.adapter` is the adapter of the last dbt invocation, so it carries that run's
        # `--target` credentials. Cleanup needs the Global key, so switch back to `default`
        # first: it must not depend on which case happened to run last (or alone).
        run_dbt(["parse", "--target", "default"])
        # Disabling a topic that was never created (the Tableflow cases didn't run) is a 400.
        if physical_name(TABLE) in {row[0] for row in project.run_sql("show tables", fetch="all")}:
            disable_tableflow_best_effort(project, TABLE)

    @pytest.mark.parametrize("target", CASES)
    def test_tableflow_model(self, project, submitted, target):
        works = CASES[target].tableflow_works

        results = run_dbt(["build", "--select", TABLE, "--target", target], expect_pass=works)

        if works:
            topic = get_tableflow(project, TABLE)
            assert topic is not None, "Tableflow was not enabled"
            assert topic.phase.name == "RUNNING"
        else:
            assert TABLEFLOW_REFUSED in error_message(results, TABLE)
            # Refused before any DDL: no table was created for this run.
            assert not any("create table" in s["sql"].lower() for s in submitted)

    @pytest.mark.parametrize("target", CASES)
    def test_function(self, project, submitted, target):
        works = CASES[target].function_works

        results = run_dbt(["build", "--select", FUNCTION, "--target", target], expect_pass=works)

        if works:
            rows = project.run_sql(f"describe function `{physical_name(FUNCTION)}`", fetch="all")
            live = {str(name): str(value) for name, value in rows}
            assert live["plugin id"].startswith("cfa-")
        else:
            assert FUNCTION_REFUSED in error_message(results, FUNCTION)
            assert not any("create function" in s["sql"].lower() for s in submitted)


def error_message(results, name: str) -> str:
    result = get_result_by_name(results, name)
    assert result is not None and result.status.name == "Error", f"{name} did not fail: {result}"
    return result.message
