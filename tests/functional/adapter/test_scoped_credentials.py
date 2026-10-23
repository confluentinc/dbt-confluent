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
| Flink key + Tableflow key, no cluster id         | refused   | refused  |
| Global key                                       | works     | works    |

(`auth: oauth` also works for both, but needs an interactive sign-in, and isn't covered.)

The Cloud and Tableflow keys are minted for the session from the Global key
(CONFLUENT_GLOBAL_API_KEY/SECRET, required) through the IAM API, and deleted afterwards. Keys a
crashed session leaked are swept at the start of the next one, once they are over two hours old.
"""

import os
import time
from typing import NamedTuple
from uuid import uuid4

import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter._api_keys import (
    RESOURCES,
    ApiKey,
    create_api_key,
    delete_api_key,
    iam_client,
    owner_id,
    resolve_kafka_cluster_id,
    sweep_stale_api_keys,
)
from tests.functional.adapter._helpers import (
    capture_submitted_statement_properties,
    delete_function_artifacts,
    delete_statements_by_label,
    disable_tableflow_best_effort,
    drop_any_relation,
    get_result_by_name,
    get_tableflow,
)
from tests.functional.adapter.test_function import (
    EXAMPLE_UDF_CLASS,
    PACKAGE_FILE,
    FunctionFixtures,
    function_sql,
    write_package,
)

GLOBAL_API_KEY = os.getenv("CONFLUENT_GLOBAL_API_KEY")
GLOBAL_API_SECRET = os.getenv("CONFLUENT_GLOBAL_API_SECRET")

pytestmark = pytest.mark.skipif(
    not (GLOBAL_API_KEY and GLOBAL_API_SECRET),
    reason="Needs CONFLUENT_GLOBAL_API_KEY/SECRET, to mint the Cloud and Tableflow keys",
)

_RUN_TAG = format(int(time.time()), "08x")
# `creds` is in test_tableflow.py's leftover-relation sweep pattern.
TABLE = f"dbttest_tf_creds_{_RUN_TAG}"
# Kept short: the uploaded artifact is named `<prefix><project>-<function>-<digest>` and a name
# over 60 characters is truncated, which `delete_function_artifacts` could no longer match.
FUNCTION = f"dbtt_creds_{_RUN_TAG}"

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
    "tableflow_key_without_cluster_id": Expected(tableflow_works=False, function_works=False),
    "global_key": Expected(tableflow_works=True, function_works=True),
}


@pytest.fixture
def submitted(monkeypatch):
    """Every statement the test's `dbt build` submits, in order."""
    return capture_submitted_statement_properties(monkeypatch)


def key_fields(prefix: str, key: str | None, secret: str | None) -> dict[str, str | None]:
    return {f"{prefix}_api_key": key, f"{prefix}_api_secret": secret}


@pytest.fixture(scope="class")
def iam():
    with iam_client(GLOBAL_API_KEY or "", GLOBAL_API_SECRET or "") as client:
        yield client


@pytest.fixture(scope="class")
def scoped_keys(iam):
    """A fresh Cloud key and Tableflow key, deleted when the class is done."""
    owner = owner_id(iam, GLOBAL_API_KEY or "")
    sweep_stale_api_keys(iam, owner)
    keys: dict[str, ApiKey] = {}
    try:
        for resource in RESOURCES:
            keys[resource] = create_api_key(iam, owner, resource, _RUN_TAG)
        yield keys
    finally:
        for key in keys.values():
            delete_api_key(iam, key.id)


@pytest.fixture(scope="class")
def kafka_cluster_id(iam, dbt_profile_target, unique_schema):
    return resolve_kafka_cluster_id(iam, dbt_profile_target["database"], unique_schema)


class TestScopedCredentials(FunctionFixtures):
    """Each credential set against the same Tableflow model and function."""

    NAME = "scopedcredentials"
    FUNCTIONS = [FUNCTION]

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, scoped_keys, kafka_cluster_id):
        """A dbt target per credential set; each is the standard test target with only its
        credentials swapped. The Global-key one is also `default`, which the test's own
        connection (setup, assertions, teardown) uses."""
        base = {k: v for k, v in dbt_profile_target.items() if "_api_" not in k}
        base |= {
            "statement_name_prefix": "dbt-adapter-test-",
            "statement_label": f"dbt-test-{uuid4()}",
        }
        global_key = key_fields("global", GLOBAL_API_KEY, GLOBAL_API_SECRET)
        # A Global key also works as the Flink key; fall back to it when none is configured.
        flink_key = key_fields(
            "flink",
            os.getenv("CONFLUENT_FLINK_API_KEY") or GLOBAL_API_KEY,
            os.getenv("CONFLUENT_FLINK_API_SECRET") or GLOBAL_API_SECRET,
        )
        cloud = scoped_keys["cloud"]
        tableflow = scoped_keys["tableflow"]
        tableflow_key = {**flink_key, **key_fields("tableflow", tableflow.id, tableflow.secret)}
        credentials = {
            "flink_key": flink_key,
            "cloud_key": {**flink_key, **key_fields("cloud", cloud.id, cloud.secret)},
            "tableflow_key": {**tableflow_key, "database_kafka_cluster_id": kafka_cluster_id},
            "tableflow_key_without_cluster_id": tableflow_key,
            "global_key": global_key,
        }
        outputs = {target: {**base, **fields} for target, fields in credentials.items()}
        outputs["default"] = outputs["global_key"]
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
    def class_clean_up(self, project, dbt_profile_data):
        yield
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        disable_tableflow_best_effort(project, TABLE)
        if drop_any_relation(project, TABLE):
            delete_statements_by_label(project, label)
        delete_function_artifacts(project, FUNCTION)

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
            rows = project.run_sql(f"describe function `{FUNCTION}`", fetch="all")
            live = {str(name): str(value) for name, value in rows}
            assert live["plugin id"].startswith("cfa-")
        else:
            assert FUNCTION_REFUSED in error_message(results, FUNCTION)
            assert not any("create function" in s["sql"].lower() for s in submitted)


def error_message(results, name: str) -> str:
    result = get_result_by_name(results, name)
    assert result is not None and result.status.name == "Error", f"{name} did not fail: {result}"
    return result.message
