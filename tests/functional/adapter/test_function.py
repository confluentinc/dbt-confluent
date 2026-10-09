"""Functional tests for the `function` materialization.

The pure logic (config validation, state diffing, action planning) is unit-tested in
tests/unit/test_functions.py. These prove the wiring end to end through a real `dbt build`.

Most classes need no uploaded artifact: they fail before (or at) `CREATE FUNCTION`, which is
all that's needed to prove the behavior flag, config validation and absent-function detection.

`TestFunctionLifecycle` walks every plan outcome (create, unchanged, keep, fail, replace) and
needs two pre-uploaded artifacts that both contain the same class; it is skipped unless
CONFLUENT_TEST_UDF_ARTIFACT_ID, CONFLUENT_TEST_UDF_ARTIFACT_ID_2 and CONFLUENT_TEST_UDF_CLASS
are set (CONFLUENT_TEST_UDF_LANGUAGE defaults to python).
"""

import os

import pytest

from dbt.adapters.confluent import functions
from dbt.tests.util import run_dbt, write_file
from tests.functional.adapter._helpers import get_result_by_name
from tests.functional.adapter.fixtures import ConfluentFixtures

FLAG = "enable_experimental_function_materialization"
FUNCTION_NAME = "udf"

# Well-formed but (presumably) nonexistent, so it passes validation but can't be created.
NONEXISTENT_ARTIFACT_ID = "cfa-doesnotexist"
UNUSED_CLASS = "com.example.Unused"


def function_sql(
    artifact_id: str, class_name: str = UNUSED_CLASS, language: str = "java", **extra
):
    """A `functions/` file body: dbt requires one, so the config call is all it contains."""
    config = {
        "materialized": "function",
        "language": language,
        "artifact_id": artifact_id,
        "class": class_name,
        **extra,
    }
    args = ",\n    ".join(f"{key}={value!r}" for key, value in config.items())
    return "{{ config(\n    " + args + ",\n) }}\n"


def run_failing(*args: str) -> dict[str, str]:
    """`dbt build`, expecting a failure; returns each errored node's name -> message."""
    results = run_dbt(["build", *args], expect_pass=False)
    return {r.node.name: r.message for r in results if r.status.name == "Error"}


@pytest.fixture
def plan_actions(monkeypatch):
    """The `action` of every function plan made during the test, in order."""
    actions: list[str] = []
    real = functions.plan_function_action

    def recording(*args, **kwargs):
        plan = real(*args, **kwargs)
        actions.append(plan.action)
        return plan

    monkeypatch.setattr(functions, "plan_function_action", recording)
    return actions


@pytest.fixture
def detected_state(monkeypatch):
    """The result of every `plan_function_change` (None = judged absent), in order."""
    results: list[list[str] | None] = []
    real = functions.plan_function_change

    def recording(*args, **kwargs):
        result = real(*args, **kwargs)
        results.append(result)
        return result

    monkeypatch.setattr(functions, "plan_function_change", recording)
    return results


class FunctionFixtures(ConfluentFixtures):
    """Flag on by default (override `flags_enabled`), and the function dropped afterwards."""

    flags_enabled = True

    @pytest.fixture(scope="class")
    def project_config_update(self, project_config_update):
        if self.flags_enabled:
            return {**project_config_update, "flags": {FLAG: True}}
        return project_config_update

    @pytest.fixture(scope="class", autouse=True)
    def drop_functions(self, project):
        yield
        for name in getattr(self, "FUNCTIONS", [FUNCTION_NAME]):
            try:
                project.run_sql(f"drop function if exists `{name}`")
            except Exception:
                pass  # best effort; nothing else to clean up for a function


class TestFunctionRequiresBehaviorFlag(FunctionFixtures):
    NAME = "functionflagoff"
    flags_enabled = False

    @pytest.fixture(scope="class")
    def functions(self):
        return {f"{FUNCTION_NAME}.sql": function_sql(NONEXISTENT_ARTIFACT_ID)}

    def test_disabled_by_default(self, project):
        assert FLAG in run_failing()[FUNCTION_NAME]


class TestFunctionValidatesMaterializationConfig(FunctionFixtures):
    NAME = "functionvalidateconfig"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            f"{FUNCTION_NAME}.sql": function_sql(
                NONEXISTENT_ARTIFACT_ID,
                # `tableflow` is a dbt-confluent key that a function doesn't consume.
                tableflow={"formats": "ICEBERG", "storage": {"kind": "Managed"}},
            )
        }

    def test_unsupported_config_fails_the_run(self, project):
        message = run_failing()[FUNCTION_NAME]
        assert "tableflow" in message and "not supported" in message


class TestFunctionConfigErrors(FunctionFixtures):
    """Each bad config fails its own node with a readable error, all in one `dbt build`.
    None reaches Flink: validation runs before any `CREATE FUNCTION` is submitted."""

    NAME = "functionconfigerrors"
    # function name -> (config overrides, expected error substring)
    CASES = {
        "bad_language": ({"language": "sql"}, "'language' must be one of"),
        "bad_artifact_id": ({"artifact_id": "abc123"}, "'artifact_id' must be"),
        "bad_class": ({"class_name": " "}, "'class' must be a non-empty string"),
        "bad_connections": ({"connections": ["a..c"]}, "'connections'"),
    }
    FUNCTIONS = list(CASES)

    @pytest.fixture(scope="class")
    def functions(self):
        def body(overrides):
            overrides = dict(overrides)
            artifact_id = overrides.pop("artifact_id", NONEXISTENT_ARTIFACT_ID)
            class_name = overrides.pop("class_name", UNUSED_CLASS)
            return function_sql(artifact_id, class_name, **overrides)

        return {f"{name}.sql": body(overrides) for name, (overrides, _) in self.CASES.items()}

    @pytest.fixture(scope="class")
    def errors(self, project):
        return run_failing()

    @pytest.mark.parametrize("name", list(CASES))
    def test_error(self, errors, name):
        _, expected = self.CASES[name]
        assert name in errors, f"{name} did not fail: {sorted(errors)}"
        assert expected in errors[name]


class TestFunctionWithNonexistentArtifact(FunctionFixtures):
    """A syntactically valid but nonexistent artifact. Proves, live, that an absent function is
    recognized from `DESCRIBE FUNCTION`'s error (a fragile string match), and that the failure
    then comes from the create. If Flink ever accepted the nonexistent artifact at create time,
    the run would pass and this test would fail, which is worth knowing."""

    NAME = "functionghostartifact"

    @pytest.fixture(scope="class")
    def functions(self):
        return {f"{FUNCTION_NAME}.sql": function_sql(NONEXISTENT_ARTIFACT_ID)}

    def test_absent_function_is_detected_then_create_fails(
        self, project, detected_state, plan_actions
    ):
        errors = run_failing()

        assert FUNCTION_NAME in errors
        # None = "doesn't exist": detection worked, so the error is not from DESCRIBE.
        assert detected_state == [None]
        assert plan_actions == ["create"]


UDF_ARTIFACT_ID = os.getenv("CONFLUENT_TEST_UDF_ARTIFACT_ID")
UDF_ARTIFACT_ID_2 = os.getenv("CONFLUENT_TEST_UDF_ARTIFACT_ID_2")
UDF_CLASS = os.getenv("CONFLUENT_TEST_UDF_CLASS")
UDF_LANGUAGE = os.getenv("CONFLUENT_TEST_UDF_LANGUAGE", "python")


@pytest.mark.skipif(
    not (UDF_ARTIFACT_ID and UDF_ARTIFACT_ID_2 and UDF_CLASS)
    or UDF_ARTIFACT_ID == UDF_ARTIFACT_ID_2,
    reason=(
        "Needs two different pre-uploaded UDF artifacts containing the same class: set "
        "CONFLUENT_TEST_UDF_ARTIFACT_ID, CONFLUENT_TEST_UDF_ARTIFACT_ID_2 and "
        "CONFLUENT_TEST_UDF_CLASS (and optionally CONFLUENT_TEST_UDF_LANGUAGE)"
    ),
)
class TestFunctionLifecycle(FunctionFixtures):
    """One function taken through every plan outcome, in order (each test builds on the last):
    create, unchanged, keep, fail, replace on a config change, replace on --full-refresh.
    The function's state is checked with `DESCRIBE FUNCTION`, the plan with `plan_actions`."""

    NAME = "functionlifecycle"

    @pytest.fixture(scope="class")
    def functions(self):
        return {f"{FUNCTION_NAME}.sql": self.sql(UDF_ARTIFACT_ID)}

    @staticmethod
    def sql(artifact_id: str, **extra) -> str:
        return function_sql(artifact_id, UDF_CLASS, UDF_LANGUAGE, **extra)

    @staticmethod
    def configure(project, artifact_id: str, **extra) -> None:
        write_file(
            TestFunctionLifecycle.sql(artifact_id, **extra),
            project.project_root,
            "functions",
            f"{FUNCTION_NAME}.sql",
        )

    @staticmethod
    def live_artifact_id(project) -> str:
        rows = project.run_sql(f"describe function `{FUNCTION_NAME}`", fetch="all")
        return {str(name): str(value) for name, value in rows}["plugin id"]

    def test_1_create(self, project, plan_actions):
        run_dbt(["build"])
        assert plan_actions == ["create"]
        assert self.live_artifact_id(project) == UDF_ARTIFACT_ID

    def test_2_unchanged_rerun_is_a_noop(self, project, plan_actions):
        run_dbt(["build"])
        assert plan_actions == ["unchanged"]
        assert self.live_artifact_id(project) == UDF_ARTIFACT_ID

    def test_3_changed_with_continue_keeps_the_existing_function(self, project, plan_actions):
        self.configure(project, UDF_ARTIFACT_ID_2, on_configuration_change="continue")
        run_dbt(["build"])
        assert plan_actions == ["keep"]
        assert self.live_artifact_id(project) == UDF_ARTIFACT_ID

    def test_4_changed_with_fail_errors_and_changes_nothing(self, project, plan_actions):
        self.configure(project, UDF_ARTIFACT_ID_2, on_configuration_change="fail")
        errors = run_failing()
        assert "on_configuration_change" in errors[FUNCTION_NAME]
        assert plan_actions == ["fail"]
        assert self.live_artifact_id(project) == UDF_ARTIFACT_ID

    def test_5_changed_with_apply_replaces_the_function(self, project, plan_actions):
        self.configure(project, UDF_ARTIFACT_ID_2)  # `apply` is dbt's default
        run_dbt(["build"])
        assert plan_actions == ["replace"]
        assert self.live_artifact_id(project) == UDF_ARTIFACT_ID_2

    def test_6_full_refresh_replaces_an_unchanged_function(self, project, plan_actions):
        # `fail` would block a change, but never a rebuild the user asked for.
        self.configure(project, UDF_ARTIFACT_ID_2, on_configuration_change="fail")
        run_dbt(["build", "--full-refresh"])
        assert plan_actions == ["replace"]
        assert self.live_artifact_id(project) == UDF_ARTIFACT_ID_2
