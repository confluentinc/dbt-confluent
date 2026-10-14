"""Functional tests for the `function` materialization.

The pure logic (config validation, state diffing, action planning) is unit-tested in
tests/unit/test_functions.py. These prove the wiring end to end through a real `dbt build`.

Most classes need no uploaded artifact: they fail before (or at) `CREATE FUNCTION`, which is
all that's needed to prove the behavior flag, config validation and absent-function detection.

`TestFunctionLifecycle` walks every plan outcome (create, unchanged, keep, fail, replace) using two
artifacts it uploads itself, and `TestFunctionArtifactUpload` covers dbt uploading a local package
(`artifact_path`). Both need a key that can use the artifact API (the Global key), and delete
whatever they upload.
"""

import os
import tarfile
import uuid
import zipfile
from pathlib import Path

import pytest
from confluent_sql import ArtifactNotFoundError

from dbt.adapters.confluent import functions
from dbt.adapters.confluent.connections import ConfluentConnectionManager
from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file
from tests.functional.adapter._helpers import get_result_by_name
from tests.functional.adapter.fixtures import ConfluentFixtures

FLAG = "enable_experimental_function_materialization"
FUNCTION_NAME = "udf"
FUNCTION_FILE = f"{FUNCTION_NAME}.sql"

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


@pytest.fixture
def submitted_sql(monkeypatch):
    """The SQL of every statement submitted to Flink during the test, in order."""
    submitted: list[str] = []
    real = ConfluentConnectionManager.add_query

    def recording(self, sql, *args, **kwargs):
        submitted.append(sql)
        return real(self, sql, *args, **kwargs)

    monkeypatch.setattr(ConfluentConnectionManager, "add_query", recording)
    return submitted


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
        "bad_artifact_id": ({"artifact_id": "abc123"}, "one of 'artifact_id'"),
        "bad_class": ({"class_name": " "}, "'class' must be a non-empty string"),
        "bad_connections": ({"connections": ["a..c"]}, "'connections'"),
        "bad_id_and_path": ({"artifact_path": "udf.zip"}, "mutually exclusive"),
        "bad_missing_package": (
            {"language": "python", "artifact_id": None, "artifact_path": "missing.zip"},
            "is not a file",
        ),
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


PACKAGE_FILE = "udf_package.zip"
EXAMPLE_UDF_SOURCE = Path(__file__).parent / "data" / "example_udf-0.1.0"
EXAMPLE_UDF_CLASS = "example_udf.tshirt_sizing.is_smaller"

# Uploading an artifact is a control-plane call, which needs the Global key (like Tableflow).
requires_global_key = pytest.mark.skipif(
    not (os.getenv("CONFLUENT_GLOBAL_API_KEY") and os.getenv("CONFLUENT_GLOBAL_API_SECRET")),
    reason="Uploading artifacts needs CONFLUENT_GLOBAL_API_KEY and CONFLUENT_GLOBAL_API_SECRET",
)


def build_package(path: Path, version: str) -> None:
    """Write a Python UDF package (a ZIP of the example UDF's sdist, as Confluent's UDF guide
    describes) to `path`. The server validates a package's contents, so it has to be real.
    `version` is a marker file's content, so changing it changes the package's digest."""
    sdist = path.parent / f"{EXAMPLE_UDF_SOURCE.name}.tar.gz"
    with tarfile.open(sdist, "w:gz") as tar:
        tar.add(EXAMPLE_UDF_SOURCE, arcname=EXAMPLE_UDF_SOURCE.name)
    with zipfile.ZipFile(path, "w") as package:
        package.write(sdist, f"dist/{sdist.name}")
        package.writestr("version.txt", version)
    sdist.unlink()


def write_package(project, version: str) -> None:
    build_package(Path(project.project_root) / PACKAGE_FILE, version)


@pytest.fixture(scope="class")
def udf_artifact_ids(project, tmp_path_factory) -> list[str]:
    """Two different, valid Python UDF artifacts (both contain `EXAMPLE_UDF_CLASS`), uploaded for
    the duration of the test class."""
    prefix = project.adapter.config.credentials.statement_name_prefix
    ids: list[str] = []
    try:
        with project.adapter.connection_named("upload_udf_artifacts"):
            handle = project.adapter.connections.get_thread_handle()
            for version in ("a", "b"):
                path = tmp_path_factory.mktemp("udf") / "package.zip"
                build_package(path, version)
                artifact = handle.get_artifacts_api().create(
                    f"{prefix}{uuid.uuid4().hex[:12]}", file=path, runtime_language="PYTHON"
                )
                ids.append(artifact.id)
        yield ids
    finally:
        # A new connection: dbt closes the one above after each of its own invocations.
        with project.adapter.connection_named("delete_udf_artifacts"):
            handle = project.adapter.connections.get_thread_handle()
            for artifact_id in ids:
                try:
                    handle.get_artifacts_api().delete(artifact_id)
                except ArtifactNotFoundError:
                    pass


@requires_global_key
class TestFunctionLifecycle(FunctionFixtures):
    """One function taken through every plan outcome, in order (each test builds on the last):
    create, unchanged, keep, fail, replace on a config change, replace on --full-refresh.
    The function's state is checked with `DESCRIBE FUNCTION`, the plan with `plan_actions`.

    Uses pre-uploaded artifacts (`artifact_id`), as opposed to `TestFunctionArtifactUpload`."""

    NAME = "functionlifecycle"

    @pytest.fixture(scope="class")
    def functions(self):
        # The artifacts need the project, which needs this: test_1 fills in the real ID.
        return {f"{FUNCTION_NAME}.sql": self.sql(NONEXISTENT_ARTIFACT_ID)}

    @staticmethod
    def sql(artifact_id: str, **extra) -> str:
        return function_sql(artifact_id, EXAMPLE_UDF_CLASS, "python", **extra)

    def configure(self, project, artifact_id: str, **extra) -> None:
        write_file(
            self.sql(artifact_id, **extra),
            project.project_root,
            "functions",
            f"{FUNCTION_NAME}.sql",
        )

    @staticmethod
    def live_artifact_id(project) -> str:
        rows = project.run_sql(f"describe function `{FUNCTION_NAME}`", fetch="all")
        return {str(name): str(value) for name, value in rows}["plugin id"]

    def test_1_create(self, project, plan_actions, udf_artifact_ids):
        self.configure(project, udf_artifact_ids[0])
        run_dbt(["build"])
        assert plan_actions == ["create"]
        assert self.live_artifact_id(project) == udf_artifact_ids[0]

    def test_2_unchanged_rerun_is_a_noop(self, project, plan_actions, udf_artifact_ids):
        run_dbt(["build"])
        assert plan_actions == ["unchanged"]
        assert self.live_artifact_id(project) == udf_artifact_ids[0]

    def test_3_changed_with_continue_keeps_the_existing_function(
        self, project, plan_actions, udf_artifact_ids
    ):
        self.configure(project, udf_artifact_ids[1], on_configuration_change="continue")
        run_dbt(["build"])
        assert plan_actions == ["keep"]
        assert self.live_artifact_id(project) == udf_artifact_ids[0]

    def test_4_changed_with_fail_errors_and_changes_nothing(
        self, project, plan_actions, udf_artifact_ids
    ):
        self.configure(project, udf_artifact_ids[1], on_configuration_change="fail")
        errors = run_failing()
        assert "on_configuration_change" in errors[FUNCTION_NAME]
        assert plan_actions == ["fail"]
        assert self.live_artifact_id(project) == udf_artifact_ids[0]

    def test_5_changed_with_apply_replaces_the_function(
        self, project, plan_actions, udf_artifact_ids
    ):
        self.configure(project, udf_artifact_ids[1])  # `apply` is dbt's default
        run_dbt(["build"])
        assert plan_actions == ["replace"]
        assert self.live_artifact_id(project) == udf_artifact_ids[1]

    def test_6_full_refresh_replaces_an_unchanged_function(
        self, project, plan_actions, udf_artifact_ids
    ):
        # `fail` would block a change, but never a rebuild the user asked for.
        self.configure(project, udf_artifact_ids[1], on_configuration_change="fail")
        run_dbt(["build", "--full-refresh"])
        assert plan_actions == ["replace"]
        assert self.live_artifact_id(project) == udf_artifact_ids[1]

    def test_7_replacing_with_a_missing_artifact_leaves_the_function_alone(
        self, project, plan_actions, udf_artifact_ids
    ):
        """The artifact is checked before the function is dropped (#187)."""
        self.configure(project, NONEXISTENT_ARTIFACT_ID)
        errors = run_failing()
        assert NONEXISTENT_ARTIFACT_ID in errors[FUNCTION_NAME]
        assert self.live_artifact_id(project) == udf_artifact_ids[1]


@requires_global_key
class TestFunctionRetryAfterFailedStatement(FunctionFixtures):
    """A failed `CREATE FUNCTION` statement must not block the next run. Statement names carry
    the dbt invocation id, so the retry's statement doesn't collide with the failed one that is
    still hanging around in Flink. Each test builds on the last."""

    NAME = "functionretryafterfail"
    WRONG_CLASS = "example_udf.tshirt_sizing.no_such_function"

    @pytest.fixture(scope="class")
    def functions(self):
        # The artifact needs the project, which needs this: the tests fill in the real ID.
        return {FUNCTION_FILE: function_sql(NONEXISTENT_ARTIFACT_ID)}

    def configure(self, project, artifact_id: str, class_name: str) -> None:
        write_file(
            function_sql(artifact_id, class_name, "python"),
            project.project_root,
            "functions",
            FUNCTION_FILE,
        )

    def test_1_wrong_class_fails(self, project, plan_actions, udf_artifact_ids):
        # The artifact is real, but it doesn't contain this class.
        self.configure(project, udf_artifact_ids[0], self.WRONG_CLASS)
        errors = run_failing()
        assert FUNCTION_NAME in errors
        assert plan_actions == ["create"]

    def test_2_fixed_class_succeeds_on_resubmit(self, project, plan_actions, udf_artifact_ids):
        self.configure(project, udf_artifact_ids[0], EXAMPLE_UDF_CLASS)
        run_dbt(["build"])
        assert plan_actions == ["create"]
        rows = project.run_sql(f"describe function `{FUNCTION_NAME}`", fetch="all")
        assert {str(name): str(value) for name, value in rows}["plugin id"] == udf_artifact_ids[0]


@requires_global_key
class TestFunctionRunsHooks(FunctionFixtures):
    """`pre_hook` runs before the function is created and `post_hook` after. The hooks are cheap
    `SHOW` statements tagged with a marker comment so they can be found in what was submitted."""

    NAME = "functionhooks"
    PRE_MARKER = "/* function pre_hook */"
    POST_MARKER = "/* function post_hook */"

    @pytest.fixture(scope="class")
    def functions(self):
        return {FUNCTION_FILE: self.sql(NONEXISTENT_ARTIFACT_ID)}

    def sql(self, artifact_id: str) -> str:
        return function_sql(
            artifact_id,
            EXAMPLE_UDF_CLASS,
            "python",
            pre_hook=f"show catalogs {self.PRE_MARKER}",
            post_hook=f"show catalogs {self.POST_MARKER}",
        )

    def test_hooks_run_around_create(self, project, submitted_sql, udf_artifact_ids):
        # The artifact needs the project, which needs `functions`: fill in the real ID here.
        write_file(self.sql(udf_artifact_ids[0]), project.project_root, "functions", FUNCTION_FILE)
        run_dbt(["build"])

        def position(fragment: str) -> int:
            matches = [i for i, sql in enumerate(submitted_sql) if fragment.lower() in sql.lower()]
            assert len(matches) == 1, f"expected one {fragment!r} in {submitted_sql}"
            return matches[0]

        assert position(self.PRE_MARKER) < position("CREATE FUNCTION") < position(self.POST_MARKER)


@requires_global_key
class TestFunctionArtifactUpload(FunctionFixtures):
    """A function from a local `artifact_path`: dbt uploads the package and registers the
    function from it. Re-running reuses the artifact; a changed package gets a new artifact,
    and the function's previous one is kept (statements may still be bound to it). Each test
    builds on the last.

    Needs no pre-uploaded artifact, but does write to Confluent's artifact API (and so needs a
    key that can: the Global key). Anything it uploads is deleted afterwards."""

    NAME = "functionupload"
    FIRST_VERSION, SECOND_VERSION = "v1", "v2"
    # The package file the live function was created from. Rebuilding a package gives different
    # bytes each time (file timestamps), so a test that needs the *same* package puts these back.
    live_package: dict[str, bytes] = {}
    # The artifact a failed run uploaded for its package and left behind, for the next run to use.
    failed_run_artifact: dict[str, str] = {}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            f"{FUNCTION_NAME}.sql": function_sql(
                artifact_id=None, class_name=EXAMPLE_UDF_CLASS, language="python"
            ).replace("artifact_id=None", f"artifact_path={PACKAGE_FILE!r}")
        }

    @pytest.fixture(scope="class", autouse=True)
    def package(self, project):
        write_package(project, self.FIRST_VERSION)

    @pytest.fixture(scope="class", autouse=True)
    def delete_artifacts(self, project):
        yield
        with project.adapter.connection_named("cleanup_artifacts"):
            for artifact in self.our_artifacts(project):
                try:
                    project.adapter.connections.get_thread_handle().get_artifacts_api().delete(
                        artifact.id
                    )
                except Exception:
                    pass  # best effort

    def our_artifacts(self, project) -> list:
        prefix = project.adapter.config.credentials.statement_name_prefix
        stem = f"{prefix}{project.adapter.config.project_name}-{FUNCTION_NAME}-"
        handle = project.adapter.connections.get_thread_handle()
        return [a for a in handle.get_artifacts_api().list() if a.display_name.startswith(stem)]

    def live_artifact_id(self, project) -> str:
        rows = project.run_sql(f"describe function `{FUNCTION_NAME}`", fetch="all")
        return {str(name): str(value) for name, value in rows}["plugin id"]

    def artifacts(self, project) -> dict[str, str]:
        """Our artifacts, id -> description."""
        with project.adapter.connection_named("list_artifacts"):
            return {a.id: a.description for a in self.our_artifacts(project)}

    def test_0_compile_does_not_upload_or_create_anything(self, project, submitted_sql):
        """Parsing and compiling never run a materialization, so there is no side effect."""
        run_dbt(["compile"])
        assert self.artifacts(project) == {}
        assert submitted_sql == []

    def test_1_uploads_the_package_and_creates_the_function(self, project, plan_actions):
        run_dbt(["build"])
        assert plan_actions == ["create"]
        (artifact_id,) = self.artifacts(project)
        assert self.live_artifact_id(project) == artifact_id

    def test_2_unchanged_package_reuses_the_artifact(self, project, plan_actions, submitted_sql):
        (artifact_id,) = self.artifacts(project)

        run_dbt(["build"])

        assert plan_actions == ["unchanged"]
        assert self.artifacts(project).keys() == {artifact_id}
        assert not any("create function" in sql.lower() for sql in submitted_sql)

    def test_3_changed_package_replaces_the_function_and_keeps_the_old_artifact(
        self, project, plan_actions, submitted_sql
    ):
        (first_artifact_id,) = self.artifacts(project)
        write_package(project, self.SECOND_VERSION)

        run_dbt(["build"])

        assert plan_actions == ["replace"]
        # the live function is described once, and the result reused to plan and to replace it
        assert sum("describe function" in sql.lower() for sql in submitted_sql) == 1
        (second_artifact_id,) = self.artifacts(project).keys() - {first_artifact_id}
        assert self.live_artifact_id(project) == second_artifact_id

    def configure(self, project, class_name: str = EXAMPLE_UDF_CLASS, **extra) -> None:
        write_file(
            function_sql(
                artifact_id=None, class_name=class_name, language="python", **extra
            ).replace("artifact_id=None", f"artifact_path={PACKAGE_FILE!r}"),
            project.project_root,
            "functions",
            f"{FUNCTION_NAME}.sql",
        )

    def test_4_failed_create_restores_the_previous_function(self, project):
        previous_artifact_id = self.live_artifact_id(project)
        before = self.artifacts(project)
        write_package(project, "v3")
        self.configure(project, class_name="example_udf.tshirt_sizing.no_such_function")

        errors = run_failing()

        assert FUNCTION_NAME in errors
        assert self.live_artifact_id(project) == previous_artifact_id  # put back
        # the new package's artifact was uploaded before the failure, and nothing was deleted
        (uploaded_artifact_id,) = self.artifacts(project).keys() - before.keys()
        self.failed_run_artifact["id"] = uploaded_artifact_id

    def test_5_the_next_run_uses_the_artifact_already_uploaded(self, project):
        before = self.artifacts(project)
        self.configure(project, class_name=EXAMPLE_UDF_CLASS)

        run_dbt(["build"])

        assert self.live_artifact_id(project) == self.failed_run_artifact["id"]
        assert self.artifacts(project).keys() == before.keys()  # nothing was uploaded or deleted

    def test_6_continue_and_fail_upload_nothing(self, project, plan_actions):
        """The plan is made before anything is uploaded, so neither touches artifacts."""
        before, live_artifact_id = self.artifacts(project), self.live_artifact_id(project)
        package_file = Path(project.project_root) / PACKAGE_FILE
        self.live_package["bytes"] = package_file.read_bytes()
        write_package(project, "v4")

        self.configure(project, on_configuration_change="continue")
        run_dbt(["build"])
        assert plan_actions == ["keep"]

        self.configure(project, on_configuration_change="fail")
        assert FUNCTION_NAME in run_failing()
        assert plan_actions == ["keep", "fail"]

        assert self.artifacts(project) == before
        assert self.live_artifact_id(project) == live_artifact_id

    def test_7_full_refresh_re_creates_on_the_same_artifact(
        self, project, plan_actions, submitted_sql
    ):
        """An unchanged package is replaced on --full-refresh, without a new upload."""
        (Path(project.project_root) / PACKAGE_FILE).write_bytes(self.live_package["bytes"])
        self.configure(project)
        before, live_artifact_id = self.artifacts(project), self.live_artifact_id(project)

        run_dbt(["build", "--full-refresh"])

        assert plan_actions == ["replace"]
        assert any("create function" in sql.lower() for sql in submitted_sql)
        assert self.artifacts(project) == before
        assert self.live_artifact_id(project) == live_artifact_id


class TestFunctionEmptyRun(FunctionFixtures):
    """`--empty` only limits the rows models read; a function is DDL, so it is still created
    (models that call it in an `--empty` run need it), uploading its package like any other run."""

    NAME = "functionempty"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            FUNCTION_FILE: function_sql(
                artifact_id=None, class_name=EXAMPLE_UDF_CLASS, language="python"
            ).replace("artifact_id=None", f"artifact_path={PACKAGE_FILE!r}")
        }

    @pytest.fixture(scope="class", autouse=True)
    def package(self, project):
        write_package(project, "empty-run")
        yield
        prefix = project.adapter.config.credentials.statement_name_prefix
        stem = f"{prefix}{project.adapter.config.project_name}-{FUNCTION_NAME}-"
        with project.adapter.connection_named("delete_artifacts"):
            handle = project.adapter.connections.get_thread_handle()
            for artifact in handle.get_artifacts_api().list():
                if artifact.display_name.startswith(stem):
                    handle.get_artifacts_api().delete(artifact.id)

    def test_empty_run_uploads_and_creates_the_function(self, project):
        run_dbt(["build", "--empty"])
        rows = project.run_sql(f"describe function `{FUNCTION_NAME}`", fetch="all")
        assert {str(name): str(value) for name, value in rows}["plugin id"].startswith("cfa-")
