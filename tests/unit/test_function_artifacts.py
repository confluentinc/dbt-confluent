"""Unit tests for uploading a function's artifact (#180).

The upload decides whether a function is re-created on a new artifact or left alone, and the
`managed_by` marker is what tells an artifact dbt created from any other, so both are tested
against in-memory fakes of the driver connection and of Flink.
"""

import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from itertools import count
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from confluent_sql import (
    ArtifactAlreadyExistsError,
    ArtifactContentFormat,
    ArtifactNotFoundError,
    ArtifactRuntimeLanguage,
    OperationalError,
)
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.confluent import function_artifacts as fa
from dbt.adapters.confluent import function_artifacts_repo, functions, impl
from dbt.adapters.confluent.function_artifacts_repo import FunctionArtifactsRepo
from dbt.adapters.confluent.impl import ConfluentAdapter
from dbt.adapters.confluent.singleflight import ComputeTimeout, SharedComputeError

CATALOG = "env-1"
DATABASE = "cluster-a"
FUNCTION = "is_smaller"
CLASS_NAME = "pkg.mod.is_smaller"
PROJECT = "proj"
DIGEST = fa.ArtifactDigest("0123456789abcdef")


class FakeDriver:
    """The slice of `confluent_sql.Connection` that artifacts use, backed by a dict."""

    def __init__(self, events: list[str]):
        self.artifacts: dict[str, Any] = {}
        self.events = events
        self.uploads: list[dict[str, Any]] = []  # `create`'s keyword arguments
        self.listed_languages: list[Any] = []  # the runtime_language of each listing
        self._ids = count(1)

    def add(
        self,
        display_name: str,
        description: str | None,
        artifact_id: str | None = None,
        runtime_language: ArtifactRuntimeLanguage = ArtifactRuntimeLanguage.PYTHON,
    ):
        artifact_id = artifact_id or f"cfa-{next(self._ids):06d}"
        self.artifacts[artifact_id] = SimpleNamespace(
            id=artifact_id,
            display_name=display_name,
            description=description,
            runtime_language=runtime_language,
        )
        return self.artifacts[artifact_id]

    def get_artifacts_api(self):
        return self

    def list(self, runtime_language=None):
        self.listed_languages.append(runtime_language)
        return list(self.artifacts.values())

    def create(self, display_name, *, file, description, **rest):
        if any(a.display_name == display_name for a in self.artifacts.values()):
            raise ArtifactAlreadyExistsError("exists", display_name=display_name)
        self.uploads.append(rest)
        self.events.append(f"upload {display_name}")
        return self.add(display_name, description, runtime_language=rest["runtime_language"])

    def get(self, artifact_id):
        if artifact_id not in self.artifacts:
            raise ArtifactNotFoundError("gone", artifact_id=artifact_id)
        return self.artifacts[artifact_id]

    def delete(self, artifact_id, **_):
        self.get(artifact_id)
        self.events.append(f"delete {artifact_id}")
        del self.artifacts[artifact_id]

    def update(self, artifact_id, *, description):
        self.get(artifact_id).description = description
        return self.artifacts[artifact_id]


class FakeFlink:
    """Functions in a catalog, answering the statements `apply_function` issues."""

    def __init__(self, events: list[str]):
        self.functions: dict[str, dict[str, str]] = {}
        self.other_database_functions: dict[str, str] = {}  # name -> artifact id
        self.events = events
        self.fail_create_once = False
        self.fail_creates = 0  # fail this many of the next creates
        self.fail_drop = False
        self.created_sql: list[str] = []
        self.queries: list[str] = []

    def add(
        self, name: str, artifact_id: str, class_name: str = CLASS_NAME, connections: str = ""
    ):
        self.functions[name] = {
            "class": class_name,
            "plugin": artifact_id,
            "connections": connections,
        }

    def execute(self, sql: str, **kwargs):
        self.queries.append(sql)
        name = re.findall(r"`([^`]+)`", sql)[2]  # catalog, database, name
        if "INFORMATION_SCHEMA" in sql:
            rows = [
                (f"confluent-artifact://{f['plugin']}/latest",) for f in self.functions.values()
            ] + [
                (f"confluent-artifact://{a}/latest",)
                for a in self.other_database_functions.values()
            ]
            return None, SimpleNamespace(rows=rows)
        if sql.startswith("DESCRIBE FUNCTION"):
            if name not in self.functions:
                raise DbtDatabaseError(f"Function with the identifier '`{name}`' doesn't exist.")
            f = self.functions[name]
            described: list[tuple[str, str]] = [
                ("class name", f["class"]),
                ("function language", "PYTHON"),
                ("plugin id", f["plugin"]),
            ]
            if f["connections"]:
                described.append(("connections", f["connections"]))
            return None, SimpleNamespace(rows=described)
        if sql.startswith("drop function"):
            if self.fail_drop:
                raise DbtDatabaseError("drop failed")
            self.events.append(f"drop {name}")
            del self.functions[name]
            return None, None
        assert sql.startswith("create function"), sql
        if self.fail_create_once or self.fail_creates:
            self.fail_create_once = False
            self.fail_creates = max(0, self.fail_creates - 1)
            raise DbtDatabaseError("create failed")
        created_on = re.search(r"confluent-artifact://(\S+)'", sql)
        assert created_on, sql
        artifact_id = created_on.group(1)
        self.created_sql.append(sql)
        self.events.append(f"create {name} on {artifact_id}")
        self.add(name, artifact_id)
        return SimpleNamespace(message="created"), None


class FakeRelation(SimpleNamespace):
    def render(self):
        return f"`{self.database}`.`{self.schema}`.`{self.identifier}`"

    def incorporate(self, path):
        return FakeRelation(**{**vars(self), **path})


def relation(name: str = FUNCTION) -> FakeRelation:
    return FakeRelation(database=CATALOG, schema=DATABASE, identifier=name)


@pytest.fixture
def events() -> list[str]:
    return []


@pytest.fixture
def driver(events) -> FakeDriver:
    return FakeDriver(events)


@pytest.fixture
def flink(events) -> FakeFlink:
    return FakeFlink(events)


@pytest.fixture
def adapter(driver, flink) -> ConfluentAdapter:
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter.connections = SimpleNamespace(get_thread_handle=lambda: driver)  # type: ignore[assignment]
    adapter._artifacts_repo = FunctionArtifactsRepo()
    adapter.execute = flink.execute  # type: ignore[method-assign, assignment]
    adapter.config = SimpleNamespace(  # type: ignore[assignment]
        credentials=SimpleNamespace(database=CATALOG), project_name=PROJECT
    )  # type: ignore[assignment]
    return adapter


@pytest.fixture
def logger(monkeypatch) -> MagicMock:
    """The adapter's logger, to assert on what it warns about."""
    mock = MagicMock()
    monkeypatch.setattr(impl, "logger", mock)
    return mock


def package(tmp_path: Path, content: bytes) -> functions.FunctionConfig:
    """A validated `artifact_path` udf config for a package with this content."""
    path = tmp_path / "udf.zip"
    path.write_bytes(content)
    digest = fa.file_digest(path)
    return functions.FunctionConfig(
        language="python",
        class_name=CLASS_NAME,
        connections=[],
        connection_names=[],
        artifact_id=None,
        artifact_config=fa.ArtifactConfig(
            path=path,
            name=fa.artifact_display_name(PROJECT, digest),
            project=PROJECT,
            language="python",
        ),
    )


def artifact_of(udf: functions.FunctionConfig) -> fa.ArtifactConfig:
    assert udf.artifact_config
    return udf.artifact_config


def ensure(driver, udf: functions.FunctionConfig, artifacts: FunctionArtifactsRepo | None = None):
    return (artifacts or FunctionArtifactsRepo()).ensure(driver, artifact_of(udf))


def apply(adapter, udf, replace: bool):
    existing = adapter.describe_function(relation()) if replace else None
    return adapter.apply_function(relation(), udf, existing, None, "create-name", "drop-name")


# A validated udf config that names an existing artifact instead of a package.
ID_UDF = functions.FunctionConfig(
    language="python",
    class_name=CLASS_NAME,
    connections=[],
    connection_names=[],
    artifact_id=None,
    artifact_config=None,
)


def managed(project: str = PROJECT) -> str:
    return fa.encode_description(project)


class TestDescription:
    def test_records_that_dbt_manages_the_artifact_and_for_which_project(self):
        assert json.loads(fa.encode_description("my_project")) == {
            "managed_by": "dbt-confluent",
            "project": "my_project",
        }

    @pytest.mark.parametrize(
        "description, project",
        [
            (managed("my_project"), "my_project"),
            (None, None),
            ("my udf", None),
            ("[1]", None),
            ('{"managed_by": "someone"}', None),
            ('{"managed_by":"dbt-confluent"}', None),  # a marker without a project
        ],
    )
    def test_the_project_of_an_artifact(self, description, project):
        assert fa.artifact_project(description) == project


class TestFileDigestAndName:
    def test_same_content_same_digest_different_content_different(self, tmp_path):
        a, b, c = (tmp_path / n for n in "abc")
        a.write_bytes(b"x")
        b.write_bytes(b"x")
        c.write_bytes(b"y")
        assert fa.file_digest(a) == fa.file_digest(b) != fa.file_digest(c)

    def test_name_is_the_project_plus_digest(self):
        assert fa.artifact_display_name("proj", DIGEST) == f"proj-{DIGEST}"

    def test_digest_is_16_hex_characters(self, tmp_path):
        path = tmp_path / "udf.zip"
        path.write_bytes(b"x")
        assert re.fullmatch(r"[0-9a-f]{16}", fa.file_digest(path))

    @pytest.mark.parametrize("length", [1, 50, 200, 1000])
    def test_the_digest_is_never_cut_off_however_long_the_names(self, length):
        """The digest is what makes a name mean "this content", so it must survive any name."""
        name = fa.artifact_display_name("j" * length, DIGEST)
        assert name.endswith("-" + DIGEST)
        assert len(name) <= fa.MAX_ARTIFACT_NAME_LENGTH

    def test_a_name_that_exactly_fits_is_left_as_is(self):
        label_length = fa.MAX_ARTIFACT_NAME_LENGTH - len(DIGEST) - 1
        project = "j" * label_length
        name = fa.artifact_display_name(project, DIGEST)
        assert name == f"{project}-{DIGEST}"
        assert len(name) == fa.MAX_ARTIFACT_NAME_LENGTH

    def test_a_long_name_is_deterministic(self):
        args = ("j" * 100, DIGEST)
        assert fa.artifact_display_name(*args) == fa.artifact_display_name(*args)

    def test_long_labels_that_differ_only_after_the_cut_get_different_names(self):
        # The labels differ only in their last characters, which truncation cuts off, so
        # only the hash of the full label tells the two names apart.
        shared = "j" * 100
        one = fa.artifact_display_name(shared + "one", DIGEST)
        two = fa.artifact_display_name(shared + "two", DIGEST)
        assert one != two


class TestEnsureArtifact:
    def test_uploads_a_new_artifact_marked_as_managed_by_dbt(self, driver, tmp_path):
        udf = package(tmp_path, b"v1")
        artifact = ensure(driver, udf)
        assert artifact.display_name == artifact_of(udf).name
        assert artifact.description == managed()

    def test_reuses_an_artifact_for_the_same_content(self, driver, events, tmp_path):
        udf = package(tmp_path, b"v1")
        artifacts = FunctionArtifactsRepo()
        first = ensure(driver, udf, artifacts)
        again = ensure(driver, udf, artifacts)
        assert again.id == first.id
        assert events == [f"upload {artifact_of(udf).name}"]

    def test_leaves_an_artifact_we_did_not_create_alone(self, driver, tmp_path):
        udf = package(tmp_path, b"v1")
        theirs = driver.add(artifact_of(udf).name, "hand made")
        assert ensure(driver, udf).id == theirs.id
        assert theirs.description == "hand made"

    @pytest.mark.parametrize(
        "language, content_format, runtime_language",
        [
            ("python", ArtifactContentFormat.ZIP, ArtifactRuntimeLanguage.PYTHON),
            ("java", ArtifactContentFormat.JAR, ArtifactRuntimeLanguage.JAVA),
        ],
    )
    def test_uploads_with_the_format_and_language_of_the_function(
        self, driver, tmp_path, language, content_format, runtime_language
    ):
        udf = package(tmp_path, b"v1")
        FunctionArtifactsRepo().ensure(driver, artifact_of(udf)._replace(language=language))
        (upload,) = driver.uploads
        assert upload["content_format"] == content_format
        assert upload["runtime_language"] == runtime_language

    def test_the_same_package_under_a_second_language_is_an_error(self, driver, tmp_path):
        udf = package(tmp_path, b"v1")  # python
        as_java = artifact_of(udf)._replace(language="java")
        artifacts = FunctionArtifactsRepo()
        artifacts.ensure(driver, artifact_of(udf))

        with pytest.raises(DbtDatabaseError, match="two different languages"):
            artifacts.ensure(driver, as_java)

    def test_an_existing_artifact_of_another_language_is_an_error(self, driver, tmp_path):
        udf = package(tmp_path, b"v1")  # python
        driver.add(artifact_of(udf).name, managed(), runtime_language=ArtifactRuntimeLanguage.JAVA)
        with pytest.raises(DbtDatabaseError, match="two different languages"):
            ensure(driver, udf)

    def test_a_concurrent_run_winning_the_upload_race_is_not_an_error(
        self, driver, tmp_path, monkeypatch
    ):
        udf = package(tmp_path, b"v1")

        def lose_the_race(display_name, **_):
            driver.add(display_name, managed())
            raise ArtifactAlreadyExistsError("exists", display_name=display_name)

        monkeypatch.setattr(driver, "create", lose_the_race)

        artifact = ensure(driver, udf)

        assert artifact.display_name == artifact_of(udf).name
        assert len(driver.listed_languages) == 2  # the shared listing, then a fresh one

    def test_an_already_exists_error_with_nothing_to_find_is_raised(
        self, driver, tmp_path, monkeypatch
    ):
        def exists_but_invisible(display_name, **_):
            raise ArtifactAlreadyExistsError("exists", display_name=display_name)

        monkeypatch.setattr(driver, "create", exists_but_invisible)
        with pytest.raises(ArtifactAlreadyExistsError):
            ensure(driver, package(tmp_path, b"v1"))


class TestApplyFunction:
    """The whole create/replace flow through `ConfluentAdapter.apply_function`."""

    def old_function(self, driver, flink):
        old = driver.add("old-artifact", managed())
        flink.add(FUNCTION, old.id)
        return old

    def test_creates_a_function_from_a_new_package(self, adapter, driver, flink, tmp_path):
        apply(adapter, package(tmp_path, b"v1"), replace=False)
        (artifact,) = driver.artifacts.values()
        assert flink.functions[FUNCTION]["plugin"] == artifact.id

    def test_replace_uploads_before_dropping_and_keeps_the_old_artifact(
        self, adapter, driver, flink, events, tmp_path
    ):
        old = self.old_function(driver, flink)
        events.clear()
        apply(adapter, package(tmp_path, b"v2"), replace=True)
        assert [e.split()[0] for e in events] == ["upload", "drop", "create"]
        assert old.id in driver.artifacts  # statements may still be bound to it

    def test_a_rerun_after_a_failed_create_uses_the_artifact_already_uploaded(
        self, adapter, driver, flink, events, tmp_path
    ):
        old = self.old_function(driver, flink)
        udf = package(tmp_path, b"v2")
        flink.fail_create_once = True
        with pytest.raises(DbtDatabaseError, match="create failed"):
            apply(adapter, udf, replace=True)
        assert flink.functions[FUNCTION]["plugin"] == old.id  # put back
        events.clear()

        apply(adapter, udf, replace=True)

        assert not any(e.startswith("upload") for e in events)
        assert len(driver.artifacts) == 2  # the old one, and the new one, uploaded once

    @pytest.mark.parametrize(
        "lookup_error, raised, match",
        [
            (None, CompilationError, "cfa-wanted"),  # not found
            # a config that names an artifact needs credentials that can read it
            (OperationalError("forbidden", http_status_code=403), OperationalError, "forbidden"),
        ],
    )
    def test_any_failure_to_check_an_artifact_id_blocks_the_drop(
        self, adapter, driver, flink, events, monkeypatch, lookup_error, raised, match
    ):
        old = self.old_function(driver, flink)
        if lookup_error:
            monkeypatch.setattr(driver, "get", MagicMock(side_effect=lookup_error))
        udf = ID_UDF._replace(artifact_id="cfa-wanted")
        with pytest.raises(raised, match=match):
            apply(adapter, udf, replace=True)
        assert flink.functions[FUNCTION]["plugin"] == old.id
        assert not any(e.startswith("drop") for e in events)

    def test_a_configured_artifact_id_is_used_as_is(self, adapter, driver, flink):
        existing = driver.add("theirs", None)
        udf = ID_UDF._replace(artifact_id=existing.id)
        apply(adapter, udf, replace=False)
        assert flink.functions[FUNCTION]["plugin"] == existing.id


class TestApplyFunctionFailures:
    """What is left behind, and what is restored, when a replacement goes wrong."""

    def old_function(self, driver, flink, connections: str = ""):
        old = driver.add("old-artifact", managed())
        flink.add(FUNCTION, old.id, connections=connections)
        return old

    @pytest.mark.parametrize("failing_step", ["upload", "drop"])
    def test_a_failed_upload_or_drop_leaves_the_function_alone(
        self, adapter, driver, flink, events, tmp_path, monkeypatch, failing_step
    ):
        old = self.old_function(driver, flink)
        if failing_step == "upload":
            denied = OperationalError("forbidden", http_status_code=403)
            monkeypatch.setattr(driver, "create", MagicMock(side_effect=denied))
        else:
            flink.fail_drop = True
        events.clear()

        with pytest.raises((OperationalError, DbtDatabaseError), match="forbidden|drop failed"):
            apply(adapter, package(tmp_path, b"v2"), replace=True)

        assert flink.functions[FUNCTION]["plugin"] == old.id  # and nothing was restored
        assert not any(e.startswith(("drop", "create")) for e in events)

    def test_the_restored_function_is_the_original_down_to_its_connections(
        self, adapter, driver, flink, tmp_path
    ):
        old = self.old_function(driver, flink, connections="[`env-1`.`cluster-a`.`conn`]")
        flink.fail_create_once = True

        with pytest.raises(DbtDatabaseError):
            apply(adapter, package(tmp_path, b"v2"), replace=True)

        (restored,) = flink.created_sql
        assert f"'{CLASS_NAME}'" in restored
        assert "language python" in restored
        assert f"confluent-artifact://{old.id}'" in restored
        assert "using connections (" in restored and "`conn`" in restored

    def test_a_first_ever_create_that_fails_has_nothing_to_restore(
        self, adapter, driver, flink, tmp_path
    ):
        flink.fail_create_once = True
        with pytest.raises(DbtDatabaseError, match="create failed"):
            apply(adapter, package(tmp_path, b"v1"), replace=False)
        assert FUNCTION not in flink.functions


class TestReplaceWarning:
    """A replacement leaves the old artifact for statements still bound to it, and says so."""

    def warnings(self, logger) -> list[str]:
        return [call.args[0] for call in logger.warning.call_args_list]

    def test_replacing_onto_a_different_artifact_names_the_old_one(
        self, adapter, driver, flink, tmp_path, logger
    ):
        old = driver.add("old-artifact", managed())
        flink.add(FUNCTION, old.id)

        apply(adapter, package(tmp_path, b"v2"), replace=True)

        (message,) = self.warnings(logger)
        assert old.id in message
        assert "cleanup_function_artifacts" in message

    def test_creating_and_re_creating_on_the_same_artifact_do_not_warn(
        self, adapter, flink, tmp_path, logger
    ):
        udf = package(tmp_path, b"v1")
        apply(adapter, udf, replace=False)
        apply(adapter, udf, replace=True)  # e.g. --full-refresh
        assert self.warnings(logger) == []


class TestDesiredStateForAnUnuploadedPackage:
    def test_never_matches_a_live_function(self, tmp_path):
        udf = package(tmp_path, b"v1")
        desired = functions.desired_function_state(udf)
        assert artifact_of(udf).name in desired.artifact_id
        live = functions.FunctionState(CLASS_NAME, "python", "cfa-123456", frozenset())
        assert functions.diff_function_state(live, desired)


class TestCleanupFunctionArtifacts:
    def names(self, results) -> list[str]:
        return [r["artifact"] for r in results]

    def test_lists_this_projects_artifacts_that_no_function_uses(self, adapter, driver, flink):
        used = driver.add("used", managed())
        flink.add(FUNCTION, used.id)
        elsewhere = driver.add("elsewhere", managed())
        flink.other_database_functions["other_fn"] = elsewhere.id
        unused = driver.add("unused", managed())
        driver.add("other project", managed(project="another"))
        driver.add("theirs", "hand made")
        driver.add("no description", None)
        before = set(driver.artifacts)

        found = adapter.cleanup_function_artifacts()

        assert self.names(found) == [f"{unused.id} (unused)"]
        assert set(driver.artifacts) == before  # nothing is deleted by default

    def test_delete_removes_only_the_unused_artifacts_of_this_project(
        self, adapter, driver, flink, events
    ):
        used = driver.add("used", managed())
        flink.add(FUNCTION, used.id)
        unused = driver.add("unused", managed())
        theirs = driver.add("theirs", "hand made")
        other = driver.add("other project", managed(project="another"))

        found = adapter.cleanup_function_artifacts(delete=True)

        assert [r["error"] for r in found] == [None]
        assert set(driver.artifacts) == {used.id, theirs.id, other.id}
        assert events == [f"delete {unused.id}"]

    def test_checks_every_artifact(self, adapter, driver):
        count = 250  # past any page size or the org's artifact limit
        for i in range(count):
            driver.add(f"unused-{i}", managed())
        assert len(adapter.cleanup_function_artifacts()) == count

    def test_a_failed_delete_does_not_stop_the_rest(self, adapter, driver, monkeypatch):
        first, second, third = (driver.add(n, managed()) for n in ("a", "b", "c"))
        real = driver.delete

        def flaky(artifact_id, **kwargs):
            if artifact_id == second.id:
                raise OperationalError("denied")
            real(artifact_id, **kwargs)

        monkeypatch.setattr(driver, "delete", flaky)

        results = adapter.cleanup_function_artifacts(delete=True)

        assert [bool(r["error"]) for r in results] == [False, True, False]
        assert set(driver.artifacts) == {second.id}

    def test_functions_are_looked_up_after_artifacts_are_listed(self, adapter, driver, flink):
        """So a function created in between can't leave its artifact looking unused."""
        driver.add("unused", managed())
        order: list[str] = []
        real_list, real_execute = driver.list, flink.execute
        driver.list = lambda **k: (order.append("list"), real_list(**k))[1]  # type: ignore[method-assign]
        adapter.execute = lambda *a, **k: (order.append("functions"), real_execute(*a, **k))[1]  # type: ignore[method-assign]

        adapter.cleanup_function_artifacts()

        assert order == ["list", "functions"]

    def test_a_marker_without_a_project_is_ignored(self, adapter, driver):
        driver.add("legacy", '{"managed_by":"dbt-confluent"}')
        assert adapter.cleanup_function_artifacts(delete=True) == []
        assert len(driver.artifacts) == 1

    def test_functions_are_read_from_the_environments_information_schema(
        self, adapter, driver, flink
    ):
        driver.add("unused", managed())
        adapter.cleanup_function_artifacts()
        (query,) = flink.queries
        assert f"`{CATALOG}`.`INFORMATION_SCHEMA`.`ROUTINES`" in query

    def test_functions_are_not_queried_when_the_project_has_no_artifacts(
        self, adapter, driver, flink
    ):
        driver.add("theirs", "hand made")
        adapter.cleanup_function_artifacts()
        assert flink.queries == []


class TestAdapterValidateFunctionConfig:
    """`validate_function_config` adds what dbt needs to name and upload an artifact."""

    def validate(self, tmp_path, artifacts: FunctionArtifactsRepo | None = None, **config):
        (tmp_path / "udf.zip").write_bytes(b"zip")
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter._artifacts_repo = artifacts or FunctionArtifactsRepo()
        adapter.config = SimpleNamespace(  # type: ignore[assignment]
            project_root=str(tmp_path),
            project_name=PROJECT,
        )
        base = {"language": "python", "class": CLASS_NAME}
        return adapter.validate_function_config(base | config, relation())

    def test_an_artifact_path_is_named_after_its_project_and_digest(self, tmp_path):
        udf = self.validate(tmp_path, artifact_path="udf.zip")
        digest = fa.file_digest(tmp_path / "udf.zip")
        assert udf.artifact_config == fa.ArtifactConfig(
            path=(tmp_path / "udf.zip").resolve(),
            name=fa.artifact_display_name(PROJECT, digest),
            project=PROJECT,
            language="python",
        )

    def test_a_package_is_read_once_between_validating_and_uploading(
        self, tmp_path, driver, monkeypatch
    ):
        hashes = []
        real = fa.file_digest
        monkeypatch.setattr(fa, "file_digest", lambda p: (hashes.append(p), real(p))[1])

        artifacts = FunctionArtifactsRepo()

        udf = self.validate(tmp_path, artifacts, artifact_path="udf.zip")
        udf_again = self.validate(tmp_path, artifacts, artifact_path="udf.zip")
        artifacts.ensure(driver, artifact_of(udf))

        assert udf == udf_again
        assert len(hashes) == 1

    def test_an_artifact_id_has_no_artifact_config(self, tmp_path):
        udf = self.validate(tmp_path, artifact_id="cfa-abc123")
        assert udf.artifact_config is None


class TestRequireArtifactCredentials:
    """The artifact API needs a Global key or OAuth; fail up front without either."""

    def check(self, adapter, auth: str = "api_key", global_api_key: str | None = None):
        adapter.config = SimpleNamespace(  # type: ignore[assignment]
            credentials=SimpleNamespace(auth=auth, global_api_key=global_api_key)
        )
        adapter.require_artifact_credentials()

    def test_an_api_key_profile_needs_a_global_key(self, adapter):
        with pytest.raises(CompilationError, match="global_api_key"):
            self.check(adapter, global_api_key=None)
        self.check(adapter, global_api_key="global-key")

    def test_an_oauth_profile_needs_no_key(self, adapter):
        self.check(adapter, auth="oauth", global_api_key=None)


class TestAdapterPlanFunctionChange:
    """For an `artifact_path`, the function is compared with the artifact already uploaded for
    that exact content: the same package is unchanged, a different one a change."""

    def live(self, artifact_id: str) -> functions.FunctionState:
        return functions.FunctionState(CLASS_NAME, "python", artifact_id, frozenset())

    def test_a_function_on_the_artifact_for_this_package_is_unchanged(
        self, adapter, driver, tmp_path
    ):
        udf = package(tmp_path, b"v1")
        artifact = driver.add(artifact_of(udf).name, managed())
        assert adapter.plan_function_change(self.live(artifact.id), udf) == []

    def test_a_function_on_another_artifact_is_changed(self, adapter, driver, tmp_path):
        udf = package(tmp_path, b"v2")
        driver.add(artifact_of(udf).name, managed())
        changes = adapter.plan_function_change(self.live("cfa-older"), udf)
        assert changes and changes[0].startswith("artifact_id")

    def test_a_package_that_is_not_uploaded_yet_is_a_change(self, adapter, driver, tmp_path):
        assert adapter.plan_function_change(self.live("cfa-older"), package(tmp_path, b"v3"))

    def test_an_absent_function_is_none(self, adapter, tmp_path):
        assert adapter.plan_function_change(None, package(tmp_path, b"v1")) is None


THREADS = 6
CHANGED_CONTENT = b"v2"
WAITER_TIMEOUT = 0.05
OTHER_NAME, OTHER_PROJECT = "dbt-other-name", "other-proj"


def run_on_threads(call, threads: int = THREADS) -> list[Any]:
    """Run `call` on `threads` threads released together; return each outcome (value or error)."""
    barrier = threading.Barrier(threads)

    def one():
        barrier.wait(5)
        try:
            return call()
        except Exception as error:
            return error

    with ThreadPoolExecutor(max_workers=threads) as pool:
        return [f.result(5) for f in [pool.submit(one) for _ in range(threads)]]


class TestFunctionArtifactsRepo:
    """What threads building functions from one package do once between them."""

    def test_a_package_is_hashed_once(self, tmp_path, monkeypatch):
        path = artifact_of(package(tmp_path, b"v1")).path
        expected = fa.file_digest(path)
        hashes = []
        real = fa.file_digest
        monkeypatch.setattr(fa, "file_digest", lambda p: (hashes.append(p), real(p))[1])
        artifacts = FunctionArtifactsRepo()

        digests = run_on_threads(lambda: artifacts.digest(path))

        assert digests == [expected] * THREADS
        assert len(hashes) == 1

    def test_the_artifact_listing_is_fetched_once(self, driver, tmp_path):
        udf = package(tmp_path, b"v1")
        existing = driver.add(artifact_of(udf).name, managed())
        artifacts = FunctionArtifactsRepo()

        found = run_on_threads(lambda: artifacts.find(driver, artifact_of(udf).name))

        assert [a.id for a in found] == [existing.id] * THREADS
        assert len(driver.listed_languages) == 1

    def test_a_failed_listing_fails_every_caller_after_one_attempt(self, driver, tmp_path):
        listing_error = RuntimeError("list failed")
        calls = []

        def failing_list(runtime_language=None):
            calls.append(runtime_language)
            raise listing_error

        driver.list = failing_list  # type: ignore[method-assign]
        artifacts = FunctionArtifactsRepo()

        outcomes = run_on_threads(lambda: artifacts.find(driver, "any-name"))

        assert len(calls) == 1
        assert all(isinstance(o, Exception) for o in outcomes)
        assert outcomes.count(listing_error) == 1
        assert sum(isinstance(o, SharedComputeError) for o in outcomes) == THREADS - 1

    def test_a_package_is_uploaded_once_however_many_threads_ask(self, driver, events, tmp_path):
        udf = package(tmp_path, b"v1")
        artifacts = FunctionArtifactsRepo()

        ids = [a.id for a in run_on_threads(lambda: artifacts.ensure(driver, artifact_of(udf)))]

        assert len(set(ids)) == 1
        assert events == [f"upload {artifact_of(udf).name}"]

    def test_copies_of_a_package_share_one_upload_and_each_is_hashed(
        self, driver, events, tmp_path, monkeypatch
    ):
        copies = []
        for folder in ("a", "b"):
            (tmp_path / folder).mkdir()
            copies.append(package(tmp_path / folder, b"v1"))
        assert artifact_of(copies[0]).name == artifact_of(copies[1]).name
        hashes = []
        real = fa.file_digest
        monkeypatch.setattr(fa, "file_digest", lambda p: (hashes.append(p), real(p))[1])
        artifacts = FunctionArtifactsRepo()

        for udf in copies:
            artifacts.digest(artifact_of(udf).path)
            artifacts.digest(artifact_of(udf).path)
        for udf in copies:
            artifacts.ensure(driver, artifact_of(udf))

        assert len(hashes) == len(copies)
        assert events == [f"upload {artifact_of(copies[0]).name}"]

    def test_the_upload_is_given_the_named_timeout(self, driver, tmp_path):
        FunctionArtifactsRepo().ensure(driver, artifact_of(package(tmp_path, b"v1")))
        (upload,) = driver.uploads
        assert upload["upload_timeout"] == fa.UPLOAD_TIMEOUT_SECS

    def test_a_transfer_that_fails_midway_is_retried_at_least_once(self, driver, tmp_path):
        FunctionArtifactsRepo().ensure(driver, artifact_of(package(tmp_path, b"v1")))
        (upload,) = driver.uploads
        assert upload["upload_retry"].transfer_retries >= 1

    def test_what_a_build_uploads_is_visible_to_later_plans_without_a_new_listing(
        self, driver, tmp_path
    ):
        udf = package(tmp_path, b"v1")
        artifacts = FunctionArtifactsRepo()

        uploaded = artifacts.ensure(driver, artifact_of(udf))

        assert artifacts.find(driver, artifact_of(udf).name) is uploaded
        assert len(driver.listed_languages) == 1  # only ensure's lookup

    def test_a_failed_upload_is_shared_not_retried(self, driver, tmp_path, monkeypatch):
        udf = package(tmp_path, b"v1")
        attempts = []

        def failing_create(display_name, **_):
            attempts.append(display_name)
            raise OperationalError("upload failed")

        monkeypatch.setattr(driver, "create", failing_create)
        artifacts = FunctionArtifactsRepo()

        outcomes = run_on_threads(lambda: artifacts.ensure(driver, artifact_of(udf)))

        assert len(attempts) == 1
        assert all(isinstance(o, Exception) for o in outcomes)
        assert sum(isinstance(o, SharedComputeError) for o in outcomes) == THREADS - 1

    def test_a_waiter_gives_up_when_the_upload_outlasts_its_timeout(self, driver, tmp_path):
        udf = package(tmp_path, b"v1")
        artifacts = FunctionArtifactsRepo(upload_timeout=WAITER_TIMEOUT)
        leader_uploading, release_leader = threading.Event(), threading.Event()
        real_create = driver.create

        def slow_create(display_name, **kwargs):
            leader_uploading.set()
            assert release_leader.wait(5), "test never released the leader"
            return real_create(display_name, **kwargs)

        driver.create = slow_create  # type: ignore[method-assign]
        with ThreadPoolExecutor(max_workers=1) as pool:
            leader = pool.submit(artifacts.ensure, driver, artifact_of(udf))
            assert leader_uploading.wait(5)

            with pytest.raises(ComputeTimeout):
                artifacts.ensure(driver, artifact_of(udf))

            release_leader.set()
            assert leader.result(5).display_name == artifact_of(udf).name

    def test_losing_the_upload_race_relists_past_the_cached_listing(self, driver, tmp_path):
        udf = package(tmp_path, b"v1")
        artifacts = FunctionArtifactsRepo()
        winner: list[Any] = []
        attempts = []

        def create_lost_to_a_concurrent_run(display_name, **_):
            attempts.append(display_name)
            # The other run's artifact appears after the build's shared listing was taken.
            winner.append(driver.add(display_name, managed()))
            raise ArtifactAlreadyExistsError("exists", display_name=display_name)

        driver.create = create_lost_to_a_concurrent_run  # type: ignore[method-assign]

        found = run_on_threads(lambda: artifacts.ensure(driver, artifact_of(udf)))

        assert [a.id for a in found] == [winner[0].id] * THREADS
        assert len(attempts) == 1
        # The cached listing (taken before the winner existed) and one fresh one.
        assert len(driver.listed_languages) == 2

    def test_a_package_requested_under_a_different_config_gets_the_first_upload(
        self, driver, events, tmp_path
    ):
        first = artifact_of(package(tmp_path, b"v1"))
        other = first._replace(name=OTHER_NAME, project=OTHER_PROJECT)
        artifacts = FunctionArtifactsRepo()

        uploaded = artifacts.ensure(driver, first)
        shared = artifacts.ensure(driver, other)

        assert shared is uploaded
        assert events == [f"upload {first.name}"]
        assert not any(a.display_name == OTHER_NAME for a in driver.artifacts.values())


class TestFunctionArtifactsRepoLogging:
    """The debug log has one line per piece of work done, however many callers shared it."""

    @pytest.fixture
    def messages(self, monkeypatch) -> list[str]:
        logged: list[str] = []
        monkeypatch.setattr(function_artifacts_repo.logger, "debug", logged.append)
        return logged

    def test_a_package_is_logged_once_when_hashed(self, messages, tmp_path):
        udf = package(tmp_path, b"v1")
        artifacts = FunctionArtifactsRepo()

        run_on_threads(lambda: artifacts.digest(artifact_of(udf).path))

        assert [m for m in messages if "Hashing" in m] == [
            f"Hashing package {artifact_of(udf).path.name}"
        ]

    def test_the_listing_is_logged_once(self, messages, driver):
        artifacts = FunctionArtifactsRepo()

        run_on_threads(lambda: artifacts.find(driver, "any-name"))

        assert messages == ["Listing artifacts"]

    def test_an_upload_is_logged_once(self, messages, driver, tmp_path):
        udf = package(tmp_path, b"v1")
        artifacts = FunctionArtifactsRepo()

        run_on_threads(lambda: artifacts.ensure(driver, artifact_of(udf)))

        assert len([m for m in messages if artifact_of(udf).name in m]) == 1


class TestAdapterSharesPackageWork:
    def test_functions_over_one_package_share_one_artifact(
        self, adapter, driver, flink, events, tmp_path
    ):
        udf = package(tmp_path, b"v1")
        names = ["first_function", "second_function"]

        for name in names:
            adapter.apply_function(relation(name), udf, None, None, "c", "d")

        (artifact,) = driver.artifacts.values()
        assert events == [
            f"upload {artifact_of(udf).name}",
            *(f"create {name} on {artifact.id}" for name in names),
        ]

    def test_artifacts_are_listed_once_however_many_functions_are_created(
        self, adapter, driver, tmp_path
    ):
        first_udf = package(tmp_path, b"v1")
        (tmp_path / "other").mkdir()
        second_udf = package(tmp_path / "other", b"v2")
        assert artifact_of(first_udf).name != artifact_of(second_udf).name

        adapter.apply_function(relation("first_function"), first_udf, None, None, "c", "d")
        adapter.apply_function(relation("second_function"), second_udf, None, None, "c", "d")

        assert len(driver.uploads) == 2
        assert len(driver.listed_languages) == 1

    def test_a_shared_upload_failure_leaves_every_function_untouched(
        self, adapter, driver, flink, events, tmp_path, monkeypatch
    ):
        udf = package(tmp_path, b"v1")
        previous = driver.add("previous", managed())
        other = "other_function"
        flink.add(FUNCTION, previous.id)
        flink.add(other, previous.id)
        monkeypatch.setattr(
            driver, "create", lambda *a, **k: (_ for _ in ()).throw(OperationalError("no"))
        )

        for name in (FUNCTION, other):
            existing = adapter.describe_function(relation(name))
            with pytest.raises((OperationalError, SharedComputeError)):
                adapter.apply_function(relation(name), udf, existing, None, "c", "d")

        assert not [e for e in events if e.startswith(("drop", "create"))]
        assert set(flink.functions) == {FUNCTION, other}

    def test_an_unchanged_function_plans_without_uploading(self, adapter, driver, tmp_path):
        udf = package(tmp_path, b"v1")
        artifact = driver.add(artifact_of(udf).name, managed())
        live = functions.FunctionState(CLASS_NAME, "python", artifact.id, frozenset())

        assert adapter.plan_function_change(live, udf) == []
        assert driver.uploads == []

    def test_each_adapter_starts_with_nothing_remembered(self, tmp_path, monkeypatch):
        monkeypatch.setattr(impl.SQLAdapter, "__init__", lambda self, config, mp_context: None)
        path = tmp_path / "udf.zip"
        path.write_bytes(b"v1")
        first = ConfluentAdapter(None, None)
        before = first._artifacts_repo.digest(path)

        path.write_bytes(CHANGED_CONTENT)
        second = ConfluentAdapter(None, None)

        assert first._artifacts_repo.digest(path) == before
        assert second._artifacts_repo.digest(path) != before
