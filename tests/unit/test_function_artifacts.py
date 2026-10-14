"""Unit tests for uploading a function's artifact (#180).

The upload decides whether a function is re-created on a new artifact or left alone, and the
`managed_by` marker is what tells an artifact dbt created from any other, so both are tested
against in-memory fakes of the driver connection and of Flink.
"""

import json
import re
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
from dbt.adapters.confluent import functions, impl
from dbt.adapters.confluent.impl import ConfluentAdapter

CATALOG = "env-1"
DATABASE = "cluster-a"
FUNCTION = "is_smaller"
CLASS_NAME = "pkg.mod.is_smaller"
PREFIX, PROJECT = "dbt-", "proj"


class FakeDriver:
    """The slice of `confluent_sql.Connection` that artifacts use, backed by a dict."""

    def __init__(self, events: list[str]):
        self.artifacts: dict[str, Any] = {}
        self.events = events
        self.uploads: list[dict[str, Any]] = []  # `create`'s keyword arguments
        self.listed_languages: list[Any] = []  # the runtime_language of each listing
        self._ids = count(1)

    def add(self, display_name: str, description: str | None, artifact_id: str | None = None):
        artifact_id = artifact_id or f"cfa-{next(self._ids):06d}"
        self.artifacts[artifact_id] = SimpleNamespace(
            id=artifact_id, display_name=display_name, description=description
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
        return self.add(display_name, description)

    def get(self, artifact_id):
        if artifact_id not in self.artifacts:
            raise ArtifactNotFoundError("gone", artifact_id=artifact_id)
        return self.artifacts[artifact_id]

    def update(self, artifact_id, *, description):
        self.get(artifact_id).description = description
        return self.artifacts[artifact_id]


class FakeFlink:
    """Functions in a catalog, answering the statements `apply_function` issues."""

    def __init__(self, events: list[str]):
        self.functions: dict[str, dict[str, str]] = {}
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
        if sql.startswith("DESCRIBE FUNCTION"):
            if name not in self.functions:
                raise DbtDatabaseError(f"Function with the identifier '`{name}`' doesn't exist.")
            f = self.functions[name]
            rows = [
                ("class name", f["class"]),
                ("function language", "PYTHON"),
                ("plugin id", f["plugin"]),
            ]
            if f["connections"]:
                rows.append(("connections", f["connections"]))
            return None, SimpleNamespace(rows=rows)
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
        artifact_id = re.search(r"confluent-artifact://(\S+)'", sql).group(1)
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


def package(tmp_path: Path, content: bytes) -> dict[str, Any]:
    """A validated `artifact_path` udf config for a package with this content."""
    path = tmp_path / "udf.zip"
    path.write_bytes(content)
    digest = fa.file_digest(path)
    return {
        "language": "python",
        "artifact_id": None,
        "artifact_path": str(path),
        "project": PROJECT,
        "artifact_digest": digest,
        "artifact_name": fa.artifact_display_name(PREFIX, PROJECT, FUNCTION, digest),
        "class_name": CLASS_NAME,
        "connections": [],
        "connection_names": [],
    }


def apply(adapter, udf, replace: bool):
    existing = adapter.describe_function(relation()) if replace else None
    return adapter.apply_function(relation(), udf, existing, None, "create-name", "drop-name")


# A validated udf config that names an existing artifact instead of a package.
ID_UDF = {
    "language": "python",
    "artifact_path": None,
    "class_name": CLASS_NAME,
    "connection_names": [],
}


def managed(project: str = PROJECT) -> str:
    return fa.encode_description(project)


class TestDescription:
    def test_records_that_dbt_manages_the_artifact_and_for_which_project(self):
        assert json.loads(fa.encode_description("my_project")) == {
            "managed_by": "dbt-confluent",
            "project": "my_project",
        }


class TestFileDigestAndName:
    def test_same_content_same_digest_different_content_different(self, tmp_path):
        a, b, c = (tmp_path / n for n in "abc")
        a.write_bytes(b"x")
        b.write_bytes(b"x")
        c.write_bytes(b"y")
        assert fa.file_digest(a) == fa.file_digest(b) != fa.file_digest(c)

    def test_name_follows_statement_naming_plus_digest(self):
        assert fa.artifact_display_name("dbt-", "proj", "fn", "abc") == "dbt-proj-fn-abc"

    def test_digest_is_16_hex_characters(self, tmp_path):
        path = tmp_path / "udf.zip"
        path.write_bytes(b"x")
        assert re.fullmatch(r"[0-9a-f]{16}", fa.file_digest(path))

    @pytest.mark.parametrize("length", [1, 50, 200, 1000])
    def test_the_digest_is_never_cut_off_however_long_the_names(self, length):
        """The digest is what makes a name mean "this content", so it must survive any name."""
        digest = "0123456789abcdef"
        name = fa.artifact_display_name("p" * length, "j" * length, "f" * length, digest)
        assert name.endswith("-" + digest)
        assert len(name) == 3 * length + 2 + len(digest)


class TestEnsureArtifact:
    def test_uploads_a_new_artifact_marked_as_managed_by_dbt(self, driver, tmp_path):
        udf = package(tmp_path, b"v1")
        artifact = fa.ensure_artifact(driver, udf)  # type: ignore[arg-type]
        assert artifact.display_name == udf["artifact_name"]
        assert artifact.description == managed()

    def test_reuses_an_artifact_for_the_same_content(self, driver, events, tmp_path):
        udf = package(tmp_path, b"v1")
        first = fa.ensure_artifact(driver, udf)  # type: ignore[arg-type]
        again = fa.ensure_artifact(driver, udf)  # type: ignore[arg-type]
        assert again.id == first.id
        assert events == [f"upload {udf['artifact_name']}"]

    def test_leaves_an_artifact_we_did_not_create_alone(self, driver, tmp_path):
        udf = package(tmp_path, b"v1")
        theirs = driver.add(udf["artifact_name"], "hand made")
        assert fa.ensure_artifact(driver, udf).id == theirs.id  # type: ignore[arg-type]
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
        udf = {**package(tmp_path, b"v1"), "language": language}
        fa.ensure_artifact(driver, udf)  # type: ignore[arg-type]
        (upload,) = driver.uploads
        assert upload["content_format"] == content_format
        assert upload["runtime_language"] == runtime_language
        assert driver.listed_languages == [runtime_language]  # the lookup only lists that language

    def test_a_concurrent_run_winning_the_upload_race_is_not_an_error(
        self, driver, tmp_path, monkeypatch
    ):
        udf = package(tmp_path, b"v1")

        def lose_the_race(display_name, **_):
            driver.add(display_name, managed())
            raise ArtifactAlreadyExistsError("exists", display_name=display_name)

        monkeypatch.setattr(driver, "create", lose_the_race)

        artifact = fa.ensure_artifact(driver, udf)  # type: ignore[arg-type]

        assert artifact.display_name == udf["artifact_name"]

    def test_an_already_exists_error_with_nothing_to_find_is_raised(
        self, driver, tmp_path, monkeypatch
    ):
        def exists_but_invisible(display_name, **_):
            raise ArtifactAlreadyExistsError("exists", display_name=display_name)

        monkeypatch.setattr(driver, "create", exists_but_invisible)
        with pytest.raises(ArtifactAlreadyExistsError):
            fa.ensure_artifact(driver, package(tmp_path, b"v1"))  # type: ignore[arg-type]


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
        udf = {**ID_UDF, "artifact_id": "cfa-wanted"}
        with pytest.raises(raised, match=match):
            apply(adapter, udf, replace=True)
        assert flink.functions[FUNCTION]["plugin"] == old.id
        assert not any(e.startswith("drop") for e in events)

    def test_a_configured_artifact_id_is_used_as_is(self, adapter, driver, flink):
        existing = driver.add("theirs", None)
        udf = {**ID_UDF, "artifact_id": existing.id}
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
        assert udf["artifact_name"] in desired.artifact_id
        live = functions.FunctionState(CLASS_NAME, "python", "cfa-123456", frozenset())
        assert functions.diff_function_state(live, desired)


class TestAdapterValidateFunctionConfig:
    """`validate_function_config` adds what dbt needs to name and upload an artifact."""

    def validate(self, tmp_path, **config):
        (tmp_path / "udf.zip").write_bytes(b"zip")
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter.config = SimpleNamespace(  # type: ignore[assignment]
            project_root=str(tmp_path),
            project_name=PROJECT,
            credentials=SimpleNamespace(statement_name_prefix=PREFIX),
        )
        base = {"language": "python", "class": CLASS_NAME}
        return adapter.validate_function_config(base | config, relation())

    def test_an_artifact_path_is_named_after_its_project_function_and_digest(self, tmp_path):
        udf = self.validate(tmp_path, artifact_path="udf.zip")
        digest = fa.file_digest(tmp_path / "udf.zip")
        assert udf["project"] == PROJECT
        assert udf["artifact_digest"] == digest
        assert udf["artifact_name"] == fa.artifact_display_name(PREFIX, PROJECT, FUNCTION, digest)

    def test_an_artifact_id_has_no_upload_details(self, tmp_path):
        udf = self.validate(tmp_path, artifact_id="cfa-abc123")
        assert not {"project", "artifact_digest", "artifact_name"} & udf.keys()


class TestAdapterPlanFunctionChange:
    """For an `artifact_path`, the function is compared with the artifact already uploaded for
    that exact content: the same package is unchanged, a different one a change."""

    def live(self, artifact_id: str) -> functions.FunctionState:
        return functions.FunctionState(CLASS_NAME, "python", artifact_id, frozenset())

    def test_a_function_on_the_artifact_for_this_package_is_unchanged(
        self, adapter, driver, tmp_path
    ):
        udf = package(tmp_path, b"v1")
        artifact = driver.add(udf["artifact_name"], managed())
        assert adapter.plan_function_change(self.live(artifact.id), udf) == []

    def test_a_function_on_another_artifact_is_changed(self, adapter, driver, tmp_path):
        udf = package(tmp_path, b"v2")
        driver.add(udf["artifact_name"], managed())
        changes = adapter.plan_function_change(self.live("cfa-older"), udf)
        assert changes and changes[0].startswith("artifact_id")

    def test_a_package_that_is_not_uploaded_yet_is_a_change(self, adapter, driver, tmp_path):
        assert adapter.plan_function_change(self.live("cfa-older"), package(tmp_path, b"v3"))

    def test_an_absent_function_is_none(self, adapter, tmp_path):
        assert adapter.plan_function_change(None, package(tmp_path, b"v1")) is None
