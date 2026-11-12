"""Artifact upload and sweeping for the `function` materialization (#180).

A function can name a local package (`artifact_path`) instead of an already-uploaded artifact.
The artifact is named `<project>-<package file name>-<content hash>` (see `artifact_display_name`)
and found by the `ArtifactKey` in its description, so it is immutable and "same key" means "same
content" from the same project: an unchanged package reuses the existing artifact, a changed one
gets a new artifact.

Flink functions are immutable (drop + create), and an artifact can't gain versions, so replacing
a function's package is a multi-step change: upload the new artifact, drop the old function,
create the new one.

The old artifact is never deleted as part of a replace: running and stopped statements stay
bound to the artifact they were compiled against, and nothing tells us which statements those
are. `ArtifactSweeper` finds the candidates for an explicit, manual sweep instead.

Every dbt-created artifact carries a `managed_by` marker naming the dbt project that uploaded
it. An artifact without it, or from another project, is never deleted by dbt.
"""

import hashlib
import json
import re
from pathlib import Path
from typing import NamedTuple, NewType

from confluent_sql import ArtifactContentFormat, ArtifactRuntimeLanguage, Connection, FlinkArtifact
from dbt_common.exceptions import CompilationError

from dbt.adapters.events.logging import AdapterLogger

from .naming import truncate_with_hash

logger = AdapterLogger("Confluent")

# Flink artifact IDs look like `cfa-xxxxxx`.
ARTIFACT_ID_PATTERN = r"cfa-[A-Za-z0-9]+"

MANAGED_BY = "dbt-confluent"


class ArtifactFormat(NamedTuple):
    """What a function's `language` implies about its package."""

    file_suffix: str
    content_format: ArtifactContentFormat
    runtime_language: ArtifactRuntimeLanguage


ARTIFACT_FORMATS: dict[str, ArtifactFormat] = {
    "java": ArtifactFormat(".jar", ArtifactContentFormat.JAR, ArtifactRuntimeLanguage.JAVA),
    "python": ArtifactFormat(".zip", ArtifactContentFormat.ZIP, ArtifactRuntimeLanguage.PYTHON),
}

# Characters of the content digest that go into an artifact name (the description has all of it).
_NAME_DIGEST_LENGTH = 8

# Characters kept from a package file's name when it goes into an artifact name.
_ILLEGAL_PACKAGE_CHARS = re.compile(r"[^A-Za-z0-9_-]")

# The artifact API's limit on a display name (the driver rejects longer ones).
MAX_ARTIFACT_NAME_LENGTH = 60

# Seconds the driver allows for transferring the package, passed explicitly so the timeouts
# that wait on an upload (see `function_artifacts_repo`) are sized from it rather than from a default.
UPLOAD_TIMEOUT_SECS = 300

# Times a package transfer that fails midway is re-sent (the driver's default is none, since it
# re-sends the whole archive). One retry, so two attempts, rides out a transient blip; failures to
# connect are retried by the driver's own default, as nothing was sent.
UPLOAD_TRANSFER_RETRIES = 1


class ArtifactConfig(NamedTuple):
    """A local package dbt uploads as an artifact, and what it is uploaded under."""

    path: Path
    name: str
    project: str
    language: str
    # The package's full content digest, recorded in the description.
    digest: "ArtifactDigest"

    @property
    def key(self) -> "ArtifactKey":
        return ArtifactKey(self.project, self.path.name, self.digest)


def encode_description(project: str, file_name: str, sha256: str) -> str:
    """The description of an artifact dbt uploaded for `project` from the package `file_name`.

    Carries everything the name may lose (it can be truncated, and its layout may change), so
    the description is the stable record of what an artifact is. `sha256` is the full digest.
    """
    return json.dumps(
        {"managed_by": MANAGED_BY, "project": project, "file_name": file_name, "sha256": sha256},
        indent=2,
    )


def _managed_description(description: str | None) -> dict | None:
    """The fields of a description that is `encode_description` output from dbt, else None."""
    try:
        marker = json.loads(description or "")
    except ValueError:
        return None
    if not isinstance(marker, dict) or marker.get("managed_by") != MANAGED_BY:
        return None
    return marker


def artifact_project(description: str | None) -> str | None:
    """The dbt project an artifact was uploaded for, from its `description`, else None.

    None if the description isn't `encode_description` output from dbt, or names no project.
    """
    marker = _managed_description(description)
    project = marker.get("project") if marker else None
    return project if isinstance(project, str) else None


class ArtifactKey(NamedTuple):
    """What identifies an uploaded package: the project, the file's name and its content.

    Recorded in the artifact's description, so artifacts are found by it and not by their name,
    whose layout is free to change.
    """

    project: str
    file_name: str
    sha256: str


def artifact_key(description: str | None) -> ArtifactKey | None:
    """The `ArtifactKey` in an artifact's `description`, else None if it isn't dbt's or is
    missing a field."""
    marker = _managed_description(description)
    if marker is None:
        return None
    fields = [marker.get(f) for f in ArtifactKey._fields]
    if not all(isinstance(f, str) for f in fields):
        return None
    return ArtifactKey(*fields)


# The digest of a package file's content. Only `file_digest` makes one, so a value of this type
# really is a content digest, and the places that key on "this content" accept nothing else.
ArtifactDigest = NewType("ArtifactDigest", str)


def file_digest(path: Path) -> ArtifactDigest:
    """The full SHA-256 hex digest of a file's content."""
    sha = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    return ArtifactDigest(sha.hexdigest())


def artifact_display_name(project: str, package: str, digest: str) -> str:
    """Name an artifact `<project>-<package>-<digest>`, so a person can tell which package it is.

    The project and package are joined with `-`, which dbt forbids in a project name, so a name
    maps back to exactly one (project, package) pair: `shop` + `eu_udfs` and `shop_eu` + `udfs`
    can't collide. The name must be unique per environment, which is what makes an upload under
    another project's name fail rather than overwrite.

    `package` is the package file's name without its suffix, with anything but letters, digits,
    `_` and `-` (e.g. the dots of a version) replaced by `_`, since the API's allowed characters
    are not documented. Two files whose names differ only there get the same label, which is fine:
    the digest still tells their artifacts apart.

    A label too long to leave room for the digest is cut short and ends in a hash of the full
    label, so the file name goes first. The project survives whole only if it is at most
    `MAX_ARTIFACT_NAME_LENGTH` minus the digest, the two `-`s and the label hash (44 characters
    today); a longer project is itself cut and the file name is gone. Ownership never depends on
    the name: it is the `project` in the description (`artifact_project`). The digest is never
    cut, since it is what makes the name mean "this content".
    """
    digest = digest[:_NAME_DIGEST_LENGTH]
    max_label = MAX_ARTIFACT_NAME_LENGTH - len(digest) - 1  # 1 for the "-"
    label = f"{project}-{_ILLEGAL_PACKAGE_CHARS.sub('_', package)}"
    if len(label) > max_label:
        label = truncate_with_hash(label, max_label, label)
    return f"{label}-{digest}"


def artifact_ids_in(external_artifacts: str) -> set[str]:
    """The artifact IDs in an `INFORMATION_SCHEMA.ROUTINES.EXTERNAL_ARTIFACTS` value, which looks
    like `confluent-artifact://cfa-xxxxxx/latest`."""
    pattern = rf"confluent-artifact://({ARTIFACT_ID_PATTERN})"
    return set(re.findall(pattern, external_artifacts))


class ProjectArtifact(NamedTuple):
    artifact: FlinkArtifact
    # The functions using it, fully qualified, as of the scan.
    functions: list[str]


def artifact_label(artifact: FlinkArtifact) -> str:
    return f"{artifact.id} ({artifact.display_name})"


class UnusedArtifact(NamedTuple):
    artifact: FlinkArtifact
    # Why deleting it failed; None if it was deleted.
    error: str | None

    @property
    def label(self) -> str:
        return artifact_label(self.artifact)


class ArtifactSweeper:
    """Finds, and optionally deletes, the artifacts of one dbt `project` that no function in
    `database` uses.
    """

    def __init__(self, conn: Connection, project: str, database: str) -> None:
        self._conn = conn
        self._project = project
        self._database = database

    def sweep(self, delete: bool = False) -> None:
        """The `sweep_function_artifacts` run-operation: report this project's unused artifacts
        (see `_unused_artifacts`), deleting them with `delete`, and fail at the end if any could
        not be deleted. `delete` must be a boolean: the macro forwards `--args` unchanged, so a
        quoted `"false"` would otherwise be truthy and delete."""
        if not isinstance(delete, bool):
            raise CompilationError(
                f"delete must be true or false, not {delete!r}; pass it unquoted: "
                "--args '{delete: true}'"
            )
        unused = self._unused_artifacts()
        if not unused:
            logger.info("No unused artifacts of this project.")
            return

        if delete:
            results = self._delete_artifacts(unused)
            for result in results:
                if result.error is None:
                    logger.info(f"Deleted: {result.label}")
            if failed := [f"{r.label}: {r.error}" for r in results if r.error is not None]:
                raise CompilationError(f"Could not delete: {'; '.join(failed)}")
        else:
            for artifact in unused:
                logger.info(f"Unused: {artifact_label(artifact)}")
            logger.info(
                "To delete the unused artifacts, once no statement depends on them: "
                "dbt run-operation sweep_function_artifacts --args '{delete: true}'"
            )

    def _unused_artifacts(self) -> list[FlinkArtifact]:
        """This project's artifacts that no function in the database uses. Read-only.

        Artifacts dbt didn't upload for the project are ignored. An artifact a run uploaded but has
        not yet created its function from looks unused too, so a sweep shouldn't run alongside a
        `dbt run`.

        Unused by a function doesn't mean unused: statements may still be bound to an artifact,
        which is why deleting is left to the user, on their word that statements using
        an old artifact were recreated. They stay bound to it, and fail to restart once it is gone.
        """
        return [entry.artifact for entry in self._scan_project_artifacts() if not entry.functions]

    def _delete_artifacts(self, artifacts: list[FlinkArtifact]) -> list[UnusedArtifact]:
        """Delete each of `artifacts`, trying every one; `error` is set on those that failed."""
        results: list[UnusedArtifact] = []
        for artifact in artifacts:
            error = None
            try:
                self._conn.get_artifacts_api().delete(artifact.id)
            except Exception as e:
                error = str(e) or type(e).__name__
            results.append(UnusedArtifact(artifact, error))
        return results

    def _scan_project_artifacts(self) -> list[ProjectArtifact]:
        """This project's artifacts, with the functions in the database that use each.

        Artifacts dbt didn't upload for the project are ignored.

        The artifacts are listed before the functions, so a function created in between can't
        leave its artifact looking unused.
        """
        ours = [
            a
            for a in self._conn.get_artifacts_api().list()
            if artifact_project(a.description) == self._project
        ]
        if not ours:
            return []
        users = self._functions_by_artifact()
        return [ProjectArtifact(a, users.get(a.id, [])) for a in ours]

    def _functions_by_artifact(self) -> dict[str, list[str]]:
        cursor = self._conn.cursor()
        try:
            cursor.execute(
                "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, EXTERNAL_ARTIFACTS "
                f"FROM `{self._database}`.`INFORMATION_SCHEMA`.`ROUTINES` "
                "WHERE EXTERNAL_ARTIFACTS IS NOT NULL"
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
        users: dict[str, list[str]] = {}
        for schema, name, artifacts in rows:
            for artifact_id in artifact_ids_in(artifacts):
                users.setdefault(artifact_id, []).append(f"{self._database}.{schema}.{name}")
        return {artifact_id: sorted(names) for artifact_id, names in users.items()}
