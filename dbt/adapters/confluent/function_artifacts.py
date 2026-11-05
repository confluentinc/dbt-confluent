"""Artifact upload and sweeping for the `function` materialization (#180).

A function can name a local package (`artifact_path`) instead of an already-uploaded artifact.
The artifact is named after the package's content hash, so it is immutable and "same name" means
"same content": an unchanged package reuses the existing artifact, a changed one gets a new artifact.

Flink functions are immutable (drop + create), and an artifact can't gain versions, so replacing
a function's package is a multi-step change: upload the new artifact, drop the old function,
create the new one.

The old artifact is never deleted as part of a replace: running and stopped statements stay
bound to the artifact they were compiled against, and nothing tells us which statements those
are. `scan_unused_artifacts` finds the candidates for an explicit, manual sweep instead.

Every dbt-created artifact carries a `managed_by` marker naming the dbt project that uploaded
it. An artifact without it, or from another project, is never deleted by dbt.
"""

import hashlib
import json
import re
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any, NamedTuple, NewType

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

_DIGEST_LENGTH = 16

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


def encode_description(project: str | None) -> str:
    """The description of an artifact dbt uploaded for `project`."""
    return json.dumps({"managed_by": MANAGED_BY, "project": project}, separators=(",", ":"))


def artifact_project(description: str | None) -> str | None:
    """The dbt project an artifact was uploaded for, from its `description`, else None.

    None if the description isn't `encode_description` output from dbt, or names no project.
    """
    try:
        marker = json.loads(description or "")
    except ValueError:
        return None
    if not isinstance(marker, dict) or marker.get("managed_by") != MANAGED_BY:
        return None
    project = marker.get("project")
    return project if isinstance(project, str) else None


# The digest of a package file's content. Only `file_digest` makes one, so a value of this type
# really is a content digest, and the places that key on "this content" accept nothing else.
ArtifactDigest = NewType("ArtifactDigest", str)


def file_digest(path: Path) -> ArtifactDigest:
    """A short, stable digest of a file's content."""
    sha = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    return ArtifactDigest(sha.hexdigest()[:_DIGEST_LENGTH])


def artifact_display_name(project: str, digest: str) -> str:
    """Name an artifact after the dbt `project`, plus the content digest.

    A label too long to leave room for the digest is cut short and ends in a hash of the full
    label. The digest is never cut, since it is what makes the name mean "this content".
    """
    max_label = MAX_ARTIFACT_NAME_LENGTH - len(digest) - 1  # 1 for '-'
    label = project
    if len(label) > max_label:
        label = truncate_with_hash(label, max_label, label)
    return f"{label}-{digest}"


def artifact_ids_in(external_artifacts: Any) -> set[str]:
    """The artifact IDs in an `INFORMATION_SCHEMA.ROUTINES.EXTERNAL_ARTIFACTS` value, which looks
    like `confluent-artifact://cfa-xxxxxx/latest`."""
    pattern = rf"confluent-artifact://({ARTIFACT_ID_PATTERN})"
    return set(re.findall(pattern, str(external_artifacts)))


class ProjectArtifact(NamedTuple):
    artifact: FlinkArtifact
    # The functions using it, fully qualified, as of the scan.
    functions: list[str]


def artifact_label(artifact: FlinkArtifact) -> str:
    return f"{artifact.id} ({artifact.display_name})"


def scan_project_artifacts(
    conn: Connection,
    project: str,
    database: str,
    query_rows: Callable[[str], Iterable[Sequence[Any]]],
) -> list[ProjectArtifact]:
    """This dbt `project`'s artifacts, with the functions in `database` that use each.

    Artifacts dbt didn't upload for `project` are ignored.

    The artifacts are listed before the functions, so a function created in between can't leave
    its artifact looking unused. `query_rows` runs a SQL query and returns its rows.
    """
    ours = [
        a for a in conn.get_artifacts_api().list() if artifact_project(a.description) == project
    ]
    if not ours:
        return []
    users = _functions_by_artifact(database, query_rows)
    return [ProjectArtifact(a, users.get(a.id, [])) for a in ours]


def _functions_by_artifact(
    database: str, query_rows: Callable[[str], Iterable[Sequence[Any]]]
) -> dict[str, list[str]]:
    rows = query_rows(
        "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, EXTERNAL_ARTIFACTS "
        f"FROM `{database}`.`INFORMATION_SCHEMA`.`ROUTINES` "
        "WHERE EXTERNAL_ARTIFACTS IS NOT NULL"
    )
    users: dict[str, list[str]] = {}
    for schema, name, artifacts in rows:
        for artifact_id in artifact_ids_in(artifacts):
            users.setdefault(artifact_id, []).append(f"{database}.{schema}.{name}")
    return {artifact_id: sorted(names) for artifact_id, names in users.items()}


class UnusedArtifact(NamedTuple):
    artifact: FlinkArtifact
    # Why deleting it failed, if it was asked for and failed.
    error: str | None

    @property
    def label(self) -> str:
        return artifact_label(self.artifact)


def scan_unused_artifacts(
    conn: Connection,
    project: str,
    database: str,
    query_rows: Callable[[str], Iterable[Sequence[Any]]],
    delete: bool = False,
) -> list[UnusedArtifact]:
    """List this dbt `project`'s artifacts that no function in `database` uses, and delete them
    with `delete`.

    Artifacts dbt didn't upload for `project` are ignored. An artifact a run uploaded but has
    not yet created its function from looks unused too, so a sweep shouldn't run alongside a
    `dbt run`.

    Unused by a function doesn't mean unused: statements may still be bound to an artifact,
    which is why deleting is left to the user, on their word that statements using
    an old artifact were recreated. They stay bound to it, and fail to restart once it is gone.

    `error` is set if deleting an artifact failed (every one is tried).
    """
    unused: list[UnusedArtifact] = []
    for entry in scan_project_artifacts(conn, project, database, query_rows):
        if entry.functions:
            continue
        error = None
        if delete:
            try:
                conn.get_artifacts_api().delete(entry.artifact.id)
            except Exception as e:
                error = str(e) or type(e).__name__
        unused.append(UnusedArtifact(entry.artifact, error))
    return unused


def sweep(
    conn: Connection,
    project: str,
    database: str,
    query_rows: Callable[[str], Iterable[Sequence[Any]]],
    delete: bool = False,
) -> None:
    """The `sweep_function_artifacts` run-operation: report this project's unused artifacts
    (see `scan_unused_artifacts`), deleting them with `delete`, and fail at the end if any could
    not be deleted. `delete` must be a boolean: the macro forwards `--args` unchanged, so a
    quoted `"false"` would otherwise be truthy and delete."""
    if not isinstance(delete, bool):
        raise CompilationError(
            f"delete must be true or false, not {delete!r}; pass it unquoted: "
            "--args '{delete: true}'"
        )
    unused = scan_unused_artifacts(conn, project, database, query_rows, delete)
    if not unused:
        logger.info("No unused artifacts of this project.")
        return
    for result in unused:
        if result.error is None:
            logger.info(f"{'Deleted' if delete else 'Unused'}: {result.label}")
    if not delete:
        logger.info(
            "To delete the unused artifacts, once no statement depends on them: "
            "dbt run-operation sweep_function_artifacts --args '{delete: true}'"
        )
    if failed := [f"{r.label}: {r.error}" for r in unused if r.error is not None]:
        raise CompilationError(f"Could not delete: {'; '.join(failed)}")
