"""Artifact upload and cleanup for the `function` materialization (#180).

A function can name a local package (`artifact_path`) instead of an already-uploaded artifact.
The artifact is named after the package's content hash, so it is immutable and "same name" means
"same content": an unchanged package reuses the existing artifact, a changed one gets a new artifact.

Flink functions are immutable (drop + create), and an artifact can't gain versions, so replacing
a function's package is a multi-step change: upload the new artifact, drop the old function,
create the new one.

The old artifact is never deleted as part of a replace: running and stopped statements stay
bound to the artifact they were compiled against, and nothing tells us which statements those
are. `find_unused_artifacts` finds the candidates for an explicit, manual cleanup instead.

Every dbt-created artifact carries a `managed_by` marker naming the dbt project that uploaded
it. An artifact without it, or from another project, is never deleted by dbt.
"""

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NewType

from confluent_sql import (
    ArtifactAlreadyExistsError,
    ArtifactContentFormat,
    ArtifactRuntimeLanguage,
    Connection,
    FlinkArtifact,
)

MANAGED_BY = "dbt-confluent"

# What a function's `language` implies about its package.
ARTIFACT_FORMATS: dict[str, tuple[str, ArtifactContentFormat, ArtifactRuntimeLanguage]] = {
    "java": (".jar", ArtifactContentFormat.JAR, ArtifactRuntimeLanguage.JAVA),
    "python": (".zip", ArtifactContentFormat.ZIP, ArtifactRuntimeLanguage.PYTHON),
}

_DIGEST_LENGTH = 16

# Seconds the driver allows for transferring the package, passed explicitly so the timeouts
# that wait on an upload (see `package_artifacts`) are sized from it rather than from a default.
UPLOAD_TIMEOUT_SECS = 300


def encode_description(project: str | None) -> str:
    """The description of an artifact dbt uploaded for `project`."""
    return json.dumps({"managed_by": MANAGED_BY, "project": project}, separators=(",", ":"))


def artifact_project(description: str | None) -> str | None:
    """The dbt project that uploaded an artifact with this description, if dbt did."""
    try:
        body = json.loads(description or "")
    except ValueError:
        return None
    if not isinstance(body, dict) or body.get("managed_by") != MANAGED_BY:
        return None
    return body.get("project")


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


def artifact_display_name(prefix: str, project: str, digest: ArtifactDigest) -> str:
    """Name an artifact after its project and content (`<prefix><project>-<digest>`), not after a
    function: every function that uses the same package shares the one artifact."""
    return f"{prefix}{project}-{digest}"


def find_artifact(conn: Connection, display_name: str, language: str) -> FlinkArtifact | None:
    """The artifact with this exact name, if any."""
    runtime_language = ARTIFACT_FORMATS[language][2]
    for artifact in conn.get_artifacts_api().list(runtime_language=runtime_language):
        if artifact.display_name == display_name:
            return artifact
    return None


@dataclass(frozen=True)
class FlinkArtifactParams:
    """What it takes to upload a function's package as an artifact."""

    name: str  # the artifact's display name (see `artifact_display_name`)
    digest: ArtifactDigest  # of the package, which `name` embeds
    path: Path  # the package file
    language: str  # the function's `language`, which decides the package's format
    project: str | None  # the dbt project that uploads it, recorded as its `managed_by` marker

    @classmethod
    def from_udf(cls, udf: dict[str, Any]) -> "FlinkArtifactParams":
        """The upload parameters in a validated `artifact_path` function config."""
        return cls(
            name=udf["artifact_name"],
            # Set by `validate_function_config` from `file_digest`, so it is one.
            digest=ArtifactDigest(udf["artifact_digest"]),
            path=Path(udf["artifact_path"]),
            language=udf["language"],
            project=udf["project"],
        )


def ensure_artifact(conn: Connection, params: FlinkArtifactParams) -> FlinkArtifact:
    """Upload the package as a new artifact unless one for this content exists; return it."""
    name, language = params.name, params.language
    existing = find_artifact(conn, name, language)
    if existing is not None:
        return existing
    _, content_format, runtime_language = ARTIFACT_FORMATS[language]
    try:
        return conn.get_artifacts_api().create(
            name,
            file=params.path,
            content_format=content_format,
            runtime_language=runtime_language,
            description=encode_description(params.project),
            upload_timeout=UPLOAD_TIMEOUT_SECS,
        )
    except ArtifactAlreadyExistsError:  # a concurrent run won the race
        existing = find_artifact(conn, name, language)
        if existing is None:
            raise
        return existing


def find_unused_artifacts(
    conn: Connection, project: str, used_artifact_ids: Callable[[], set[str]]
) -> list[FlinkArtifact]:
    """This dbt `project`'s artifacts that no function uses.

    Artifacts dbt didn't upload for `project` are ignored. An artifact a run uploaded but has
    not yet created its function from looks unused too, so cleanup shouldn't run alongside a
    `dbt run`.

    Artifacts are listed *before* functions are looked up (`used_artifact_ids`), so a function
    created in between can't be missed. Unused by a function doesn't mean unused: statements
    may still be bound to an artifact, which is why deleting is left to the user.
    """
    ours = [
        a for a in conn.get_artifacts_api().list() if artifact_project(a.description) == project
    ]
    if not ours:
        return []
    used = used_artifact_ids()
    return [a for a in ours if a.id not in used]
