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
from pathlib import Path
from typing import NamedTuple, NewType

from confluent_sql import ArtifactContentFormat, ArtifactRuntimeLanguage, Connection, FlinkArtifact

from .naming import truncate_with_hash

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
