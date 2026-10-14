"""Artifact upload for the `function` materialization (#180).

A function can name a local package (`artifact_path`) instead of an already-uploaded artifact.
The artifact is named after the package's content hash, so it is immutable and "same name" means
"same content": an unchanged package reuses the existing artifact, a changed one gets a new artifact.

Flink functions are immutable (drop + create), and an artifact can't gain versions, so replacing
a function's package is a multi-step change: upload the new artifact, drop the old function,
create the new one.

The old artifact is never deleted as part of a replace: running and stopped statements stay
bound to the artifact they were compiled against, and nothing tells us which statements those
are, so removing old artifacts is left to the user.

Every dbt-created artifact carries a `managed_by` marker naming the dbt project that uploaded
it, which tells an artifact dbt created from one it didn't.
"""

import hashlib
import json
from pathlib import Path
from typing import Any

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


def encode_description(project: str | None) -> str:
    """The description of an artifact dbt uploaded for `project`."""
    return json.dumps({"managed_by": MANAGED_BY, "project": project}, separators=(",", ":"))


def file_digest(path: Path) -> str:
    """A short, stable digest of a file's content."""
    sha = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    return sha.hexdigest()[:_DIGEST_LENGTH]


def artifact_display_name(prefix: str, project: str, function: str, digest: str) -> str:
    """Name an artifact like a statement (`<prefix><project>-<function>`), plus the content digest."""
    return f"{prefix}{project}-{function}-{digest}"


def find_artifact(conn: Connection, display_name: str, language: str) -> FlinkArtifact | None:
    """The artifact with this exact name, if any."""
    runtime_language = ARTIFACT_FORMATS[language][2]
    for artifact in conn.get_artifacts_api().list(runtime_language=runtime_language):
        if artifact.display_name == display_name:
            return artifact
    return None


def ensure_artifact(conn: Connection, udf: dict[str, Any]) -> FlinkArtifact:
    """Upload the package as a new artifact unless one for this content exists; return it."""
    name, language = udf["artifact_name"], udf["language"]
    existing = find_artifact(conn, name, language)
    if existing is not None:
        return existing
    _, content_format, runtime_language = ARTIFACT_FORMATS[language]
    try:
        return conn.get_artifacts_api().create(
            name,
            file=Path(udf["artifact_path"]),
            content_format=content_format,
            runtime_language=runtime_language,
            description=encode_description(udf["project"]),
        )
    except ArtifactAlreadyExistsError:  # a concurrent run won the race
        existing = find_artifact(conn, name, language)
        if existing is None:
            raise
        return existing
