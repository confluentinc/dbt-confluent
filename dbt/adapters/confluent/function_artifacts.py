"""Artifact upload and journaling for the `function` materialization (#180).

A function can name a local package (`artifact_path`) instead of an already-uploaded artifact.
The artifact is named after the package's content hash, so it is immutable and "same name" means
"same content": an unchanged package reuses the existing artifact, a changed one gets a new artifact.

Flink functions are immutable (drop + create), and an artifact can't gain versions, so replacing
a function's package is a multi-step change that can fail part way: upload the new artifact, drop
the old function, create the new one. The *journal* is what makes that recoverable. It lives in
the new artifact's description, is written before anything is dropped, and is removed once the
change completes. It records the old function to put back if the new one can't be created.

The old artifact is never deleted as part of a replace: running and stopped statements stay
bound to the artifact they were compiled against, and nothing tells us which statements those
are, so removing old artifacts is left to the user.

Every dbt-created artifact carries a `managed_by` marker naming the dbt project that uploaded
it, which tells an artifact dbt created from one it didn't.
"""

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from confluent_sql import (
    ArtifactAlreadyExistsError,
    ArtifactContentFormat,
    ArtifactNotFoundError,
    ArtifactRuntimeLanguage,
    Connection,
    FlinkArtifact,
)

from dbt.adapters.confluent.functions import FunctionState

MANAGED_BY = "dbt-confluent"

# What a function's `language` implies about its package.
ARTIFACT_FORMATS: dict[str, tuple[str, ArtifactContentFormat, ArtifactRuntimeLanguage]] = {
    "java": (".jar", ArtifactContentFormat.JAR, ArtifactRuntimeLanguage.JAVA),
    "python": (".zip", ArtifactContentFormat.ZIP, ArtifactRuntimeLanguage.PYTHON),
}

_DIGEST_LENGTH = 16


@dataclass(frozen=True)
class Journal:
    """An in-flight function replacement. See the module docstring."""

    # The old function's config (`restore_function_state` shape), to put back if the new one fails.
    restore: dict[str, Any] | None


def function_state_to_restore(state: FunctionState) -> dict[str, Any]:
    """A FunctionState as journal JSON. Connections are stored already rendered for DDL."""
    return {
        "class_name": state.class_name,
        "language": state.language,
        "artifact_id": state.artifact_id,
        "connections": sorted(c.render() for c in state.connections),
    }


def encode_description(project: str | None, journal: Journal | None) -> str:
    """The description of a dbt-managed artifact of `project`, carrying `journal` if a change
    is in flight."""
    body: dict[str, Any] = {"managed_by": MANAGED_BY, "project": project}
    if journal is not None:
        body["journal"] = {
            "restore": journal.restore,
        }
    return json.dumps(body, separators=(",", ":"), sort_keys=True)


def _decode(description: str | None) -> dict[str, Any] | None:
    try:
        body = json.loads(description or "")
    except ValueError:
        return None
    return body if isinstance(body, dict) and body.get("managed_by") == MANAGED_BY else None


def artifact_project(description: str | None) -> str | None:
    """The dbt project that uploaded this artifact, if dbt did."""
    body = _decode(description)
    return body.get("project") if body else None


def decode_description(description: str | None) -> tuple[bool, Journal | None]:
    """(is this artifact managed by dbt, its journal if a change is in flight)."""
    body = _decode(description)
    if body is None:
        return False, None
    raw = body.get("journal")
    if not isinstance(raw, dict):
        return True, None
    return True, Journal(raw.get("restore"))


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
    for artifact in conn.list_artifacts(runtime_language=runtime_language):
        if artifact.display_name == display_name:
            return artifact
    return None


def ensure_artifact(
    conn: Connection, udf: dict[str, Any], journal: Journal | None
) -> FlinkArtifact:
    """Upload the package as a new artifact unless one for this content exists; return it.

    A new artifact is created with `journal`. An existing one that carries no journal of its own
    gets `journal` written to it, but one that already has a journal (an earlier, interrupted
    run) keeps it, since that one remembers the original function.
    """
    name, language = udf["artifact_name"], udf["language"]
    existing = find_artifact(conn, name, language)
    if existing is None:
        _, content_format, runtime_language = ARTIFACT_FORMATS[language]
        try:
            return conn.create_artifact(
                name,
                file=Path(udf["artifact_path"]),
                content_format=content_format,
                runtime_language=runtime_language,
                description=encode_description(udf["project"], journal),
            )
        except ArtifactAlreadyExistsError:  # a concurrent run won the race
            existing = find_artifact(conn, name, language)
            if existing is None:
                raise
    managed, current = decode_description(existing.description)
    if managed and current is None and journal is not None:
        return conn.update_artifact(
            existing.id, description=encode_description(udf["project"], journal)
        )
    return existing


def read_journal(conn: Connection, artifact_id: str) -> Journal | None:
    """The journal on an artifact, or None (no journal, not managed, or artifact gone)."""
    try:
        return decode_description(conn.get_artifact(artifact_id).description)[1]
    except ArtifactNotFoundError:
        return None


def settle_journal(conn: Connection, artifact_id: str) -> None:
    """Finish a function change by clearing its journal.

    Call only once the function exists on `artifact_id`. If this raises, the journal is left
    for the next run to retry.
    """
    try:
        artifact = conn.get_artifact(artifact_id)
    except ArtifactNotFoundError:
        return
    managed, journal = decode_description(artifact.description)
    if managed and journal is not None:
        conn.update_artifact(
            artifact_id,
            description=encode_description(artifact_project(artifact.description), None),
        )
