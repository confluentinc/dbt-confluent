"""Artifact upload, journaling and cleanup for the `function` materialization (#180).

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
are. `find_unused_artifacts` finds the candidates for an explicit, manual cleanup instead.

Every dbt-created artifact carries a `managed_by` marker naming the dbt project that uploaded
it. An artifact without it, or from another project, is never deleted by dbt.
"""

import hashlib
import json
from collections.abc import Callable
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
    for artifact in conn.get_artifacts_api().list_artifacts(runtime_language=runtime_language):
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
            return conn.get_artifacts_api().create(
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
        return conn.get_artifacts_api().update(
            existing.id, description=encode_description(udf["project"], journal)
        )
    return existing


def read_journal(conn: Connection, artifact_id: str) -> Journal | None:
    """The journal on an artifact, or None (no journal, not managed, or artifact gone)."""
    try:
        return decode_description(conn.get_artifacts_api().get(artifact_id).description)[1]
    except ArtifactNotFoundError:
        return None


def settle_journal(conn: Connection, artifact_id: str) -> None:
    """Finish a function change by clearing its journal.

    Call only once the function exists on `artifact_id`. If this raises, the journal is left
    for the next run to retry.
    """
    try:
        artifact = conn.get_artifacts_api().get(artifact_id)
    except ArtifactNotFoundError:
        return
    managed, journal = decode_description(artifact.description)
    if managed and journal is not None:
        conn.get_artifacts_api().update(
            artifact_id,
            description=encode_description(artifact_project(artifact.description), None),
        )


def find_unused_artifacts(
    conn: Connection, project: str, used_artifact_ids: Callable[[], set[str]]
) -> tuple[list[FlinkArtifact], list[FlinkArtifact]]:
    """This dbt `project`'s artifacts that no function uses: (orphans, unsettled).

    *Unsettled* artifacts still carry a journal. That is a replacement that never finished, or
    one running right now (its artifact exists before its function does), so they are reported
    apart from the orphans. Artifacts dbt didn't upload for `project` are ignored.

    Artifacts are listed *before* functions are looked up (`used_artifact_ids`), so a function
    created in between can't be missed. Unused by a function doesn't mean unused: statements
    may still be bound to an artifact, which is why deleting is left to the user.
    """
    ours = [
        a
        for a in conn.list_artifacts()
        if decode_description(a.description)[0] and artifact_project(a.description) == project
    ]
    if not ours:
        return [], []
    used = used_artifact_ids()
    unused = [a for a in ours if a.id not in used]
    unsettled = [a for a in unused if decode_description(a.description)[1] is not None]
    return [a for a in unused if a not in unsettled], unsettled
