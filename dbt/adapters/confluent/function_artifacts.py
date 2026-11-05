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

    @property
    def label(self) -> str:
        return artifact_label(self.artifact)


def artifact_label(artifact: FlinkArtifact) -> str:
    return f"{artifact.id} ({artifact.display_name})"


class SkippedCandidate(NamedTuple):
    """A candidate that was not considered for deletion, and why."""

    label: str
    reason: str


class ProjectScan(NamedTuple):
    # This project's artifacts, only the candidates among them if there are any.
    artifacts: list[ProjectArtifact]
    # Candidates that aren't this project's artifacts.
    skipped: list[SkippedCandidate]


def scan_project_artifacts(
    conn: Connection,
    project: str,
    database: str,
    query_rows: Callable[[str], Iterable[Sequence[Any]]],
    candidates: Sequence[str] | None = None,
) -> ProjectScan:
    """This dbt `project`'s artifacts, with the functions in `database` that use each.

    Artifacts dbt didn't upload for `project` are ignored. With `candidates` (artifact IDs), only
    those are scanned, and any that aren't this project's are reported as skipped.

    The artifacts are listed before the functions, so a function created in between can't leave
    its artifact looking unused. `query_rows` runs a SQL query and returns its rows.
    """
    listed = conn.get_artifacts_api().list()
    ours = [a for a in listed if artifact_project(a.description) == project]
    skipped: list[SkippedCandidate] = []
    if candidates is not None:
        ours_ids = {a.id for a in ours}
        listed_by_id = {a.id: a for a in listed}
        for candidate in candidates:
            if candidate in ours_ids:
                continue
            other = listed_by_id.get(candidate)
            if other is None:
                skipped.append(SkippedCandidate(candidate, "no such artifact"))
            else:
                skipped.append(
                    SkippedCandidate(artifact_label(other), "not uploaded by dbt for this project")
                )
        wanted = set(candidates)
        ours = [a for a in ours if a.id in wanted]
    if not ours:
        return ProjectScan([], skipped)
    users = _functions_by_artifact(database, query_rows)
    return ProjectScan([ProjectArtifact(a, users.get(a.id, [])) for a in ours], skipped)


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


def _unqualified(function: str, database: str, schema: str | None) -> str:
    """A function's `database.schema.name` without what its reader already knows: the database
    (a scan covers just one), and the schema if it is `schema`, the one being built into."""
    for prefix in (f"{database}.{schema}." if schema else None, f"{database}."):
        if prefix and function.startswith(prefix):
            return function[len(prefix) :]
    return function


def _first_and_count(names: Sequence[str]) -> str:
    """The first of `names`, and how many more there are, e.g. `a and 2 others`."""
    others = len(names) - 1
    if others == 0:
        return names[0]
    return f"{names[0]} and {others} other{'' if others == 1 else 's'}"


class SweepResult(NamedTuple):
    unused: list[UnusedArtifact]
    # Candidates that were left alone, if there were any.
    skipped: list[SkippedCandidate]


def scan_unused_artifacts(
    conn: Connection,
    project: str,
    database: str,
    query_rows: Callable[[str], Iterable[Sequence[Any]]],
    delete: bool = False,
    candidates: Sequence[str] | None = None,
    schema: str | None = None,
) -> SweepResult:
    """List this dbt `project`'s artifacts that no function in `database` uses, and delete them
    with `delete`. With `candidates` (artifact IDs), only those are considered, and the outcome
    is reported for each of them. Functions are named in the `schema` being built into, if any,
    without it.

    Artifacts dbt didn't upload for `project` are ignored. An artifact a run uploaded but has
    not yet created its function from looks unused too, so a sweep shouldn't run alongside a
    `dbt run`. Candidates narrow that to artifacts that existed when they were listed, but don't
    remove it: the artifact API doesn't refuse to delete an artifact a function uses, so only
    the functions looked up just before deleting guard against that.

    Unused by a function doesn't mean unused: statements may still be bound to an artifact,
    which is why deleting is left to the user, on their word that statements using
    an old artifact were recreated. They stay bound to it, and fail to restart once it is gone.

    `error` is set if deleting an artifact failed (every one is tried).
    """
    scan = scan_project_artifacts(conn, project, database, query_rows, candidates)
    skipped = list(scan.skipped)
    unused: list[UnusedArtifact] = []
    for entry in scan.artifacts:
        if entry.functions:
            if candidates is not None:
                skipped.append(
                    SkippedCandidate(
                        entry.label,
                        "still used by "
                        + _first_and_count(
                            [_unqualified(f, database, schema) for f in entry.functions]
                        ),
                    )
                )
            continue
        error = None
        if delete:
            try:
                conn.get_artifacts_api().delete(entry.artifact.id)
            except Exception as e:
                error = str(e) or type(e).__name__
        unused.append(UnusedArtifact(entry.artifact, error))
    return SweepResult(unused, skipped)


LISTING_FORMAT_VERSION = 1
DEFAULT_LISTING_FILE = "confluent_function_artifacts.json"


def render_listing(project: str, artifacts: Iterable[ProjectArtifact]) -> str:
    """The `list_function_artifacts` file: this project's artifacts and who uses them."""
    entries = [
        {"id": a.artifact.id, "name": a.artifact.display_name, "referenced_by": a.functions}
        for a in sorted(artifacts, key=lambda a: a.artifact.id)
    ]
    listing = {"format_version": LISTING_FORMAT_VERSION, "project": project, "artifacts": entries}
    return json.dumps(listing, indent=2) + "\n"


class ListingError(ValueError):
    """The candidates file isn't a usable `render_listing` output."""


def parse_listing(text: str, project: str) -> list[str]:
    """The artifact IDs in a `render_listing` file, without duplicates.

    Only the IDs are read: the other fields are for people, and what an artifact is used by is
    looked up again when it is deleted. Unknown keys are ignored.
    """
    try:
        listing = json.loads(text)
    except ValueError as e:
        raise ListingError(f"isn't valid JSON ({e})") from e
    if not isinstance(listing, dict):
        raise ListingError("isn't a JSON object")
    version = listing.get("format_version")
    if version != LISTING_FORMAT_VERSION:
        raise ListingError(
            f"has format_version {version!r}, but only {LISTING_FORMAT_VERSION} is supported"
        )
    if listing.get("project") != project:
        raise ListingError(
            f"is for project {listing.get('project')!r}, not this project ({project!r})"
        )
    entries = listing.get("artifacts")
    if not isinstance(entries, list):
        raise ListingError("has no 'artifacts' list")
    ids: list[str] = []
    for i, entry in enumerate(entries):
        artifact_id = entry.get("id") if isinstance(entry, dict) else None
        if not isinstance(artifact_id, str) or not artifact_id:
            raise ListingError(f"has an artifact (number {i + 1}) without a string 'id'")
        ids.append(artifact_id)
    return list(dict.fromkeys(ids))


def write_listing(
    conn: Connection,
    project: str,
    database: str,
    query_rows: Callable[[str], Iterable[Sequence[Any]]],
    project_root: str,
    target_path: str,
    list_file: str | None = None,
) -> None:
    """The `list_function_artifacts` run-operation: write this project's artifacts, and the
    functions using each, to `list_file` (see `render_listing`).

    `list_file` is relative to `project_root`; by default it is in `target_path`. Passed to
    `sweep_function_artifacts` as `list_file`, it limits the sweep to what existed now.
    """
    list_file = list_file or str(Path(target_path) / DEFAULT_LISTING_FILE)
    path = Path(project_root) / list_file
    scan = scan_project_artifacts(conn, project, database, query_rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_listing(project, scan.artifacts))
    for entry in sorted(scan.artifacts, key=lambda a: a.artifact.id):
        count = len(entry.functions)
        used = f"used by {count} function{'' if count == 1 else 's'}" if count else "unused"
        logger.info(f"{entry.label}: {used}")
    try:
        shown = path.relative_to(Path.cwd())
    except ValueError:
        shown = path
    logger.info(f"Wrote {len(scan.artifacts)} artifact(s) of this project to {shown}")
    logger.info(
        "To see which artifacts would be deleted: dbt run-operation sweep_function_artifacts "
        f"--args '{{list_file: {list_file}}}'"
    )


def read_candidates(project_root: str, file: str, project: str) -> list[str]:
    """The artifact IDs in a `list_file` (relative to `project_root`), failing on anything
    that isn't a `render_listing` file for `project`, so that a bad file never turns into
    "everything is a candidate"."""
    path = Path(project_root) / file
    try:
        return parse_listing(path.read_text(), project)
    except OSError as e:
        raise CompilationError(f"Can't read list_file {path}: {e}") from e
    except ListingError as e:
        raise CompilationError(f"list_file {path} {e}") from e


def sweep(
    conn: Connection,
    project: str,
    database: str,
    query_rows: Callable[[str], Iterable[Sequence[Any]]],
    project_root: str,
    delete: bool = False,
    list_file: str | None = None,
    schema: str | None = None,
) -> None:
    """The `sweep_function_artifacts` run-operation: report this project's unused artifacts
    (see `scan_unused_artifacts`), deleting them with `delete`, and fail at the end if any could
    not be deleted. With `list_file` (see `write_listing`), only the artifacts in it are
    considered, and what became of each is reported. `schema` is the one being built into
    (see `scan_unused_artifacts`). `delete` must be a boolean: the macro forwards `--args`
    unchanged, so a quoted `"false"` would otherwise be truthy and delete."""
    if not isinstance(delete, bool):
        raise CompilationError(
            f"delete must be true or false, not {delete!r}; pass it unquoted: "
            "--args '{delete: true}'"
        )
    candidates = read_candidates(project_root, list_file, project) if list_file else None
    outcome = scan_unused_artifacts(
        conn, project, database, query_rows, delete, candidates, schema
    )
    for skipped in outcome.skipped:
        logger.info(f"Skipped: {skipped.label}: {skipped.reason}")
    if not outcome.unused:
        logger.info(
            "No unused artifacts of this project."
            if candidates is None
            else "None of the candidate artifacts is unused."
        )
        return
    for result in outcome.unused:
        if result.error is None:
            logger.info(f"{'Deleted' if delete else 'Unused'}: {result.label}")
    if not delete:
        args = "delete: true"
        if list_file:
            args += f", list_file: {list_file}"
        logger.info(
            "To delete the unused artifacts, once no statement depends on them: "
            f"dbt run-operation sweep_function_artifacts --args '{{{args}}}'"
        )
    if failed := [f"{r.label}: {r.error}" for r in outcome.unused if r.error is not None]:
        raise CompilationError(f"Could not delete: {'; '.join(failed)}")
