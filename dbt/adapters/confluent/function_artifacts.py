"""Artifact upload and sweeping for the `function` materialization (#180).

A function can name a local package (`artifact_path`) instead of an already-uploaded artifact.
The artifact is named `<project>-<package file name>-<key hash>` (see `artifact_display_name`)
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
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import NamedTuple, NewType

from confluent_sql import ArtifactContentFormat, ArtifactRuntimeLanguage, Connection, FlinkArtifact
from dbt_common.exceptions import CompilationError

from dbt.adapters.events.logging import AdapterLogger

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

# Characters of the key's hash that end an artifact name (the description has the whole key).
_NAME_HASH_LENGTH = 8

# Characters kept from a package file's name when it goes into an artifact name.
_ILLEGAL_PACKAGE_CHARS = re.compile(r"[^A-Za-z0-9_-]")

# The artifact API's limit on a display name (the driver rejects longer ones).
MAX_ARTIFACT_NAME_LENGTH = 60

# The artifact API's limit on a description (documented; the server doesn't enforce it). Unlike
# the name, the description can't be cut to fit: it is the key lookup parses.
MAX_ARTIFACT_DESCRIPTION_LENGTH = 256

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


class ArtifactKey(NamedTuple):
    """What identifies an uploaded package: the project, the file's name and its content.

    Recorded in the artifact's description, so artifacts are found by it and not by their name,
    whose layout is free to change.
    """

    project: str
    file_name: str
    sha256: str


def encode_description(key: ArtifactKey) -> str:
    """The description of the artifact dbt uploaded for `key`: its project, package file name
    and full content digest.

    Carries everything the name may lose (it can be truncated, and its layout may change), so
    the description is the stable record of what an artifact is.

    Compact JSON, to leave as much of `MAX_ARTIFACT_DESCRIPTION_LENGTH` as possible for the
    project and file name; see `check_description_length`.
    """
    return json.dumps(
        {"managed_by": MANAGED_BY, **key._asdict()},
        separators=(",", ":"),
    )


def check_description_length(key: ArtifactKey) -> None:
    """Raise a CompilationError if the description for `key` would exceed the API's limit.

    The description can't be truncated, since lookup and ownership read the key back from it, so
    a project or file name too long to fit is rejected before upload instead.
    """
    length = len(encode_description(key))
    if length > MAX_ARTIFACT_DESCRIPTION_LENGTH:
        raise CompilationError(
            f"Cannot upload package {key.file_name!r}: the artifact description recording its "
            f"project ({key.project!r}) and file name would be {length} characters, over the "
            f"artifact API's limit of {MAX_ARTIFACT_DESCRIPTION_LENGTH}. Shorten the file "
            "name or the dbt project name."
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


def artifact_key(description: str | None) -> ArtifactKey | None:
    """The `ArtifactKey` in an artifact's `description`, else None if it isn't dbt's or is
    missing a field."""
    marker = _managed_description(description)
    if marker is None:
        return None
    project, file_name, sha256 = (marker.get(f) for f in ArtifactKey._fields)
    if not (isinstance(project, str) and isinstance(file_name, str) and isinstance(sha256, str)):
        return None
    return ArtifactKey(project, file_name, sha256)


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


def artifact_display_name(key: ArtifactKey) -> str:
    """Name an artifact `<project>-<package>-<hash>` so a person can tell which package it is.

    `package` is the file's name without its suffix, with anything but letters, digits, `_` and
    `-` (e.g. the dots of a version) replaced by `_`, since the API's allowed characters are not
    documented. The label (`<project>-<package>`) is cut to leave room for the hash, so only the
    start of a long project or file name shows.

    `hash` is the start of a SHA-256 of the whole key (project, file name with its suffix, and
    content digest), so the name is as unique as the key and never depends on the label: names
    that sanitize or cut to the same label (`udfs-1.0.zip` and `udfs-1_0.zip`) still differ.
    The name must be unique per environment, which is what makes an upload under another
    project's name fail rather than overwrite. Ownership never depends on the name: it is the
    `project` in the description (`artifact_project`).
    """
    # NUL can't appear in any field, so no two keys join to the same text.
    key_hash = hashlib.sha256("\0".join(key).encode()).hexdigest()[:_NAME_HASH_LENGTH]
    package = _ILLEGAL_PACKAGE_CHARS.sub("_", Path(key.file_name).stem)
    max_label = MAX_ARTIFACT_NAME_LENGTH - len(key_hash) - 1  # 1 for the "-"
    return f"{key.project}-{package}"[:max_label] + f"-{key_hash}"


def artifact_ids_in(external_artifacts: str) -> set[str]:
    """The artifact IDs in an `INFORMATION_SCHEMA.ROUTINES.EXTERNAL_ARTIFACTS` value, which looks
    like `confluent-artifact://cfa-xxxxxx/latest`."""
    pattern = rf"confluent-artifact://({ARTIFACT_ID_PATTERN})"
    return set(re.findall(pattern, external_artifacts))


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


class UnusedArtifact(NamedTuple):
    artifact: FlinkArtifact
    # Why deleting it failed; None if it was deleted.
    error: str | None

    @property
    def label(self) -> str:
        return artifact_label(self.artifact)


class UnusedScan(NamedTuple):
    unused: list[FlinkArtifact]
    # Candidates that were left alone, if there were any.
    skipped: list[SkippedCandidate]


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


class ArtifactSweeper:
    """Lists, and optionally deletes, the artifacts of one dbt `project` that no function in
    `database` uses.

    `schema` is the one being built into, and `project_root` is what a `list_file` is relative
    to.
    """

    def __init__(
        self,
        conn: Connection,
        project: str,
        database: str,
        schema: str | None,
        project_root: str,
    ) -> None:
        self._conn = conn
        self._project = project
        self._database = database
        self._schema = schema
        self._project_root = project_root

    def write_listing(self, target_path: str, list_file: str | None = None) -> None:
        """The `list_function_artifacts` run-operation: write this project's artifacts, and the
        functions using each, to `list_file` (see `render_listing`).

        `list_file` is relative to the project root; by default it is in `target_path`. Passed to
        `sweep` as `list_file`, it limits the sweep to what existed now.
        """
        list_file = list_file or str(Path(target_path) / DEFAULT_LISTING_FILE)
        path = Path(self._project_root) / list_file
        scan = self._scan_project_artifacts()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_listing(self._project, scan.artifacts))
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
            "To delete any unused artifacts from this list: dbt run-operation "
            f"sweep_function_artifacts --args '{{list_file: {list_file}}}'"
        )

    def sweep(self, delete: bool = False, list_file: str | None = None) -> None:
        """The `sweep_function_artifacts` run-operation: report this project's unused artifacts
        (see `_unused_artifacts`), deleting them with `delete`, and fail at the end if any
        could not be deleted. With `list_file` (see `write_listing`), only the artifacts in it
        are considered, and what became of each is reported. `delete` must be a boolean and
        `list_file` a non-empty string or `None`: the macro forwards `--args` unchanged, so a
        quoted `"false"` would otherwise be truthy and delete, and an empty or falsy `list_file`
        would drop the restriction to the listed artifacts."""
        if not isinstance(delete, bool):
            raise CompilationError(
                f"delete must be true or false, not {delete!r}; pass it unquoted: "
                "--args '{delete: true}'"
            )
        if list_file is not None and (not isinstance(list_file, str) or not list_file):
            raise CompilationError(
                f"list_file must be a non-empty file path, not {list_file!r}; "
                "leave it out to consider every unused artifact"
            )
        candidates = self._read_candidates(list_file) if list_file is not None else None
        scan = self._unused_artifacts(candidates)
        for skipped in scan.skipped:
            logger.info(f"Skipped: {skipped.label}: {skipped.reason}")
        if not scan.unused:
            logger.info(
                "No unused artifacts of this project."
                if candidates is None
                else "None of the candidate artifacts is unused."
            )
            return

        if delete:
            results = self._delete_artifacts(scan.unused)
            for result in results:
                if result.error is None:
                    logger.info(f"Deleted: {result.label}")
            if failed := [f"{r.label}: {r.error}" for r in results if r.error is not None]:
                raise CompilationError(f"Could not delete: {'; '.join(failed)}")
        else:
            for artifact in scan.unused:
                logger.info(f"Unused: {artifact_label(artifact)}")
            args = "delete: true"
            if list_file is not None:
                args += f", list_file: {list_file}"
            logger.info(
                "To delete the unused artifacts, once no statement depends on them: "
                f"dbt run-operation sweep_function_artifacts --args '{{{args}}}'"
            )

    def _read_candidates(self, file: str) -> list[str]:
        """The artifact IDs in a `list_file`, failing on anything that isn't a `render_listing`
        file for this project, so that a bad file never turns into "everything is a
        candidate"."""
        path = Path(self._project_root) / file
        try:
            return parse_listing(path.read_text(), self._project)
        except OSError as e:
            raise CompilationError(f"Can't read list_file {path}: {e}") from e
        except ListingError as e:
            raise CompilationError(f"list_file {path} {e}") from e

    def _unused_artifacts(self, candidates: Sequence[str] | None = None) -> UnusedScan:
        """This project's artifacts that no function in the database uses. Read-only. With
        `candidates` (artifact IDs), only those are considered, and what became of each is
        reported. Functions are named in the schema being built into, if any, without it.

        Artifacts dbt didn't upload for the project are ignored. An artifact a run uploaded but
        has not yet created its function from looks unused too, so a sweep shouldn't run
        alongside a `dbt run`. Candidates narrow that to artifacts that existed when they were
        listed, but don't remove it: the artifact API doesn't refuse to delete an artifact a
        function uses, so only the functions looked up just before deleting guard against that.

        Unused by a function doesn't mean unused: statements may still be bound to an artifact,
        which is why deleting is left to the user, on their word that statements using
        an old artifact were recreated. They stay bound to it, and fail to restart once it is
        gone.
        """
        scan = self._scan_project_artifacts(candidates)
        skipped = list(scan.skipped)
        unused: list[FlinkArtifact] = []
        for entry in scan.artifacts:
            if not entry.functions:
                unused.append(entry.artifact)
            elif candidates is not None:
                functions = [
                    _unqualified(f, self._database, self._schema) for f in entry.functions
                ]
                skipped.append(
                    SkippedCandidate(entry.label, f"still used by {_first_and_count(functions)}")
                )
        return UnusedScan(unused, skipped)

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

    def _scan_project_artifacts(self, candidates: Sequence[str] | None = None) -> ProjectScan:
        """This project's artifacts, with the functions in the database that use each.

        Artifacts dbt didn't upload for the project are ignored. With `candidates` (artifact
        IDs), only those are scanned, and any that aren't this project's are reported as skipped.

        The artifacts are listed before the functions, so a function created in between can't
        leave its artifact looking unused.
        """
        listed = self._conn.get_artifacts_api().list()
        ours = [a for a in listed if artifact_project(a.description) == self._project]
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
                        SkippedCandidate(
                            artifact_label(other), "not uploaded by dbt for this project"
                        )
                    )
            wanted = set(candidates)
            ours = [a for a in ours if a.id in wanted]
        if not ours:
            return ProjectScan([], skipped)
        users = self._functions_by_artifact()
        return ProjectScan([ProjectArtifact(a, users.get(a.id, [])) for a in ours], skipped)

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
