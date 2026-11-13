"""What a dbt invocation knows about its functions' package artifacts (#180).

Every thread building a `function` shares one `FunctionArtifactsRepo`, so a package is hashed once,
the artifacts are listed once and a package is uploaded at most once, however many functions
(and threads) use it. Each result, a failure included, is shared by every caller (see
`singleflight`). The pure helpers (`function_artifacts`) stay free of locks.
"""

from pathlib import Path

from confluent_sql import (
    ArtifactAlreadyExistsError,
    ArtifactRuntimeLanguage,
    Connection,
    FlinkArtifact,
    UploadRetry,
)
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.events.logging import AdapterLogger

from . import function_artifacts
from .function_artifacts import ArtifactConfig, ArtifactDigest, ArtifactKey
from .singleflight import SingleFlightCell, SingleFlightDict

logger = AdapterLogger("Confluent")

# How long a waiting thread lets another thread compute before giving up. Backstops, not bounds:
# an upload is a find, a presign, a transfer (with connect retries, and re-sent if it fails midway)
# and a register, so it gets the driver's timeout for each transfer attempt plus one more for the
# rest, and the listing wait for the leader's own lookup first.
DIGEST_TIMEOUT_SECS = 300.0
LISTING_TIMEOUT_SECS = 300.0
UPLOAD_WAIT_TIMEOUT_SECS = (
    LISTING_TIMEOUT_SECS
    + (function_artifacts.UPLOAD_TRANSFER_RETRIES + 2) * function_artifacts.UPLOAD_TIMEOUT_SECS
)


class FunctionArtifactsRepo:
    """Package digests, the artifact listing and artifact uploads, shared by all of a build's threads.

    One instance per dbt invocation: nothing here is ever evicted, so a package edited mid-build
    is not re-read.
    """

    def __init__(
        self,
        digest_timeout: float = DIGEST_TIMEOUT_SECS,
        listing_timeout: float = LISTING_TIMEOUT_SECS,
        upload_timeout: float = UPLOAD_WAIT_TIMEOUT_SECS,
    ) -> None:
        self._digests = SingleFlightDict[Path, ArtifactDigest](digest_timeout)
        self._listing = SingleFlightCell[dict[ArtifactKey, FlinkArtifact]](listing_timeout)
        self._uploads = SingleFlightDict[ArtifactKey, FlinkArtifact](upload_timeout)
        # Artifacts this build uploaded, by key.
        # Written once per key, by that key's upload leader, so it needs no lock.
        self._uploaded_by_key: dict[ArtifactKey, FlinkArtifact] = {}

    def find(self, conn: Connection, key: ArtifactKey) -> FlinkArtifact | None:
        """Read-only lookup for the plan step: the listing fetched once for this build, plus
        anything this build has uploaded since.

        Artifacts are matched on the key in their description, never on their name.
        """
        uploaded = self._uploaded_by_key.get(key)
        if uploaded is not None:
            return uploaded
        # A key identifies one artifact whatever its language, so the listing needs no filter.
        return self._listing.get(lambda: self._list_artifacts(conn)).get(key)

    def ensure(self, conn: Connection, config: ArtifactConfig) -> FlinkArtifact:
        """The artifact for this package, found or uploaded once per artifact key.

        The key has the content digest, so one key is one content. Two files with identical
        bytes but different names are two artifacts, so each traces back to its file.
        """
        artifact = self._uploads.get(config.key, lambda: self._find_or_upload(conn, config))
        # Uploads are keyed on the key alone, so this may be the artifact another function (the
        # leader) uploaded under its own language, or one already there from an earlier run.
        # A single package contain functions in multiple languages, so we validate the mismatch
        # rather than uploading the package again with different metadata. This is unlikely to
        # happen, since we validate the package format upfront, but included here for safety.
        _check_language(artifact, config)
        return artifact

    def _find_or_upload(self, conn: Connection, config: ArtifactConfig) -> FlinkArtifact:
        logger.debug(f"Finding or uploading artifact {config.name} for package {config.path.name}")
        artifact = self.find(conn, config.key) or self._upload(conn, config)
        self._uploaded_by_key[config.key] = artifact
        return artifact

    def _upload(self, conn: Connection, config: ArtifactConfig) -> FlinkArtifact:
        artifact_format = function_artifacts.ARTIFACT_FORMATS[config.language]
        try:
            return conn.get_artifacts_api().create(
                config.name,
                file=config.path,
                content_format=artifact_format.content_format,
                runtime_language=artifact_format.runtime_language,
                description=function_artifacts.encode_description(*config.key),
                upload_timeout=function_artifacts.UPLOAD_TIMEOUT_SECS,
                upload_retry=UploadRetry(
                    transfer_retries=function_artifacts.UPLOAD_TRANSFER_RETRIES
                ),
            )
        except ArtifactAlreadyExistsError as e:
            # Usually a concurrent run that won the race. The shared listing predates its upload,
            # so look again, uncached.
            winner = self._list_artifacts(conn).get(config.key)
            if winner is not None:
                return winner
            # The name is taken by an artifact that isn't this package: one made by hand, one whose
            # description was edited, or (very unlikely) another package whose name hash collides.
            raise DbtDatabaseError(
                f"Cannot upload package {config.path.name} as artifact '{config.name}': an "
                "artifact with that name already exists, but its description doesn't match this "
                f"package (project '{config.project}', file name '{config.path.name}', sha256 "
                f"{config.digest}), so dbt won't reuse it. Delete that artifact if it is no "
                "longer needed, or rename the package file, which gives it a different artifact "
                "name."
            ) from e

    def digest(self, path: Path) -> ArtifactDigest:
        """Digest of the package file's content, computed once per real path."""

        def hash_file() -> ArtifactDigest:
            # Logged only where the work happens, so one line per package however many callers.
            logger.debug(f"Hashing package {path.name}")
            return function_artifacts.file_digest(path)

        return self._digests.get(path.resolve(), hash_file)

    def _list_artifacts(self, conn: Connection) -> dict[ArtifactKey, FlinkArtifact]:
        """The artifacts dbt uploaded, by the key in their description. Others are ignored."""
        logger.debug("Listing artifacts")
        # No retry here: the driver already retries this GET. A failure that outlasts that is
        # cached by `self._listing` and so fails every function in the build.
        artifacts = conn.get_artifacts_api().list()
        return {
            key: a for a in artifacts if (key := function_artifacts.artifact_key(a.description))
        }


def _check_language(artifact: FlinkArtifact, config: ArtifactConfig) -> None:
    """Raise if `artifact` was uploaded under a different language than `config` asks for."""
    expected = function_artifacts.ARTIFACT_FORMATS[config.language].runtime_language
    # The server treats an artifact with no runtime language as Java.
    actual = artifact.runtime_language or ArtifactRuntimeLanguage.JAVA
    if actual != expected:
        raise DbtDatabaseError(
            f"Package {config.path} is already uploaded as artifact '{artifact.display_name}' "
            f"with language {actual.value.lower()}, but a function asks for it as "
            f"{config.language}. A single package cannot be uploaded under two different "
            "languages; give each language its own package."
        )
