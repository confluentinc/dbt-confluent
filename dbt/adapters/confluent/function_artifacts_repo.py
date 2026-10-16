"""What a dbt invocation knows about its functions' package artifacts (#180).

Every thread building a `function` shares one `FunctionArtifactsRepo`, so a package is hashed once,
the artifacts are listed once and a package is uploaded at most once, however many functions
(and threads) use it. Each result, a failure included, is shared by every caller (see
`singleflight`). The pure helpers (`function_artifacts`) stay free of locks and of driver calls
that list.
"""

from pathlib import Path

from confluent_sql import ArtifactAlreadyExistsError, Connection, FlinkArtifact, UploadRetry

from dbt.adapters.events.logging import AdapterLogger

from . import function_artifacts
from .function_artifacts import ArtifactConfig, ArtifactDigest
from .singleflight import SingleFlightCell, SingleFlightDict

logger = AdapterLogger("Confluent")

# How long a waiting thread lets another thread compute before giving up. Backstops, not bounds:
# an upload is a find, a presign, a transfer (with connect retries, and re-sent if it fails midway)
# and a register, so it gets the driver's timeout for each transfer attempt plus one more for the
# rest.
DIGEST_TIMEOUT_SECS = 300.0
LISTING_TIMEOUT_SECS = 300.0
UPLOAD_TIMEOUT_SECS = (
    function_artifacts.UPLOAD_TRANSFER_RETRIES + 2
) * function_artifacts.UPLOAD_TIMEOUT_SECS


class FunctionArtifactsRepo:
    """Package digests, the artifact listing and artifact uploads, shared by all of a build's threads.

    One instance per dbt invocation: nothing here is ever evicted, so a package edited mid-build
    is not re-read.
    """

    def __init__(
        self,
        digest_timeout: float = DIGEST_TIMEOUT_SECS,
        listing_timeout: float = LISTING_TIMEOUT_SECS,
        upload_timeout: float = UPLOAD_TIMEOUT_SECS,
    ) -> None:
        self._digests = SingleFlightDict[Path, ArtifactDigest](digest_timeout)
        self._listing = SingleFlightCell[list[FlinkArtifact]](listing_timeout)
        self._uploads = SingleFlightDict[ArtifactDigest, FlinkArtifact](upload_timeout)
        # Artifacts this build uploaded, by name.
        # Written once per name, by that name's upload leader, so it needs no lock.
        self._uploaded_by_name: dict[str, FlinkArtifact] = {}

    def find(self, conn: Connection, name: str) -> FlinkArtifact | None:
        """Read-only lookup for the plan step: the listing fetched once for this build, plus
        anything this build has uploaded since."""
        uploaded = self._uploaded_by_name.get(name)
        if uploaded is not None:
            return uploaded
        # A name identifies one artifact whatever its language, so the listing needs no filter.
        listing = self._listing.get(lambda: self._list_artifacts(conn))
        for artifact in listing:
            if artifact.display_name == name:
                return artifact
        return None

    def ensure(self, conn: Connection, params: ArtifactConfig) -> FlinkArtifact:
        """The artifact for this package, found or uploaded once per package content."""
        digest = self._digest(params.path)
        return self._uploads.get(digest, lambda: self._find_or_upload(conn, digest, params))

    def _find_or_upload(
        self, conn: Connection, digest: ArtifactDigest, config: ArtifactConfig
    ) -> FlinkArtifact:
        logger.debug(f"Finding or uploading artifact {config.name} for package {config.path}")
        artifact = self.find(conn, config.name) or self._upload(conn, config)
        self._uploaded_by_name[config.name] = artifact
        return artifact

    def _upload(self, conn: Connection, params: ArtifactConfig) -> FlinkArtifact:
        _, content_format, runtime_language = function_artifacts.ARTIFACT_FORMATS[params.language]
        try:
            return conn.get_artifacts_api().create(
                params.name,
                file=params.path,
                content_format=content_format,
                runtime_language=runtime_language,
                description=function_artifacts.encode_description(params.project),
                upload_timeout=function_artifacts.UPLOAD_TIMEOUT_SECS,
                upload_retry=UploadRetry(
                    transfer_retries=function_artifacts.UPLOAD_TRANSFER_RETRIES
                ),
            )
        except ArtifactAlreadyExistsError:  # a concurrent run won the race
            # The shared listing predates the winner's upload, so look again, uncached.
            for artifact in self._list_artifacts(conn):
                if artifact.display_name == params.name:
                    return artifact
            raise

    def _digest(self, path: Path) -> ArtifactDigest:
        """Digest of the package file's content, computed once per real path."""

        def hash_file() -> ArtifactDigest:
            # Logged only where the work happens, so one line per package however many callers.
            logger.debug(f"Hashing package {path}")
            return function_artifacts.file_digest(path)

        return self._digests.get(path.resolve(), hash_file)

    def _list_artifacts(self, conn: Connection) -> list[FlinkArtifact]:
        logger.debug("Listing artifacts")
        # No retry here: we depend on the driver's default retry policy for GETs rather than
        # adding another layer of our own. A failure that outlasts it is shared, not retried.
        return conn.get_artifacts_api().list()
