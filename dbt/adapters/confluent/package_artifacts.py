"""What a dbt invocation knows about its functions' package artifacts (#180).

Every thread building a `function` shares one `PackageArtifacts`, so a package is hashed once,
the artifacts are listed once and a package is uploaded at most once, however many functions
(and threads) use it. Each result, a failure included, is shared by every caller (see
`singleflight`); the find-or-upload code itself (`function_artifacts`) stays free of locks.
"""

from os import getcwd

from pathlib import Path

from confluent_sql import Connection, FlinkArtifact

from dbt.adapters.events.logging import AdapterLogger

from . import function_artifacts
from .function_artifacts import ArtifactDigest, FlinkArtifactParams
from .singleflight import SingleFlightCell, SingleFlightDict

logger = AdapterLogger("Confluent")

# How long a waiting thread lets another thread compute before giving up. Backstops, not bounds:
# an upload is a find, a presign, a transfer (with connect retries) and a register, so it gets
# twice the driver's timeout for the transfer alone.
DIGEST_TIMEOUT_SECS = 300.0
LISTING_TIMEOUT_SECS = 300.0
UPLOAD_TIMEOUT_SECS = 2 * function_artifacts.UPLOAD_TIMEOUT_SECS


class PackageArtifacts:
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
        # Artifacts this build uploaded, by name. Written once per name, by that name's upload
        # leader, so it needs no lock.
        self._uploaded: dict[str, FlinkArtifact] = {}

    def digest(self, path: Path) -> ArtifactDigest:
        """Digest of the package file's content, computed once per real path."""

        def hash_file() -> ArtifactDigest:
            # Logged only where the work happens, so one line per package however many callers.
            logger.info(f"SingleShot: Hashing package {path.relative_to(getcwd())}")
            return function_artifacts.file_digest(path)

        return self._digests.get(path.resolve(), hash_file)

    def find(self, conn: Connection, name: str) -> FlinkArtifact | None:
        """Read-only lookup for the plan step: the listing fetched once for this build, plus
        anything this build has uploaded since."""
        uploaded = self._uploaded.get(name)
        if uploaded is not None:
            return uploaded
        # A name identifies one artifact whatever its language, so the listing needs no filter.
        listing = self._listing.get(lambda: self._list_artifacts(conn))
        for artifact in listing:
            if artifact.display_name == name:
                return artifact
        return None

    def _list_artifacts(self, conn: Connection) -> list[FlinkArtifact]:
        logger.info("SingleShot: Listing artifacts")
        return conn.get_artifacts_api().list()

    def ensure(self, conn: Connection, params: FlinkArtifactParams) -> FlinkArtifact:
        """The artifact for this package, found or uploaded once per package content."""
        return self._uploads.get(params.digest, lambda: self._ensure_and_record(conn, params))

    def _ensure_and_record(self, conn: Connection, params: FlinkArtifactParams) -> FlinkArtifact:
        logger.info(f"SingleShot: Finding or uploading artifact with digest={params.digest} for package {params.path.relative_to(getcwd())}")
        artifact = function_artifacts.ensure_artifact(conn, params)
        self._uploaded[params.name] = artifact
        return artifact
