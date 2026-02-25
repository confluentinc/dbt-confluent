import logging
import time

import agate
from confluent_sql import Cursor

logger = logging.getLogger(__name__)


def fetchmany_with_retry(cursor, limit, attempts=3, interval=3):
    """Try to fetch one `limit` rows `attempts` times.

    On a streaming cursor, this will return the first batch of results received,
    even if it's less than `limit`.
    You'll need to call this multiple times if you don't get all the results at once.
    """
    results = []
    retry = attempts
    if cursor.returns_changelog:
        msg = (
            "Calling fetchmany on a non-append-only stream. "
            "Results comes from a snapshot, and they may be partial. "
            "Let us know if this is causing issues!"
        )
        logger.warning(msg)
        compressor = cursor.changelog_compressor()
        snapshots = compressor.snapshots()
        while len(results) < limit and retry > 0:
            results = next(snapshots)
            time.sleep(interval)
            retry -= 1
    else:
        if cursor.statement.is_bounded:
            results = cursor.fetchmany(limit)
        else:
            while len(results) < limit and retry > 0:
                results += cursor.fetchmany(limit)
                time.sleep(interval)
                retry -= 1
    return results


def fetchall_with_retry(cursor, attempts=3, interval=3):
    """Try to fetch all rows `attempts` times.

    On a streaming cursor, fetchall won't work, so we revert to call
    fetchmany with a limit of 1000.
    """
    results = []
    retry = attempts
    if cursor.returns_changelog:
        compressor = cursor.changelog_compressor()
        snapshots = compressor.snapshots()
        while not results and retry > 0:
            results = next(snapshots)
            time.sleep(interval)
            retry -= 1
    else:
        if cursor.statement.is_bounded:
            results = cursor.fetchall()
        else:
            # Try to fetch a high number of results.
            # This is not exactly correct, but should cover our use cases
            logger.warning(
                "Trying to call fetchall on an unbounded statement. Using fetchmany(1000) instead."
            )
            results = fetchmany_with_retry(cursor, 1000)
    return results


def fetch_from_cursor(
    cursor: Cursor, limit: int | None = None, attempts=4, interval=5
) -> agate.Table:
    if limit is None:
        return fetchall_with_retry(cursor, attempts, interval)
    else:
        return fetchmany_with_retry(cursor, limit, attempts, interval)
