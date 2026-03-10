import logging
import time

import agate
from confluent_sql import Cursor

logger = logging.getLogger(__name__)


def fetchmany_with_retry(cursor, limit, attempts=4, interval=5):
    """Try to fetch one `limit` rows `attempts` times.

    On a streaming cursor, this will return the first batch of results received,
    even if it's less than `limit`.
    You'll need to call this multiple times if you don't get all the results at once.
    """
    results = []
    if cursor.returns_changelog:
        msg = (
            "Calling fetchmany on a non-append-only stream. "
            "Results comes from a snapshot, and they may be partial. "
            "Let us know if this is causing issues!"
        )
        logger.warning(msg)
        compressor = cursor.changelog_compressor()
        for _ in range(attempts):
            results = compressor.get_current_snapshot(limit)
            if len(results) >= limit or not cursor.may_have_results:
                break
            time.sleep(interval)
    else:
        if cursor.statement.is_bounded:
            results = cursor.fetchmany(limit)
        else:
            for _ in range(attempts):
                results += cursor.fetchmany(limit)
                if len(results) >= limit or not cursor.may_have_results:
                    break
                time.sleep(interval)
    return results


def fetchall_with_retry(cursor, attempts=4, interval=5):
    """Try to fetch all rows `attempts` times.

    On a streaming cursor, fetchall won't work, so we revert to call
    fetchmany with a limit of 1000.
    """
    results = []
    if cursor.returns_changelog:
        compressor = cursor.changelog_compressor()
        for _ in range(attempts):
            # Use a batch_size bigger than the default `1`
            results = compressor.get_current_snapshot(10)
            # Early break if we got results to avoid the sleep
            if results or not cursor.may_have_results:
                break
            time.sleep(interval)
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
