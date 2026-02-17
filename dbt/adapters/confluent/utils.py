import logging
import time

import agate
from confluent_sql import Cursor
from confluent_sql.statement import Op

logger = logging.getLogger(__name__)


def fetchone_with_retry(cursor, attempts=3, interval=3):
    res = cursor.fetchone()
    if not res and cursor.may_have_results and attempts > 0:
        time.sleep(interval)
        res = fetchone_with_retry(cursor, attempts=attempts - 1)
    return res


def fetchmany_with_retry(cursor, limit=None, attempts=3, interval=3):
    rows = cursor.fetchmany(limit)
    if not rows and cursor.may_have_results and attempts > 0:
        time.sleep(interval)
        rows = fetchmany_with_retry(cursor, limit, attempts=attempts - 1, interval=interval)
    return rows


def fetchall_with_retry(cursor, attempts=3, interval=3):
    if cursor.statement.is_bounded:
        return cursor.fetchall()
    else:
        # Try to fetch a high number of results.
        # This is not exactly correct, but should
        # cover our use case
        logger.warning(
            "Trying to call fetchall on an unbounded statement. Using fetchmany(1000) instead"
        )
        return fetchmany_with_retry(cursor, 1000)


def fetch_from_cursor(cursor: Cursor, limit: int | None) -> agate.Table:
    if limit:
        rows = fetchmany_with_retry(cursor, limit)
    else:
        rows = fetchall_with_retry(cursor)
    if cursor.returns_changelog and rows:
        rows = compact_changelog_results(rows)
    return rows


def compact_changelog_results(rows: list) -> list:
    results = []
    for row in rows:
        if row.op is Op.INSERT:
            results.append(row.row)
        elif row.op is Op.DELETE:
            results.remove(row.row)
        elif row.op is Op.UPDATE_BEFORE:
            index = results.index(row.row)
        elif row.op is Op.UPDATE_AFTER:
            assert index is not None, (
                "Received update_after without an update_before, this is probably a bug"
            )
            results[index] = row.row
            index = None
    return results
