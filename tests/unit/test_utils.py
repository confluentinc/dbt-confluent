"""Unit tests for dbt.adapters.confluent.utils — fetch helpers.

All tests use mock cursors so no live connection is required.
"""

from unittest.mock import MagicMock, call, patch

import pytest

from dbt.adapters.confluent.utils import (
    fetch_from_cursor,
    fetchall_with_retry,
    fetchmany_with_retry,
)


def _bounded_cursor(rows):
    """Cursor for a bounded (finite) non-changelog statement."""
    c = MagicMock()
    c.returns_changelog = False
    c.statement.is_bounded = True
    c.may_have_results = False
    c.fetchall.return_value = rows
    c.fetchmany.return_value = rows
    return c


def _unbounded_cursor(batches, *, may_have_results_seq=None):
    """Cursor for an unbounded non-changelog statement.

    `batches` is a list of lists; each call to fetchmany returns the next batch.
    `may_have_results_seq` controls the `may_have_results` flag after each call;
    defaults to [True, True, …, False] so the loop naturally terminates.
    """
    c = MagicMock()
    c.returns_changelog = False
    c.statement.is_bounded = False
    c.fetchmany.side_effect = batches
    if may_have_results_seq is None:
        # Exhaust after all batches have been consumed.
        may_have_results_seq = [True] * (len(batches) - 1) + [False]
    c.may_have_results_values = iter(may_have_results_seq)
    type(c).may_have_results = property(
        lambda self: next(self.may_have_results_values)
    )
    return c


def _changelog_cursor(snapshots, *, may_have_results_seq=None):
    """Cursor for a changelog (non-append-only) statement."""
    c = MagicMock()
    c.returns_changelog = True
    compressor = MagicMock()
    compressor.get_current_snapshot.side_effect = snapshots
    c.changelog_compressor.return_value = compressor
    if may_have_results_seq is None:
        may_have_results_seq = [True] * (len(snapshots) - 1) + [False]
    c.may_have_results_values = iter(may_have_results_seq)
    type(c).may_have_results = property(
        lambda self: next(self.may_have_results_values)
    )
    return c


# ---------------------------------------------------------------------------
# fetchmany_with_retry — bounded
# ---------------------------------------------------------------------------

class TestFetchmanyWithRetryBounded:
    def test_bounded_returns_fetchmany_result_immediately(self):
        rows = [("a",), ("b",)]
        c = _bounded_cursor(rows)
        assert fetchmany_with_retry(c, limit=10) == rows
        c.fetchmany.assert_called_once_with(10)

    def test_bounded_uses_limit(self):
        c = _bounded_cursor([("x",)])
        fetchmany_with_retry(c, limit=5)
        c.fetchmany.assert_called_once_with(5)


# ---------------------------------------------------------------------------
# fetchmany_with_retry — unbounded
# ---------------------------------------------------------------------------

class TestFetchmanyWithRetryUnbounded:
    def test_returns_when_limit_reached(self):
        # Two batches of 3 each → limit is 5; second call fills to 5+1 but
        # the code checks >= limit so 6 > 5 → breaks.
        c = MagicMock()
        c.returns_changelog = False
        c.statement.is_bounded = False
        c.fetchmany.side_effect = [[1, 2, 3], [4, 5, 6]]
        c.may_have_results = True
        result = fetchmany_with_retry(c, limit=5, attempts=4, interval=0)
        assert len(result) >= 5

    def test_returns_when_no_more_results(self):
        # Single batch of 2; after that may_have_results is False → stop.
        c = MagicMock()
        c.returns_changelog = False
        c.statement.is_bounded = False
        c.fetchmany.return_value = [1, 2]
        # First property access returns False so loop exits after one iteration.
        c.may_have_results = False
        result = fetchmany_with_retry(c, limit=100, attempts=4, interval=0)
        assert result == [1, 2]

    def test_sleeps_between_retries(self):
        # Returns empty batch on first try, real batch on second.
        c = MagicMock()
        c.returns_changelog = False
        c.statement.is_bounded = False

        call_count = 0

        def fetchmany_side(n):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return []
            return [1, 2, 3]

        c.fetchmany.side_effect = fetchmany_side
        c.may_have_results = True  # always True so it keeps retrying

        with patch("dbt.adapters.confluent.utils.time.sleep") as mock_sleep:
            result = fetchmany_with_retry(c, limit=3, attempts=2, interval=0.01)

        mock_sleep.assert_called_once_with(0.01)
        assert result == [1, 2, 3]


# ---------------------------------------------------------------------------
# fetchmany_with_retry — changelog
# ---------------------------------------------------------------------------

class TestFetchmanyWithRetryChangelog:
    def test_returns_snapshot_when_limit_reached(self):
        rows = list(range(10))
        c = MagicMock()
        c.returns_changelog = True
        compressor = MagicMock()
        compressor.get_current_snapshot.return_value = rows
        c.changelog_compressor.return_value = compressor
        c.may_have_results = False  # stops after first check

        with patch("dbt.adapters.confluent.utils.logger") as mock_logger:
            result = fetchmany_with_retry(c, limit=5, attempts=4, interval=0)

        assert result == rows
        mock_logger.warning.assert_called_once()

    def test_retries_until_no_more_results(self):
        c = MagicMock()
        c.returns_changelog = True
        compressor = MagicMock()
        # Return small snapshot each time; may_have_results → False after 2nd
        compressor.get_current_snapshot.return_value = [1]
        c.changelog_compressor.return_value = compressor

        call_no = 0

        def may_have():
            nonlocal call_no
            call_no += 1
            return call_no < 2  # False on 2nd access → exits loop

        type(c).may_have_results = property(lambda self: may_have())

        with patch("dbt.adapters.confluent.utils.logger"):
            with patch("dbt.adapters.confluent.utils.time.sleep") as mock_sleep:
                fetchmany_with_retry(c, limit=100, attempts=4, interval=0.1)

        mock_sleep.assert_called_once_with(0.1)


# ---------------------------------------------------------------------------
# fetchall_with_retry — bounded
# ---------------------------------------------------------------------------

class TestFetchallWithRetryBounded:
    def test_bounded_uses_fetchall(self):
        rows = [("a",), ("b",), ("c",)]
        c = _bounded_cursor(rows)
        assert fetchall_with_retry(c) == rows
        c.fetchall.assert_called_once()

    def test_bounded_does_not_call_fetchmany(self):
        c = _bounded_cursor([])
        fetchall_with_retry(c)
        c.fetchmany.assert_not_called()


# ---------------------------------------------------------------------------
# fetchall_with_retry — unbounded
# ---------------------------------------------------------------------------

class TestFetchallWithRetryUnbounded:
    def test_unbounded_warns_and_uses_fetchmany_1000(self):
        c = MagicMock()
        c.returns_changelog = False
        c.statement.is_bounded = False
        c.fetchmany.return_value = [1, 2]
        c.may_have_results = False

        with patch("dbt.adapters.confluent.utils.logger") as mock_logger:
            result = fetchall_with_retry(c)

        mock_logger.warning.assert_called_once()
        assert result == [1, 2]


# ---------------------------------------------------------------------------
# fetchall_with_retry — changelog
# ---------------------------------------------------------------------------

class TestFetchallWithRetryChangelog:
    def test_returns_non_empty_snapshot_immediately(self):
        rows = [("r1",), ("r2",)]
        c = MagicMock()
        c.returns_changelog = True
        compressor = MagicMock()
        compressor.get_current_snapshot.return_value = rows
        c.changelog_compressor.return_value = compressor
        c.may_have_results = True  # still True, but early-break triggered

        result = fetchall_with_retry(c)
        assert result == rows
        compressor.get_current_snapshot.assert_called_once_with(10)

    def test_retries_on_empty_snapshot(self):
        c = MagicMock()
        c.returns_changelog = True
        compressor = MagicMock()
        call_no = 0

        def get_snapshot(n):
            nonlocal call_no
            call_no += 1
            return [] if call_no == 1 else [("row",)]

        compressor.get_current_snapshot.side_effect = get_snapshot
        c.changelog_compressor.return_value = compressor
        c.may_have_results = True  # keeps retrying

        with patch("dbt.adapters.confluent.utils.time.sleep") as mock_sleep:
            result = fetchall_with_retry(c, attempts=4, interval=0.05)

        mock_sleep.assert_called_once_with(0.05)
        assert result == [("row",)]

    def test_stops_when_no_more_results(self):
        c = MagicMock()
        c.returns_changelog = True
        compressor = MagicMock()
        compressor.get_current_snapshot.return_value = []
        c.changelog_compressor.return_value = compressor
        c.may_have_results = False  # stop immediately

        result = fetchall_with_retry(c, attempts=4, interval=0)
        assert result == []
        compressor.get_current_snapshot.assert_called_once()


# ---------------------------------------------------------------------------
# fetch_from_cursor
# ---------------------------------------------------------------------------

class TestFetchFromCursor:
    def test_none_limit_calls_fetchall(self):
        rows = [1, 2, 3]
        c = _bounded_cursor(rows)
        result = fetch_from_cursor(c, limit=None)
        assert result == rows
        c.fetchall.assert_called_once()

    def test_with_limit_calls_fetchmany(self):
        rows = [1, 2]
        c = _bounded_cursor(rows)
        result = fetch_from_cursor(c, limit=2)
        assert result == rows
        c.fetchmany.assert_called_once_with(2)

    def test_forwards_attempts_and_interval(self):
        c = _bounded_cursor([])
        with patch("dbt.adapters.confluent.utils.fetchall_with_retry") as mock_fa:
            mock_fa.return_value = []
            fetch_from_cursor(c, limit=None, attempts=7, interval=3)
        mock_fa.assert_called_once_with(c, 7, 3)

    def test_forwards_attempts_and_interval_to_fetchmany(self):
        c = _bounded_cursor([])
        with patch("dbt.adapters.confluent.utils.fetchmany_with_retry") as mock_fm:
            mock_fm.return_value = []
            fetch_from_cursor(c, limit=10, attempts=7, interval=3)
        mock_fm.assert_called_once_with(c, 10, 7, 3)
