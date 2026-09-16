"""Unit tests for dbt.adapters.confluent.utils — fetch helpers.

All tests use mock cursors so no live connection is required.
"""

import pytest

from dbt.adapters.confluent.utils import (
    fetch_from_cursor,
    fetchall_with_retry,
    fetchmany_with_retry,
)


@pytest.fixture
def bounded_cursor(mocker):
    """Cursor for a bounded (finite) non-changelog statement."""

    def _make(rows):
        c = mocker.MagicMock()
        c.returns_changelog = False
        c.statement.is_bounded = True
        c.may_have_results = False
        c.fetchall.return_value = rows
        c.fetchmany.return_value = rows
        return c

    return _make


@pytest.fixture
def unbounded_cursor(mocker):
    """Cursor for an unbounded non-changelog statement.

    `batches` is a list of lists; each call to fetchmany returns the next batch.
    `may_have_results_seq` controls the `may_have_results` flag after each call;
    defaults to [True, True, …, False] so the loop naturally terminates.
    """

    def _make(batches, *, may_have_results_seq=None):
        c = mocker.MagicMock()
        c.returns_changelog = False
        c.statement.is_bounded = False
        c.fetchmany.side_effect = batches
        seq = may_have_results_seq or [True] * (len(batches) - 1) + [False]
        values = iter(seq)
        type(c).may_have_results = property(lambda self: next(values))
        return c

    return _make


@pytest.fixture
def changelog_cursor(mocker):
    """Cursor for a changelog (non-append-only) statement."""

    def _make(snapshots, *, may_have_results_seq=None):
        c = mocker.MagicMock()
        c.returns_changelog = True
        compressor = mocker.MagicMock()
        compressor.get_current_snapshot.side_effect = snapshots
        c.changelog_compressor.return_value = compressor
        seq = may_have_results_seq or [True] * (len(snapshots) - 1) + [False]
        values = iter(seq)
        type(c).may_have_results = property(lambda self: next(values))
        return c

    return _make


# ---------------------------------------------------------------------------
# fetchmany_with_retry — bounded
# ---------------------------------------------------------------------------


class TestFetchmanyWithRetryBounded:
    def test_bounded_returns_fetchmany_result_immediately(self, bounded_cursor):
        rows = [("a",), ("b",)]
        c = bounded_cursor(rows)
        assert fetchmany_with_retry(c, limit=10) == rows
        c.fetchmany.assert_called_once_with(10)

    def test_bounded_uses_limit(self, bounded_cursor):
        c = bounded_cursor([("x",)])
        fetchmany_with_retry(c, limit=5)
        c.fetchmany.assert_called_once_with(5)


# ---------------------------------------------------------------------------
# fetchmany_with_retry — unbounded
# ---------------------------------------------------------------------------


class TestFetchmanyWithRetryUnbounded:
    def test_returns_when_limit_reached(self, unbounded_cursor):
        # Two batches of 3 each → limit is 5; second call fills to 5+1 but
        # the code checks >= limit so 6 > 5 → breaks.
        c = unbounded_cursor([[1, 2, 3], [4, 5, 6]])
        result = fetchmany_with_retry(c, limit=5, attempts=4, interval=0)
        assert len(result) >= 5

    def test_returns_when_no_more_results(self, unbounded_cursor):
        # Single batch of 2; after that may_have_results is False → stop.
        c = unbounded_cursor([[1, 2]])
        result = fetchmany_with_retry(c, limit=100, attempts=4, interval=0)
        assert result == [1, 2]

    def test_sleeps_between_retries(self, mocker, unbounded_cursor):
        # Returns empty batch on first try, real batch on second.
        c = unbounded_cursor(
            [[], [1, 2, 3]],
            may_have_results_seq=[True, True],
        )

        mock_sleep = mocker.patch("dbt.adapters.confluent.utils.time.sleep")
        result = fetchmany_with_retry(c, limit=3, attempts=2, interval=0.01)

        mock_sleep.assert_called_once_with(0.01)
        assert result == [1, 2, 3]


# ---------------------------------------------------------------------------
# fetchmany_with_retry — changelog
# ---------------------------------------------------------------------------


class TestFetchmanyWithRetryChangelog:
    def test_returns_snapshot_when_limit_reached(self, mocker, changelog_cursor):
        rows = list(range(10))
        c = changelog_cursor([rows], may_have_results_seq=[False])

        mock_logger = mocker.patch("dbt.adapters.confluent.utils.logger")
        result = fetchmany_with_retry(c, limit=5, attempts=4, interval=0)

        assert result == rows
        mock_logger.warning.assert_called_once()

    def test_retries_until_no_more_results(self, mocker, changelog_cursor):
        # Return small snapshot each time; may_have_results → False after 2nd
        c = changelog_cursor([[1], [1]])

        mocker.patch("dbt.adapters.confluent.utils.logger")
        mock_sleep = mocker.patch("dbt.adapters.confluent.utils.time.sleep")
        fetchmany_with_retry(c, limit=100, attempts=4, interval=0.1)

        mock_sleep.assert_called_once_with(0.1)


# ---------------------------------------------------------------------------
# fetchall_with_retry — bounded
# ---------------------------------------------------------------------------


class TestFetchallWithRetryBounded:
    def test_bounded_uses_fetchall(self, bounded_cursor):
        rows = [("a",), ("b",), ("c",)]
        c = bounded_cursor(rows)
        assert fetchall_with_retry(c) == rows
        c.fetchall.assert_called_once()

    def test_bounded_does_not_call_fetchmany(self, bounded_cursor):
        c = bounded_cursor([])
        fetchall_with_retry(c)
        c.fetchmany.assert_not_called()


# ---------------------------------------------------------------------------
# fetchall_with_retry — unbounded
# ---------------------------------------------------------------------------


class TestFetchallWithRetryUnbounded:
    def test_unbounded_warns_and_uses_fetchmany_1000(self, mocker, unbounded_cursor):
        c = unbounded_cursor([[1, 2]])

        mock_logger = mocker.patch("dbt.adapters.confluent.utils.logger")
        result = fetchall_with_retry(c)

        mock_logger.warning.assert_called_once()
        assert result == [1, 2]


# ---------------------------------------------------------------------------
# fetchall_with_retry — changelog
# ---------------------------------------------------------------------------


class TestFetchallWithRetryChangelog:
    def test_returns_non_empty_snapshot_immediately(self, changelog_cursor):
        rows = [("r1",), ("r2",)]
        c = changelog_cursor([rows], may_have_results_seq=[True])

        result = fetchall_with_retry(c)
        assert result == rows
        c.changelog_compressor.return_value.get_current_snapshot.assert_called_once_with(10)

    def test_retries_on_empty_snapshot(self, mocker, changelog_cursor):
        c = changelog_cursor([[], [("row",)]], may_have_results_seq=[True, True])

        mock_sleep = mocker.patch("dbt.adapters.confluent.utils.time.sleep")
        result = fetchall_with_retry(c, attempts=4, interval=0.05)

        mock_sleep.assert_called_once_with(0.05)
        assert result == [("row",)]

    def test_stops_when_no_more_results(self, changelog_cursor):
        c = changelog_cursor([[]], may_have_results_seq=[False])

        result = fetchall_with_retry(c, attempts=4, interval=0)
        assert result == []
        c.changelog_compressor.return_value.get_current_snapshot.assert_called_once()


# ---------------------------------------------------------------------------
# fetch_from_cursor
# ---------------------------------------------------------------------------


class TestFetchFromCursor:
    def test_none_limit_calls_fetchall(self, bounded_cursor):
        rows = [1, 2, 3]
        c = bounded_cursor(rows)
        result = fetch_from_cursor(c, limit=None)
        assert result == rows
        c.fetchall.assert_called_once()

    def test_with_limit_calls_fetchmany(self, bounded_cursor):
        rows = [1, 2]
        c = bounded_cursor(rows)
        result = fetch_from_cursor(c, limit=2)
        assert result == rows
        c.fetchmany.assert_called_once_with(2)

    def test_forwards_attempts_and_interval(self, mocker, bounded_cursor):
        c = bounded_cursor([])
        mock_fa = mocker.patch("dbt.adapters.confluent.utils.fetchall_with_retry", return_value=[])
        fetch_from_cursor(c, limit=None, attempts=7, interval=3)
        mock_fa.assert_called_once_with(c, 7, 3)

    def test_forwards_attempts_and_interval_to_fetchmany(self, mocker, bounded_cursor):
        c = bounded_cursor([])
        mock_fm = mocker.patch(
            "dbt.adapters.confluent.utils.fetchmany_with_retry", return_value=[]
        )
        fetch_from_cursor(c, limit=10, attempts=7, interval=3)
        mock_fm.assert_called_once_with(c, 10, 7, 3)
