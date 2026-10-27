"""Unit tests for the catalog query over a set of relations.

The relations are queried in chunks (`ThrottledChunkReader`). A failed chunk is retried after a
backoff with half the chunk size, the smaller size is kept for the remaining chunks, and the
error is raised once a single-relation chunk has failed repeatedly.
`query_catalog_rows` runs the queries on that schedule, and the adapter delegates to it.
"""

from dataclasses import dataclass, field

import pytest
from confluent_sql import OperationalError
from dbt_common.exceptions import CompilationError, DbtDatabaseError, DbtRuntimeError

from dbt.adapters.confluent.catalog import (
    CATALOG_RELATIONS_CHUNK_SIZE,
    Backoff,
    ThrottledChunkReader,
    is_retryable_backend_error,
    query_catalog_rows,
)
from dbt.adapters.confluent.impl import ConfluentAdapter
from tests.unit._helpers import relation

LARGE = CATALOG_RELATIONS_CHUNK_SIZE
HALF = LARGE // 2
QUARTER = LARGE // 4  # chunk size after two failures
ONE = 1
MAX_ERRORS = 4  # the schedule's `max_consecutive_errors` in these tests
BACKOFF = Backoff(base_delay_secs=1.0, max_delay_secs=10.0, jitter_band=(0.75, 1.25))


def _database_error(http_status_code):
    """What `execute_macro` raises when the backend answers with `http_status_code`: the adapter's
    `DbtDatabaseError` wrapping the driver's `OperationalError`."""
    error = DbtDatabaseError("confluent_sql error for 'macro get_catalog_relations'")
    error.__cause__ = OperationalError(
        f"error sending request '{http_status_code}'", http_status_code=http_status_code
    )
    return error


def _macro_error(http_status_code):
    """What `execute_macro` actually raises: the macro boundary wraps the adapter's
    `DbtDatabaseError` (see `_database_error`) in a `DbtRuntimeError` again."""
    error = DbtRuntimeError("Error running SQL 'macro get_catalog_relations': Database Error")
    error.__cause__ = _database_error(http_status_code)
    return error


# What the backend returns when it gives up on a slow statement.
BACKEND_ERROR = _macro_error(500)


def _relations(count):
    # Zero-padded so that the adapter's sort by `str` keeps creation order.
    return [relation(f"table_{i:03d}") for i in range(count)]


class _Retryable(Exception):
    pass


class _Unlisted(Exception):
    pass


@dataclass
class _Run:
    """What happened when a schedule was driven to completion (or until it gave up)."""

    chunks: list = field(default_factory=list)
    delays: list = field(default_factory=list)
    items: list = field(default_factory=list)
    error: Exception | None = None

    @property
    def sizes(self):
        return [len(chunk) for chunk in self.chunks]

    @property
    def gave_up(self):
        return self.error is not None


@pytest.fixture
def sleep(mocker):
    """A stand-in for `time.sleep` that records the waits instead of making them."""
    return mocker.Mock()


@pytest.fixture
def on_retryable_error(mocker):
    return mocker.Mock()


@pytest.fixture
def make_schedule(sleep, on_retryable_error):
    def make_schedule(relations, max_consecutive_errors=MAX_ERRORS):
        return ThrottledChunkReader(
            relations,
            initial_chunk_size=LARGE,
            backoff=BACKOFF,
            is_retryable=lambda error: isinstance(error, _Retryable),
            max_consecutive_errors=max_consecutive_errors,
            on_retryable_error=on_retryable_error,
            sleep=sleep,
        )

    return make_schedule


@pytest.fixture
def drive(sleep):
    """Plays out the adapter's loop against a schedule. `failures` has one entry per attempt, in
    order: an exception to raise, True for a `_Retryable`, or False/absent to succeed."""

    def drive(schedule, failures=()):
        outcomes = list(failures)
        run = _Run()

        def query(chunk):
            run.chunks.append(chunk)
            outcome = outcomes.pop(0) if outcomes else False
            if outcome is True:
                outcome = _Retryable()
            if outcome:
                raise outcome
            return chunk

        try:
            run.items = list(schedule.run(query))
        except Exception as e:
            run.error = e
        run.delays = [call.args[0] for call in sleep.call_args_list]
        return run

    return drive


@pytest.fixture
def no_jitter(mocker):
    mocker.patch("dbt.adapters.confluent.catalog.random.uniform", return_value=1.0)


class TestChunking:
    def test_hands_out_every_relation_once_in_order_at_the_largest_size(
        self, drive, make_schedule
    ):
        relations = _relations(LARGE * 2 + 3)

        run = drive(make_schedule(relations))

        assert run.sizes == [LARGE, LARGE, 3]
        assert [r for chunk in run.chunks for r in chunk] == relations
        assert run.delays == []

    def test_yields_the_items_of_each_chunk_individually_in_order(self, drive, make_schedule):
        relations = _relations(LARGE * 2 + 3)

        run = drive(make_schedule(relations))

        assert run.items == relations

    def test_a_failed_chunk_yields_nothing_until_its_retries_succeed(self, drive, make_schedule):
        relations = _relations(LARGE)

        run = drive(make_schedule(relations), failures=[True])

        assert run.items == relations

    def test_no_relations_means_no_queries(self, drive, make_schedule):
        assert drive(make_schedule([])).chunks == []


class TestHalvingOnFailure:
    def test_failed_chunk_is_handed_out_again_at_half_the_size(self, drive, make_schedule):
        relations = _relations(LARGE)

        run = drive(make_schedule(relations), failures=[True])

        # The failed chunk's relations are all covered, once each, by the smaller retries.
        assert run.sizes == [LARGE, HALF, HALF]
        assert run.chunks[0] == relations
        assert run.chunks[1] + run.chunks[2] == relations
        assert len(run.delays) == 1

    def test_smaller_size_is_kept_for_the_remaining_chunks(self, drive, make_schedule):
        run = drive(make_schedule(_relations(LARGE + HALF * 2)), failures=[True])

        # After the first chunk fails, nothing climbs back up to the largest size.
        assert run.sizes == [LARGE] + [HALF] * 4

    def test_halves_down_to_one_relation_per_query(self, drive, make_schedule):
        run = drive(make_schedule(_relations(LARGE)), failures=[True] * 3)

        # 10 -> 5 -> 2 -> 1: a size that doesn't divide evenly rounds down.
        assert run.sizes == [LARGE, HALF, QUARTER] + [ONE] * LARGE


class TestUnretryableErrors:
    def test_other_errors_propagate_without_retrying(self, drive, make_schedule):
        unlisted = _Unlisted()

        run = drive(make_schedule(_relations(LARGE)), failures=[unlisted])

        assert run.error is unlisted
        assert len(run.chunks) == 1
        assert run.delays == []


class TestRetryNotification:
    def test_reports_the_error_chunk_and_delay_before_sleeping(
        self, drive, make_schedule, on_retryable_error, sleep, no_jitter
    ):
        relations = _relations(LARGE)
        error = _Retryable("boom")

        drive(make_schedule(relations), failures=[error])

        on_retryable_error.assert_called_once_with(error, relations, BACKOFF.base_delay_secs)
        sleep.assert_called_once_with(BACKOFF.base_delay_secs)

    def test_is_not_called_for_other_errors_or_the_final_failure(
        self, drive, make_schedule, on_retryable_error
    ):
        drive(make_schedule(_relations(LARGE)), failures=[_Unlisted()])
        drive(make_schedule(_relations(LARGE)), failures=[True] * 100)

        # Once for each failure that was retried, none for the other error or the last failure.
        assert on_retryable_error.call_count == MAX_ERRORS - 1


class TestGivingUp:
    def test_gives_up_after_the_maximum_consecutive_failures(self, drive, make_schedule):
        run = drive(make_schedule(_relations(LARGE)), failures=[True] * 100)

        assert run.gave_up
        assert run.sizes == [LARGE, HALF, QUARTER, ONE]
        assert len(run.chunks) == MAX_ERRORS
        # One backoff per failure that is going to be retried: every failure but the last.
        assert len(run.delays) == len(run.chunks) - 1

    def test_success_resets_the_count_of_consecutive_failures(self, drive, make_schedule):
        under_limit = [True] * (MAX_ERRORS - 1)

        run = drive(make_schedule(_relations(LARGE)), failures=under_limit + [False] + under_limit)

        assert not run.gave_up


class TestBackoff:
    def test_waits_longer_after_each_consecutive_failure(self, drive, make_schedule, no_jitter):
        run = drive(make_schedule(_relations(LARGE)), failures=[True] * 3)

        assert run.delays == [
            BACKOFF.base_delay_secs,
            BACKOFF.base_delay_secs * 2,
            BACKOFF.base_delay_secs * 4,
        ]

    def test_backoff_starts_over_after_a_success(self, drive, make_schedule, no_jitter):
        # Fail, succeed, then fail again on a later chunk.
        run = drive(make_schedule(_relations(LARGE + HALF * 2)), failures=[True, False, True])

        assert run.delays == [BACKOFF.base_delay_secs] * 2

    def test_delay_is_capped(self, drive, make_schedule, no_jitter):
        schedule = make_schedule(_relations(LARGE), max_consecutive_errors=100)
        schedule._consecutive_failures = 50

        run = drive(schedule, failures=[True])

        assert run.delays == [BACKOFF.max_delay_secs]

    def test_delay_is_jittered_within_the_band(self, drive, make_schedule):
        low, high = BACKOFF.jitter_band
        delays = set()
        for _ in range(50):
            run = drive(make_schedule(_relations(LARGE)), failures=[True])
            delays.update(run.delays)

        assert len(delays) > 1
        assert all(BACKOFF.base_delay_secs * low <= d for d in delays)
        assert all(d <= BACKOFF.base_delay_secs * high for d in delays)


class _FakeCatalogMacro:
    """Stands in for `execute_macro`: raises or returns per call, and records the relations
    each call was asked for. Each relation yields one catalog row, named after it."""

    def __init__(self, outcomes=None):
        # One entry per call: an exception to raise, or None to succeed. Calls beyond the
        # list succeed.
        self.outcomes = list(outcomes or [])
        self.calls: list[list[str]] = []

    def __call__(self, macro_name, kwargs):
        assert macro_name == "get_catalog_relations"
        self.calls.append([r.identifier for r in kwargs["relations"]])
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if outcome is not None:
            raise outcome
        return [{"table_name": r.identifier} for r in kwargs["relations"]]


@pytest.fixture
def get_catalog(mocker, sleep):
    """Runs `query_catalog_rows` against a fake macro."""

    def get_catalog(macro, relations):
        return list(
            query_catalog_rows(macro, mocker.sentinel.information_schema, relations, sleep=sleep)
        )

    return get_catalog


class TestQueryCatalogRows:
    def test_queries_every_relation_once_when_nothing_fails(self, get_catalog, sleep):
        macro = _FakeCatalogMacro()
        relations = _relations(LARGE + 3)

        rows = get_catalog(macro, relations)

        assert [len(call) for call in macro.calls] == [LARGE, 3]
        assert [row["table_name"] for row in rows] == [r.identifier for r in relations]
        sleep.assert_not_called()

    def test_failed_chunk_is_retried_smaller_after_waiting(self, get_catalog, sleep):
        macro = _FakeCatalogMacro([BACKEND_ERROR])
        relations = _relations(LARGE)

        rows = get_catalog(macro, relations)

        assert [len(call) for call in macro.calls] == [LARGE, HALF, HALF]
        assert [row["table_name"] for row in rows] == [r.identifier for r in relations]
        assert sleep.call_count == 1

    def test_raises_the_error_once_the_reader_gives_up(self, get_catalog, sleep):
        macro = _FakeCatalogMacro([BACKEND_ERROR] * 100)

        with pytest.raises(DbtRuntimeError) as exc_info:
            get_catalog(macro, _relations(LARGE))

        assert exc_info.value is BACKEND_ERROR
        # No pointless wait before giving up: a backoff for every failure but the last.
        assert sleep.call_count == len(macro.calls) - 1

    @pytest.mark.parametrize("wrap", [_database_error, _macro_error], ids=["bare", "nested"])
    @pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
    def test_retryable_errors_are_retried(self, get_catalog, status, wrap):
        macro = _FakeCatalogMacro([wrap(status)])

        get_catalog(macro, _relations(LARGE))

        assert len(macro.calls) > 1

    @pytest.mark.parametrize(
        "error",
        [
            _database_error(400),
            _database_error(404),
            _database_error(None),
            _macro_error(400),
            DbtDatabaseError("no driver error behind this one"),
            DbtRuntimeError("not a database error"),
            CompilationError("bad macro"),
        ],
        ids=[
            "http-400",
            "http-404",
            "no-status",
            "nested-http-400",
            "no-cause",
            "runtime-error",
            "compilation-error",
        ],
    )
    def test_other_errors_are_not_retried(self, get_catalog, sleep, error):
        macro = _FakeCatalogMacro([error])

        with pytest.raises(type(error)) as exc_info:
            get_catalog(macro, _relations(LARGE))

        assert exc_info.value is error
        assert len(macro.calls) == 1
        sleep.assert_not_called()


class TestIsRetryableBackendError:
    @pytest.mark.parametrize("status", [429, 500, 501, 503, 599])
    def test_429_and_5xx_statuses_are_retryable(self, status):
        assert is_retryable_backend_error(_database_error(status))
        assert is_retryable_backend_error(_macro_error(status))

    @pytest.mark.parametrize("status", [None, 200, 400, 404, 428, 430, 499, 600])
    def test_other_statuses_are_not(self, status):
        assert not is_retryable_backend_error(_database_error(status))


class TestAdapterDelegation:
    def test_catalog_for_relations_is_queried_by_the_catalog_module_then_assembled(self, mocker):
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        relations = _relations(2)
        rows = [{"table_name": "t"}]
        query = mocker.patch("dbt.adapters.confluent.impl.query_catalog_rows", return_value=rows)
        assemble = mocker.patch.object(adapter, "_assemble_catalog")
        used_schemas = frozenset({("env-1", "cluster-a")})

        result = adapter._get_one_catalog_by_relations(
            mocker.sentinel.information_schema, relations, used_schemas
        )

        query.assert_called_once_with(
            adapter.execute_macro, mocker.sentinel.information_schema, relations
        )
        assemble.assert_called_once_with(rows, used_schemas)
        assert result is assemble.return_value
