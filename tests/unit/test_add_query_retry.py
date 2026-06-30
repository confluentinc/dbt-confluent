"""Unit tests for _execute_query_with_retry.

Covers:
- Success on first attempt (no retry).
- Retry on existing retryable_exceptions class (ComputePoolExhaustedError).
- Retry on OperationalError with http_status_code=409 (name-conflict
  during async teardown of a prior statement with the same name).
- Pass-through on OperationalError with a non-409 status code.
- Exhaustion: re-raises after retry_limit attempts.
"""

from unittest.mock import MagicMock, patch

import pytest
from confluent_sql.exceptions import ComputePoolExhaustedError, OperationalError

from dbt.adapters.confluent.connections import _execute_query_with_retry


@pytest.fixture(autouse=True)
def no_sleep():
    """Replace time.sleep so the retry tests run instantly."""
    with patch("dbt.adapters.confluent.connections.time.sleep"):
        yield


def _run(cursor, **overrides):
    kwargs = {
        "cursor": cursor,
        "sql": "SELECT 1",
        "bindings": None,
        "retryable_exceptions": (ComputePoolExhaustedError,),
        "retry_limit": 5,
        "attempt": 1,
        "statement_name": "dbt-test-stmt",
        "statement_labels": ["dbt-confluent"],
    }
    kwargs.update(overrides)
    return _execute_query_with_retry(**kwargs)


class TestRetryBehavior:
    def test_success_no_retry(self):
        cursor = MagicMock()
        _run(cursor)
        assert cursor.execute.call_count == 1

    def test_retries_on_compute_pool_exhausted_then_succeeds(self):
        cursor = MagicMock()
        cursor.execute.side_effect = [
            ComputePoolExhaustedError("pool exhausted", "dbt-test-stmt", True),
            None,
        ]
        _run(cursor)
        assert cursor.execute.call_count == 2

    def test_retries_on_409_then_succeeds(self):
        cursor = MagicMock()
        cursor.execute.side_effect = [
            OperationalError("name in use", http_status_code=409),
            None,
        ]
        _run(cursor)
        assert cursor.execute.call_count == 2

    def test_does_not_retry_on_non_409_operational_error(self):
        cursor = MagicMock()
        e = OperationalError("permission denied", http_status_code=403)
        cursor.execute.side_effect = e
        with pytest.raises(OperationalError) as exc_info:
            _run(cursor)
        assert exc_info.value is e
        assert cursor.execute.call_count == 1

    def test_does_not_retry_on_operational_error_without_status(self):
        cursor = MagicMock()
        cursor.execute.side_effect = OperationalError("no status code")
        with pytest.raises(OperationalError):
            _run(cursor)
        assert cursor.execute.call_count == 1

    def test_exhausts_retries_and_raises_on_persistent_409(self):
        cursor = MagicMock()
        cursor.execute.side_effect = OperationalError("still in use", http_status_code=409)
        with pytest.raises(OperationalError):
            _run(cursor, retry_limit=3)
        # attempt=1 plus 2 retries = 3 calls; attempt #3 hits the limit and re-raises.
        assert cursor.execute.call_count == 3

    def test_exhausts_retries_and_raises_on_persistent_pool_exhausted(self):
        cursor = MagicMock()
        cursor.execute.side_effect = ComputePoolExhaustedError("pool", "dbt-test-stmt", True)
        with pytest.raises(ComputePoolExhaustedError):
            _run(cursor, retry_limit=2)
        assert cursor.execute.call_count == 2

    def test_reuses_statement_name_across_retries(self):
        """The same statement_name must be passed to each cursor.execute attempt."""
        cursor = MagicMock()
        cursor.execute.side_effect = [
            OperationalError("name in use", http_status_code=409),
            OperationalError("name in use", http_status_code=409),
            None,
        ]
        _run(cursor, statement_name="dbt-fixed-name")
        for call in cursor.execute.call_args_list:
            assert call.kwargs["statement_name"] == "dbt-fixed-name"


class TestComputePoolForwarding:
    def test_compute_pool_id_defaults_to_none(self):
        """When unset, cursor.execute receives compute_pool_id=None (connection default)."""
        cursor = MagicMock()
        _run(cursor)
        assert cursor.execute.call_args.kwargs["compute_pool_id"] is None

    def test_compute_pool_id_is_forwarded(self):
        """A per-model compute_pool_id reaches cursor.execute."""
        cursor = MagicMock()
        _run(cursor, compute_pool_id="lfcp-override")
        assert cursor.execute.call_args.kwargs["compute_pool_id"] == "lfcp-override"

    def test_compute_pool_id_preserved_across_retries(self):
        """The same compute_pool_id must be passed to each cursor.execute attempt."""
        cursor = MagicMock()
        cursor.execute.side_effect = [
            OperationalError("name in use", http_status_code=409),
            None,
        ]
        _run(cursor, compute_pool_id="lfcp-override")
        assert cursor.execute.call_count == 2
        for call in cursor.execute.call_args_list:
            assert call.kwargs["compute_pool_id"] == "lfcp-override"
