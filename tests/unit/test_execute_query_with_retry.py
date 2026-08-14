"""Unit tests for _execute_query_with_retry.

Covers:
- Retry behavior: success on first attempt, retry on ComputePoolExhaustedError
  and on OperationalError with http_status_code=409 (name-conflict during
  async teardown of a prior statement with the same name), retry on
  OperationalError "being modified" (materialized table still
  establishing/evolving) and "kafka topic does not exist" (recreate racing a
  recent drop's asynchronous teardown) — each with a dedicated, more generous
  budget — pass-through on a non-409 OperationalError, and exhaustion
  (re-raises after retry_limit attempts).
- No retry on OperationalError "table already exists": it never clears by
  waiting, and retrying would delay every genuine name conflict by the
  whole budget.
- No retry on Schema Registry subject "doesn't match" either: subjects are
  not cleaned up when a relation is dropped, so it never clears by waiting;
  it is re-raised immediately with recovery guidance appended.
- Failed-statement cleanup before each retry: a FAILED statement still
  occupies its name (confluent-sql only auto-deletes pool-exhausted ones), so
  the retry path must call cursor.delete_statement() to free it — otherwise
  the retry bounces off 409 name conflicts instead of the condition clearing.
- Parameter forwarding: statement_name, compute_pool_id, and
  statement_properties all reach cursor.execute() correctly and are
  preserved across retries (each in its own Test*Forwarding class below).
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

    def test_retries_on_being_modified_then_succeeds(self):
        cursor = MagicMock()
        cursor.execute.side_effect = [
            OperationalError("Materialized table is currently being modified by statement: s1"),
            None,
        ]
        _run(cursor)
        assert cursor.execute.call_count == 2

    def test_being_modified_uses_generous_budget_beyond_retry_limit(self):
        """'being modified' gets a dedicated budget of max(retry_limit, 12), so it
        keeps waiting for the MT to settle even when the default retry_limit is
        small (a rapid re-run can outlast the normal budget)."""
        cursor = MagicMock()
        cursor.execute.side_effect = OperationalError("table is being modified by statement: s1")
        with pytest.raises(OperationalError):
            _run(cursor, retry_limit=2)
        assert cursor.execute.call_count == 12

    def test_being_modified_classified_by_message_even_with_409_status(self):
        """A 'being modified' error can arrive with a 409 status. It must be
        classified by message (generous budget of 12), not as a 409 name-reuse
        race (which would cap at retry_limit). Guards the message-before-status
        ordering in _execute_query_with_retry."""
        cursor = MagicMock()
        cursor.execute.side_effect = OperationalError(
            "table is being modified by statement: s1", http_status_code=409
        )
        with pytest.raises(OperationalError):
            _run(cursor, retry_limit=2)
        assert cursor.execute.call_count == 12

    def test_retries_on_topic_gone_then_succeeds(self):
        """A recreate racing a recent drop's asynchronous teardown is rejected
        with "... was found, but its Kafka topic does not exist ... try again
        later" — retried until the dying catalog entry clears."""
        cursor = MagicMock()
        cursor.execute.side_effect = [
            OperationalError(
                "Statement submission failed: The table 'my_table' was found, but its "
                "Kafka topic does not exist; this may be due to delayed deletion or "
                "eventual consistency, so try again later"
            ),
            None,
        ]
        _run(cursor)
        assert cursor.execute.call_count == 2

    def test_topic_gone_uses_generous_budget_beyond_retry_limit(self):
        cursor = MagicMock()
        cursor.execute.side_effect = OperationalError(
            "The table 'my_table' was found, but its Kafka topic does not exist"
        )
        with pytest.raises(OperationalError):
            _run(cursor, retry_limit=2)
        assert cursor.execute.call_count == 12

    def test_does_not_retry_sr_subject_mismatch_and_appends_guidance(self):
        """ "Schema Registry subject ... doesn't match" is not retryable: a
        dropped relation's SR subjects are not deleted with it, so it never
        clears by waiting. It surfaces on the first attempt, re-raised with
        recovery guidance appended (delete the subject or use a different
        relation name) because the raw server message is cryptic."""
        cursor = MagicMock()
        e = OperationalError(
            "Statement submission failed: Cannot create table because the "
            "Schema Registry subject 'my_table-value' doesn't match the existing one."
        )
        cursor.execute.side_effect = e
        with pytest.raises(OperationalError) as exc_info:
            _run(cursor)
        assert cursor.execute.call_count == 1
        assert exc_info.value.__cause__ is e
        assert str(e) in str(exc_info.value)
        assert "delete the lingering subject" in str(exc_info.value)

    def test_sr_subject_mismatch_guidance_covers_does_not_match_variant(self):
        cursor = MagicMock()
        cursor.execute.side_effect = OperationalError(
            "Cannot create table because the Schema Registry subject "
            "'my_table-value' does not match the existing one."
        )
        with pytest.raises(OperationalError) as exc_info:
            _run(cursor)
        assert cursor.execute.call_count == 1
        assert "delete the lingering subject" in str(exc_info.value)

    def test_does_not_retry_table_already_exists(self):
        """ "table already exists" is not a retryable condition: it never
        clears by waiting (see module docstring), so it surfaces on the
        first attempt."""
        cursor = MagicMock()
        e = OperationalError(
            "Statement 'dbt-my-stmt' failed: failed creating table: table already exists"
        )
        cursor.execute.side_effect = e
        with pytest.raises(OperationalError) as exc_info:
            _run(cursor)
        assert exc_info.value is e
        assert cursor.execute.call_count == 1

    def test_deletes_failed_statement_before_each_retry(self):
        """A FAILED statement still occupies statement_name; each retry must
        free it via cursor.delete_statement() or the resubmission would 409.
        No delete after the final (budget-exhausted) attempt — the FAILED
        statement is left in place for debugging."""
        cursor = MagicMock()
        cursor.execute.side_effect = [
            OperationalError("table is being modified by statement: s1"),
            OperationalError("table is being modified by statement: s1"),
            None,
        ]
        _run(cursor)
        assert cursor.execute.call_count == 3
        assert cursor.delete_statement.call_count == 2

    def test_no_delete_when_budget_exhausted(self):
        cursor = MagicMock()
        cursor.execute.side_effect = OperationalError("being modified")
        with pytest.raises(OperationalError):
            _run(cursor, retry_limit=12)
        assert cursor.execute.call_count == 12
        # 11 retries were attempted; the 12th failure re-raises without cleanup.
        assert cursor.delete_statement.call_count == 11

    def test_delete_statement_failure_does_not_mask_retry(self):
        """cursor.delete_statement() failing (e.g. transient API error) must
        not abort the retry loop — the retry proceeds and may still succeed."""
        cursor = MagicMock()
        cursor.execute.side_effect = [
            OperationalError("being modified"),
            None,
        ]
        cursor.delete_statement.side_effect = OperationalError("deletion failed")
        _run(cursor)
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


class TestStatementPropertiesForwarding:
    def test_statement_properties_defaults_to_none(self):
        """When unset, cursor.execute receives properties=None."""
        cursor = MagicMock()
        _run(cursor)
        assert cursor.execute.call_args.kwargs["properties"] is None

    def test_statement_properties_is_forwarded(self):
        """A per-model statement_properties dict reaches cursor.execute as `properties`."""
        cursor = MagicMock()
        props = {"sql.tables.scan.idle-timeout": "30 s"}
        _run(cursor, statement_properties=props)
        assert cursor.execute.call_args.kwargs["properties"] == props

    def test_statement_properties_preserved_across_retries(self):
        """The same statement_properties must be passed to each cursor.execute attempt."""
        cursor = MagicMock()
        cursor.execute.side_effect = [
            OperationalError("name in use", http_status_code=409),
            None,
        ]
        props = {"sql.tables.scan.idle-timeout": "30 s"}
        _run(cursor, statement_properties=props)
        assert cursor.execute.call_count == 2
        for call in cursor.execute.call_args_list:
            assert call.kwargs["properties"] == props
