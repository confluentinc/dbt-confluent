"""Unit tests for the deferred-cleanup registry (pre/post_model_hook).

Materializations register temp relations (defer_drop) and Flink statements
(defer_statement_delete) mid-macro; dbt calls post_model_hook in a try/finally
around the materialization, so the registered cleanups run even when the
macro raises — Jinja itself has no try/finally. Statements are deleted before
relations are dropped (DROP TABLE strands still-RUNNING dependents in
DEGRADED), failures are demoted to warnings, and the registry is keyed by
thread because dbt runs each node — and both its hooks — on one thread.
"""

import threading
from unittest.mock import MagicMock, call, patch

from dbt.adapters.confluent.connections import (
    ConfluentAdapterResponse,
    ConfluentConnectionManager,
)
from dbt.adapters.confluent.impl import ConfluentAdapter


def _adapter() -> ConfluentAdapter:
    """A real adapter instance (bypassing __init__) with mocked connections
    and the registry storage __init__ would have created."""
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter.connections = MagicMock()
    adapter._deferred_cleanups = threading.local()
    return adapter


def _adapter_with_effector_mock() -> tuple[ConfluentAdapter, MagicMock]:
    """Adapter whose delete_statement/execute are children of one mock, so
    tests can assert the relative order of statement deletes and table drops."""
    adapter = _adapter()
    effects = MagicMock()
    adapter.delete_statement = effects.delete_statement
    adapter.execute = effects.execute
    return adapter, effects


class TestDeferredCleanups:
    def test_drops_registered_relations_hidden(self):
        """The real execute path receives DROP TABLE IF EXISTS with hidden=True."""
        adapter = _adapter()
        adapter.defer_drop("`db`.`sch`.`tmp_a`")
        adapter.post_model_hook({}, None)
        adapter.connections.execute.assert_called_once_with(
            sql="DROP TABLE IF EXISTS `db`.`sch`.`tmp_a`",
            auto_begin=False,
            fetch=False,
            limit=None,
            execution_mode=None,
            hidden=True,
            statement_name=None,
            compute_pool_id=None,
        )

    def test_deletes_registered_statements_without_expecting_existence(self):
        """The real delete path passes expect_exists=False: by post-hook time
        the statement may already be terminal or gone, which is fine."""
        adapter = _adapter()
        adapter.defer_statement_delete("dbt-abc123")
        adapter.post_model_hook({}, None)
        handle = adapter.connections.get_thread_connection.return_value.handle
        handle.delete_statement.assert_called_once_with("dbt-abc123")

    def test_statements_deleted_before_relations_dropped(self):
        """Registration order is drop-first (macros register the drop before
        CREATE), but the hook must stop writers before dropping their table."""
        adapter, effects = _adapter_with_effector_mock()
        adapter.defer_drop("`db`.`sch`.`tmp`")
        adapter.defer_statement_delete("dbt-ctas")
        adapter.post_model_hook({}, None)
        assert effects.mock_calls == [
            call.delete_statement("dbt-ctas", expect_exists=False),
            call.execute("DROP TABLE IF EXISTS `db`.`sch`.`tmp`", hidden=True),
        ]

    def test_registry_consumed_by_hook(self):
        """A second post-hook (or the next node reusing the thread without new
        registrations) has nothing left to clean."""
        adapter, effects = _adapter_with_effector_mock()
        adapter.defer_drop("`db`.`sch`.`tmp`")
        adapter.post_model_hook({}, None)
        effects.reset_mock()
        adapter.post_model_hook({}, None)
        assert effects.mock_calls == []

    def test_duplicate_registrations_cleaned_once(self):
        adapter, effects = _adapter_with_effector_mock()
        adapter.defer_drop("`db`.`sch`.`tmp`")
        adapter.defer_drop("`db`.`sch`.`tmp`")
        adapter.defer_statement_delete("dbt-ctas")
        adapter.defer_statement_delete("dbt-ctas")
        adapter.post_model_hook({}, None)
        assert effects.delete_statement.call_count == 1
        assert effects.execute.call_count == 1

    def test_failures_warn_and_do_not_stop_remaining_cleanups(self):
        """post_model_hook runs inside dbt's finally: raising would mask the
        materialization's own error, so failures become warnings and the rest
        of the registry is still processed."""
        adapter, effects = _adapter_with_effector_mock()
        effects.delete_statement.side_effect = RuntimeError("api down")
        effects.execute.side_effect = RuntimeError("drop failed")
        adapter.defer_statement_delete("dbt-ctas")
        adapter.defer_drop("`db`.`sch`.`tmp_a`")
        adapter.defer_drop("`db`.`sch`.`tmp_b`")
        with patch("dbt.adapters.confluent.impl.fire_event") as fire_event:
            adapter.post_model_hook({}, None)
        assert effects.delete_statement.call_count == 1
        assert effects.execute.call_count == 2
        assert fire_event.call_count == 3
        messages = [c.args[0].base_msg for c in fire_event.call_args_list]
        assert any("dbt-ctas" in m for m in messages)
        assert any("`db`.`sch`.`tmp_b`" in m for m in messages)

    def test_pre_model_hook_clears_stale_entries(self):
        """Worker threads are reused across nodes: entries left by a node that
        died without reaching its post-hook must not leak into the next node."""
        adapter, effects = _adapter_with_effector_mock()
        adapter.defer_drop("`db`.`sch`.`stale`")
        adapter.defer_statement_delete("dbt-stale")
        adapter.pre_model_hook({})
        adapter.post_model_hook({}, None)
        assert effects.mock_calls == []

    def test_registrations_are_thread_local(self):
        adapter, effects = _adapter_with_effector_mock()
        worker = threading.Thread(target=adapter.defer_drop, args=("`db`.`sch`.`other`",))
        worker.start()
        worker.join()
        adapter.post_model_hook({}, None)
        assert effects.mock_calls == []

    def test_empty_registry_is_a_noop(self):
        """Nodes that register nothing (the common case) pay zero round-trips,
        even when pre_model_hook never ran on this thread."""
        adapter, effects = _adapter_with_effector_mock()
        adapter.post_model_hook({}, None)
        assert effects.mock_calls == []


class TestGetResponseStatementName:
    def test_response_carries_statement_name_and_phase(self):
        cursor = MagicMock()
        cursor._statement.phase = "COMPLETED"
        cursor._statement.name = "dbt-generated-uuid"
        response = ConfluentConnectionManager.get_response(cursor)
        assert isinstance(response, ConfluentAdapterResponse)
        assert response.statement_name == "dbt-generated-uuid"
        assert response._message == "COMPLETED"
