"""Unit tests for the deferred-cleanup registry (pre/post_model_hook).

Materializations register temp relations (defer_drop) mid-macro; dbt calls
post_model_hook in a try/finally around the materialization, so the registered
drops run even when the macro raises — Jinja itself has no try/finally. Drop
failures are demoted to warnings, and the registry is keyed by thread because
dbt runs each node — and both its hooks — on one thread.

Only tables are registered: the temp objects are created by bounded statements
that reach a terminal phase immediately and are reaped by cursor.close() in the
connection manager, so there is nothing to delete explicitly.
"""

import threading
from unittest.mock import MagicMock, patch

from dbt.adapters.confluent.impl import ConfluentAdapter


def _adapter() -> ConfluentAdapter:
    """A real adapter instance (bypassing __init__) with mocked connections
    and the registry storage __init__ would have created."""
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter.connections = MagicMock()
    adapter._deferred_cleanups = threading.local()
    return adapter


def _adapter_with_execute_mock() -> tuple[ConfluentAdapter, MagicMock]:
    """Adapter whose execute is a plain mock, so tests can assert what the
    post-hook dropped without threading through the connection manager."""
    adapter = _adapter()
    adapter.execute = MagicMock()
    return adapter, adapter.execute


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

    def test_registry_consumed_by_hook(self):
        """A second post-hook (or the next node reusing the thread without new
        registrations) has nothing left to clean."""
        adapter, execute = _adapter_with_execute_mock()
        adapter.defer_drop("`db`.`sch`.`tmp`")
        adapter.post_model_hook({}, None)
        execute.reset_mock()
        adapter.post_model_hook({}, None)
        assert execute.mock_calls == []

    def test_duplicate_registrations_dropped_once(self):
        adapter, execute = _adapter_with_execute_mock()
        adapter.defer_drop("`db`.`sch`.`tmp`")
        adapter.defer_drop("`db`.`sch`.`tmp`")
        adapter.post_model_hook({}, None)
        assert execute.call_count == 1

    def test_failures_warn_and_do_not_stop_remaining_cleanups(self):
        """post_model_hook runs inside dbt's finally: raising would mask the
        materialization's own error, so failures become warnings and the rest
        of the registry is still processed."""
        adapter, execute = _adapter_with_execute_mock()
        execute.side_effect = RuntimeError("drop failed")
        adapter.defer_drop("`db`.`sch`.`tmp_a`")
        adapter.defer_drop("`db`.`sch`.`tmp_b`")
        with patch("dbt.adapters.confluent.impl.fire_event") as fire_event:
            adapter.post_model_hook({}, None)
        assert execute.call_count == 2
        assert fire_event.call_count == 2
        messages = [c.args[0].base_msg for c in fire_event.call_args_list]
        assert any("`db`.`sch`.`tmp_a`" in m for m in messages)
        assert any("`db`.`sch`.`tmp_b`" in m for m in messages)

    def test_pre_model_hook_clears_stale_entries(self):
        """Worker threads are reused across nodes: entries left by a node that
        died without reaching its post-hook must not leak into the next node."""
        adapter, execute = _adapter_with_execute_mock()
        adapter.defer_drop("`db`.`sch`.`stale`")
        adapter.pre_model_hook({})
        adapter.post_model_hook({}, None)
        assert execute.mock_calls == []

    def test_registrations_are_thread_local(self):
        adapter, execute = _adapter_with_execute_mock()
        worker = threading.Thread(target=adapter.defer_drop, args=("`db`.`sch`.`other`",))
        worker.start()
        worker.join()
        adapter.post_model_hook({}, None)
        assert execute.mock_calls == []

    def test_empty_registry_is_a_noop(self):
        """Nodes that register nothing (the common case) pay zero round-trips,
        even when pre_model_hook never ran on this thread."""
        adapter, execute = _adapter_with_execute_mock()
        adapter.post_model_hook({}, None)
        assert execute.mock_calls == []
