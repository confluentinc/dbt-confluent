"""Unit tests for the deferred-cleanup registry (pre/post_model_hook).

Materializations register temp relations (defer_drop) mid-macro; dbt calls
post_model_hook in a try/finally around the materialization, so the registered
drops run even when the macro raises — Jinja itself has no try/finally. The
post-hook drops via adapter.drop_relation (type-aware, IF EXISTS). Drop failures
are demoted to warnings, and the registry is keyed by thread because dbt runs
each node — and both its hooks — on one thread.

Only tables are registered by today's consumers, but drop_relation handles any
relation type; the temp objects are created by bounded statements that reach a
terminal phase immediately and are reaped by cursor.close() in the connection
manager, so there is nothing to delete explicitly.
"""

import threading
from unittest.mock import MagicMock, patch

from dbt.adapters.confluent.impl import ConfluentAdapter, _CleanupRegistry
from tests.unit._helpers import relation


def _adapter() -> tuple[ConfluentAdapter, MagicMock]:
    """A real adapter instance (bypassing __init__) with the registry storage
    __init__ would have created and a mocked drop_relation."""
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter._deferred_cleanups = _CleanupRegistry()
    adapter.drop_relation = MagicMock()
    return adapter, adapter.drop_relation


class TestDeferredCleanups:
    def test_drops_registered_relations_via_drop_relation(self):
        adapter, drop_relation = _adapter()
        rel = relation("tmp_a")
        adapter.defer_drop(rel)
        adapter.post_model_hook({}, None)
        drop_relation.assert_called_once_with(rel)

    def test_registry_consumed_by_hook(self):
        """A second post-hook (or the next node reusing the thread without new
        registrations) has nothing left to clean."""
        adapter, drop_relation = _adapter()
        adapter.defer_drop(relation("tmp"))
        adapter.post_model_hook({}, None)
        drop_relation.reset_mock()
        adapter.post_model_hook({}, None)
        assert drop_relation.mock_calls == []

    def test_duplicate_registrations_dropped_once(self):
        adapter, drop_relation = _adapter()
        adapter.defer_drop(relation("tmp"))
        adapter.defer_drop(relation("tmp"))  # distinct object, equal by value
        adapter.post_model_hook({}, None)
        assert drop_relation.call_count == 1

    def test_failures_warn_and_do_not_stop_remaining_cleanups(self):
        """post_model_hook runs inside dbt's finally: raising would mask the
        materialization's own error, so failures become warnings and the rest
        of the registry is still processed."""
        adapter, drop_relation = _adapter()
        drop_relation.side_effect = RuntimeError("drop failed")
        adapter.defer_drop(relation("tmp_a"))
        adapter.defer_drop(relation("tmp_b"))
        with patch("dbt.adapters.confluent.impl.fire_event") as fire_event:
            adapter.post_model_hook({}, None)
        assert drop_relation.call_count == 2
        assert fire_event.call_count == 2
        messages = [c.args[0].base_msg for c in fire_event.call_args_list]
        assert any("`env-1`.`cluster-a`.`tmp_a`" in m for m in messages)
        assert any("`env-1`.`cluster-a`.`tmp_b`" in m for m in messages)

    def test_pre_model_hook_clears_stale_entries(self):
        """Worker threads are reused across nodes: entries left by a node that
        died without reaching its post-hook must not leak into the next node."""
        adapter, drop_relation = _adapter()
        adapter.defer_drop(relation("stale"))
        adapter.pre_model_hook({})
        adapter.post_model_hook({}, None)
        assert drop_relation.mock_calls == []

    def test_registrations_are_thread_local(self):
        adapter, drop_relation = _adapter()
        worker = threading.Thread(target=adapter.defer_drop, args=(relation("other"),))
        worker.start()
        worker.join()
        adapter.post_model_hook({}, None)
        assert drop_relation.mock_calls == []

    def test_empty_registry_is_a_noop(self):
        """Nodes that register nothing (the common case) pay zero round-trips,
        even when pre_model_hook never ran on this thread."""
        adapter, drop_relation = _adapter()
        adapter.post_model_hook({}, None)
        assert drop_relation.mock_calls == []
