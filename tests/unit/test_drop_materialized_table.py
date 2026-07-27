"""Unit tests for materialized-table drops.

A materialized table must be dropped with DROP MATERIALIZED TABLE.
`drop_materialized_table` uses the IF EXISTS form so a stale relation cache or
an externally dropped table doesn't fail the run, evicts the relation cache
like SQLAdapter.drop_relation does, and then waits for the catalog entry to
disappear: the MT drop removes it asynchronously, and every caller immediately
recreates the name. Errors propagate unmodified.

`drop_relation` must detect a materialized table BEFORE dropping (one
IS_MATERIALIZED lookup, skipped for views) and route it to
`drop_materialized_table`: the server silently accepts DROP TABLE against an
MT and phantom-drops it — the catalog entry disappears transiently, same-name
creates fail "table already exists", and the MT resurfaces. The failed drop
raises no error, so only the pre-check can prevent it.
"""

from unittest.mock import MagicMock, patch

import pytest
from confluent_sql.exceptions import OperationalError
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter


@pytest.fixture(autouse=True)
def no_sleep():
    """Replace time.sleep so the catalog-absence polling runs instantly."""
    with patch("dbt.adapters.confluent.impl.time.sleep"):
        yield


def _catalog_result(row_count):
    """A (response, agate-ish table) pair as returned by execute(fetch=True)."""
    table = MagicMock()
    table.rows = [("name",)] * row_count
    return (MagicMock(), table)


def _is_materialized_result(*values):
    """Result of the IS_MATERIALIZED pre-check: one row per value ('YES'/'NO'),
    no values = relation absent from the catalog."""
    table = MagicMock()
    table.rows = [(v,) for v in values]
    return (MagicMock(), table)


def _wire_execute(adapter, fetch_results):
    """Make adapter.execute serve non-fetch calls (DROPs) trivially and fetch
    calls (IS_MATERIALIZED pre-check, catalog polls) from `fetch_results`, in
    order."""
    results = iter(fetch_results)

    def fake_execute(sql, **kwargs):
        if kwargs.get("fetch"):
            return next(results)
        return (MagicMock(), MagicMock())

    adapter.execute = MagicMock(side_effect=fake_execute)


def _mt_drop_sqls(adapter):
    return [
        c.args[0]
        for c in adapter.execute.call_args_list
        if c.args and "DROP MATERIALIZED TABLE IF EXISTS" in c.args[0]
    ]


@pytest.fixture
def adapter():
    a = ConfluentAdapter.__new__(ConfluentAdapter)  # bypass __init__
    _wire_execute(a, [_catalog_result(0)])  # default: gone on the first poll
    a.cache_dropped = MagicMock()
    return a


@pytest.fixture
def relation():
    rel = MagicMock()
    rel.type = "table"
    rel.database = "env-x"
    rel.schema = "sch"
    rel.identifier = "my_mt"
    rel.__str__ = MagicMock(return_value="`env-x`.`sch`.`my_mt`")
    return rel


class TestDropMaterializedTable:
    def test_issues_drop_materialized_table_if_exists_as_streaming_ddl(self, adapter, relation):
        adapter.drop_materialized_table(relation)
        drop_call = adapter.execute.call_args_list[0]
        assert "DROP MATERIALIZED TABLE IF EXISTS" in drop_call.args[0]
        assert "my_mt" in drop_call.args[0]
        assert drop_call.kwargs.get("execution_mode") == "streaming_ddl"

    def test_evicts_relation_cache(self, adapter, relation):
        adapter.drop_materialized_table(relation)
        adapter.cache_dropped.assert_called_once_with(relation)

    def test_waits_for_catalog_entry_to_disappear(self, adapter, relation):
        """The entry lingers for two polls, then clears — the drop returns
        only once the catalog agrees, so the caller can recreate the name."""
        _wire_execute(adapter, [_catalog_result(1), _catalog_result(1), _catalog_result(0)])
        adapter.drop_materialized_table(relation)
        # 1 DROP + 3 polls
        assert adapter.execute.call_count == 4

    def test_wait_timeout_raises_retriable_error(self, adapter, relation):
        _wire_execute(adapter, [_catalog_result(1)])
        with pytest.raises(DbtDatabaseError) as exc_info:
            adapter._wait_for_catalog_absence(relation, timeout=0)
        assert "dbt retry" in str(exc_info.value)

    def test_errors_propagate(self, adapter, relation):
        err = OperationalError("Could not execute DropTable in path env.db.my_mt")
        adapter.execute = MagicMock(side_effect=err)
        with pytest.raises(OperationalError) as exc_info:
            adapter.drop_materialized_table(relation)
        assert exc_info.value is err


class TestDropRelationRouting:
    def test_materialized_table_is_detected_and_routed_before_any_drop_table(
        self, adapter, relation
    ):
        """The critical path: DROP TABLE would be silently accepted by the
        server and phantom-drop the MT, so the pre-check must route to DROP
        MATERIALIZED TABLE without the regular drop ever being submitted."""
        # Pre-check says MT, then the absence poll sees it gone.
        _wire_execute(adapter, [_is_materialized_result("YES"), _catalog_result(0)])
        adapter.execute_macro = MagicMock()
        adapter.drop_relation(relation)
        adapter.execute_macro.assert_not_called()
        assert len(_mt_drop_sqls(adapter)) == 1

    def test_regular_table_uses_the_regular_drop(self, adapter, relation):
        _wire_execute(adapter, [_is_materialized_result("NO")])
        adapter.execute_macro = MagicMock()
        adapter.drop_relation(relation)
        adapter.execute_macro.assert_called_once()
        assert _mt_drop_sqls(adapter) == []

    def test_absent_relation_uses_the_regular_drop(self, adapter, relation):
        """No catalog row (e.g. stale cache): the regular IF EXISTS drop
        handles it; no MT drop."""
        _wire_execute(adapter, [_is_materialized_result()])
        adapter.execute_macro = MagicMock()
        adapter.drop_relation(relation)
        adapter.execute_macro.assert_called_once()
        assert _mt_drop_sqls(adapter) == []

    def test_views_skip_the_pre_check(self, adapter, relation):
        """A view can't be an MT — no catalog round-trip on the drop path."""
        relation.type = "view"
        adapter.execute_macro = MagicMock()
        adapter.drop_relation(relation)
        adapter.execute_macro.assert_called_once()
        adapter.execute.assert_not_called()
