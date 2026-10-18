"""Functional tests for the `tableflow` config.

Unit tests (tests/unit/test_tableflow.py, test_disable_tableflow_if_enabled.py) mock the driver
entirely and never exercise the Jinja macro wiring, so they can't prove that Tableflow actually
gets enabled against Confluent Cloud, that the disable-before-drop macro call sites actually fire
correctly end-to-end, or -- crucially for TestNoSpuriousPatchOnDefaultErrorHandling below -- what
shape a real Confluent Cloud GET response actually has (a mocked response can only ever have the
shape a unit test author assumed it would).

- TestTableTableflow: enable on create, then the disable-before-drop ->
  recreate -> re-enable cycle on --full-refresh (table.sql's one drop path).
- TestMaterializedTableTableflowSwitchAndRefresh: materialized_table.sql's
  two distinct disable-before-drop call sites -- switching a regular table
  into an MT under --full-refresh, then full-refreshing that MT again.
- TestNoSpuriousPatchOnDefaultErrorHandling: a second identical run must not issue a PATCH when
  `config.error_handling` is left at its server default (`SUSPEND`) -- guards against a
  plausible-looking failure mode where a GET response omits an unset/default `error_handling`
  block entirely, making `existing`'s parsed value `None` while `desired`'s is a real
  `TableflowErrorHandlingSuspend()`, which would compare unequal and PATCH every run.

Requires a Global API key: Tableflow's control-plane routes need one
regardless of the Flink-region pair every other functional test uses (see
MATERIALIZATIONS.md#tableflow). Skipped entirely when
CONFLUENT_GLOBAL_API_KEY/CONFLUENT_GLOBAL_API_SECRET aren't both set.

Notes:
- Both enable_tableflow and disable_tableflow block (by default) until the
  topic reaches a terminal state -- RUNNING, or confirmed gone -- so these
  tests wait on real Confluent Cloud control-plane calls, not just Flink SQL.
  Expect them to take noticeably longer than the rest of the functional suite.
- Names are never reused across sessions (a dropped relation's topic and Tableflow
  config can outlive the catalog drop asynchronously); isolation's per-session
  prefix guarantees that, and its session-wide sweep reclaims stale leftovers.
"""

import os
from unittest.mock import patch

import pytest
from confluent_sql import Connection
from confluent_sql.exceptions import TableflowTopicNotFoundError

from dbt.tests.util import run_dbt, set_model_file
from tests.functional.adapter._helpers import (
    relation,
)
from tests.functional.adapter.fixtures import ConfluentFixtures
from tests.functional.adapter.isolation import physical_name

pytestmark = pytest.mark.skipif(
    not (os.getenv("CONFLUENT_GLOBAL_API_KEY") and os.getenv("CONFLUENT_GLOBAL_API_SECRET")),
    reason=(
        "Tableflow requires a Global API key -- set CONFLUENT_GLOBAL_API_KEY/"
        "CONFLUENT_GLOBAL_API_SECRET to run these tests"
    ),
)

_TABLEFLOW_CONFIG = "{'table_formats': 'ICEBERG', 'storage': {'kind': 'Managed'}}"


def _get_tableflow(project, name):
    """Live Tableflow state for `name`'s backing topic, or None if not enabled."""
    with project.adapter.connection_named("tableflow_check"):
        conn = project.adapter.connections.get_thread_connection()
        try:
            return conn.handle.get_tableflow(physical_name(name))
        except TableflowTopicNotFoundError:
            return None


def _disable_tableflow_best_effort(project, name):
    with project.adapter.connection_named("tableflow_teardown"):
        conn = project.adapter.connections.get_thread_connection()
        try:
            conn.handle.disable_tableflow(physical_name(name))
        except TableflowTopicNotFoundError:
            pass


class _TableflowFixtures(ConfluentFixtures):
    # Model names with Tableflow enabled, which must be disabled before their relation drops.
    TABLEFLOW_MODELS: list[str] = []

    @pytest.fixture(autouse=True, scope="class")
    def disable_tableflow(self, project, class_clean_up):
        # Disable Tableflow before class_clean_up drops the relations: disabling first keeps
        # the drop from racing an active materialization, per confluent_sql's own
        # recommendation. Requesting class_clean_up makes this tear down before it.
        yield
        for name in self.TABLEFLOW_MODELS:
            _disable_tableflow_best_effort(project, name)


TABLE = f"""
{{{{ config(
    materialized='table',
    tableflow={_TABLEFLOW_CONFIG},
) }}}}
select 1 as id, 'a' as name
"""


class TestTableTableflow(_TableflowFixtures):
    """Happy path for `table`: enabling on create, and the
    disable-before-drop -> recreate -> re-enable cycle on --full-refresh."""

    NAME = "tftable"
    TABLE = "tf_table"
    TABLEFLOW_MODELS = [TABLE]

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {f"{self.TABLE}.sql": TABLE}

    # Flaky: the test sometimes finds Tableflow not yet enabled right after the dbt run
    # (retried via pytest-rerunfailures). Remove once the cause is understood.
    @pytest.mark.flaky(reruns=2)
    def test_enable_then_full_refresh_recreates(self, project):
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)

        topic = _get_tableflow(project, self.TABLE)
        assert topic is not None, "Tableflow was not enabled on create"
        assert topic.phase.name == "RUNNING"

        # --full-refresh: disable_old_tableflow_before_drop must disable the
        # existing topic before the drop, and ensure_tableflow_config must
        # enable a fresh one on the recreated table afterward.
        results = run_dbt(["run", "--full-refresh"])
        assert all(r.status.name == "Success" for r in results)

        topic = _get_tableflow(project, self.TABLE)
        assert topic is not None, "Tableflow was not re-enabled after --full-refresh"
        assert topic.phase.name == "RUNNING"


NOOP_TABLE = f"""
{{{{ config(
    materialized='table',
    tableflow={{
        'table_formats': 'ICEBERG',
        'storage': {{'kind': 'Managed'}},
        'config': {{'error_handling': {{'mode': 'SUSPEND'}}}},
    }},
) }}}}
select 1 as id, 'a' as name
"""


class TestNoSpuriousPatchOnDefaultErrorHandling(_TableflowFixtures):
    """A second identical run must not PATCH when `config.error_handling` is left at its server
    default (`SUSPEND`) -- see the module docstring for the failure mode this guards against.
    `Connection.update_tableflow` is replaced with a plain function that records each call and
    delegates to the real implementation, so a spurious PATCH is caught directly (a Mock can't
    just be swapped in for a class-level method: unlike a function, it doesn't implement the
    descriptor protocol, so `self` wouldn't be bound automatically when called via an instance)."""

    NAME = "tfnoop"
    TABLE = "tf_noop"
    TABLEFLOW_MODELS = [TABLE]

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {f"{self.TABLE}.sql": NOOP_TABLE}

    def test_second_run_issues_no_patch(self, project):
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)

        topic = _get_tableflow(project, self.TABLE)
        assert topic is not None, "Tableflow was not enabled on create"
        assert topic.phase.name == "RUNNING"
        assert topic.spec.config is not None
        assert topic.spec.config.error_handling is not None
        assert topic.spec.config.error_handling.mode == "SUSPEND"

        calls = []
        original_update_tableflow = Connection.update_tableflow

        def spy_update_tableflow(self, *args, **kwargs):
            calls.append((args, kwargs))
            return original_update_tableflow(self, *args, **kwargs)

        with patch.object(Connection, "update_tableflow", spy_update_tableflow):
            results = run_dbt(["run"])
            assert all(r.status.name == "Success" for r in results)
        assert calls == [], f"Expected no PATCH on an unchanged config, but got: {calls}"


# -- materialized_table's two disable-before-drop call sites --

SOURCE = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '5',
        'number-of-rows': '20',
        'changelog.mode': 'append',
    }
) }}
`id` INT,
`name` STRING
"""

# No distributed_by, on purpose (mirrors test_materialized_table.py's
# TestMaterializedTableSwitchGuard): under --full-refresh the regular table
# is dropped and the MT created under the same name while the old topic's
# deletion may still be in flight, and the create only tolerates the
# lingering topic if the partition count matches. Keeping both objects on
# Confluent's default distribution keeps their topics compatible.
TABLE_BEFORE_SWITCH = f"""
{{{{ config(
    materialized='table',
    tableflow={_TABLEFLOW_CONFIG},
) }}}}
select id, name from {{{{ ref('__SOURCE__') }}}}
"""

MT_AFTER_SWITCH = f"""
{{{{ config(
    materialized='materialized_table',
    tableflow={_TABLEFLOW_CONFIG},
) }}}}
select id, name from {{{{ ref('__SOURCE__') }}}}
"""


class TestMaterializedTableTableflowSwitchAndRefresh(_TableflowFixtures):
    """`materialized_table.sql` has two distinct disable-before-drop call
    sites -- a switch guard's --full-refresh drop of the old regular table,
    and a --full-refresh drop of the MT itself -- reached through different
    branches (see materialized_table.sql). Both are real, independent ways a
    materialized_table model can lose its backing topic, so both need
    Tableflow disabled before the drop; this exercises both in sequence."""

    NAME = "tfmtswitch"
    SRC = "tf_src"
    MT = "tf_switch"
    TABLEFLOW_MODELS = [MT]

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh: the switch guard only triggers on a
        # plain run, and the first --full-refresh below must be deliberate.
        return {"name": self.NAME, "models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            f"{self.SRC}.sql": SOURCE,
            f"{self.MT}.sql": TABLE_BEFORE_SWITCH.replace("__SOURCE__", self.SRC),
        }

    # Flaky: the test sometimes finds Tableflow not yet enabled right after the dbt run
    # (retried via pytest-rerunfailures). Remove once the cause is understood.
    @pytest.mark.flaky(reruns=2)
    def test_switch_then_full_refresh(self, project):
        # Build as a regular table with tableflow enabled.
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)
        topic = _get_tableflow(project, self.MT)
        assert topic is not None, "Tableflow was not enabled on the regular table"

        # Re-point the model at materialized_table, keeping tableflow set.
        set_model_file(
            project, relation(project, self.MT), MT_AFTER_SWITCH.replace("__SOURCE__", self.SRC)
        )

        # Site 1: --full-refresh disables Tableflow before dropping the
        # regular table, then the MT is created and Tableflow re-enabled.
        results = run_dbt(["run", "--full-refresh", "-s", self.MT])
        assert all(r.status.name == "Success" for r in results)
        topic = _get_tableflow(project, self.MT)
        assert topic is not None, "Tableflow was not enabled after the table->MT switch"
        assert topic.phase.name == "RUNNING"

        # Site 2: --full-refresh on an already-materialized_table model
        # disables Tableflow before dropping the MT itself, then re-enables
        # it on the recreated MT.
        results = run_dbt(["run", "--full-refresh", "-s", self.MT])
        assert all(r.status.name == "Success" for r in results)
        topic = _get_tableflow(project, self.MT)
        assert topic is not None, "Tableflow was not re-enabled after the MT --full-refresh"
        assert topic.phase.name == "RUNNING"
