"""Functional tests for the `tableflow` config.

Unit tests (tests/unit/test_ensure_tableflow_config.py,
test_disable_tableflow_if_enabled.py) mock the driver entirely and never
exercise the Jinja macro wiring, so they can't prove that Tableflow actually
gets enabled against Confluent Cloud, or that the disable-before-drop macro
call sites actually fire correctly end-to-end. These two tests cover exactly
that gap -- deliberately narrow (a single Managed-storage config; every
storage backend/error_handling permutation is already exhaustively covered
by the unit tests):

- TestTableTableflow: enable on create, then the disable-before-drop ->
  recreate -> re-enable cycle on --full-refresh (table.sql's one drop path).
- TestMaterializedTableTableflowSwitchAndRefresh: materialized_table.sql's
  two distinct disable-before-drop call sites -- switching a regular table
  into an MT under --full-refresh, then full-refreshing that MT again.

Requires a Global API key: Tableflow's control-plane routes need one
regardless of the Flink-region pair every other functional test uses (see
MATERIALIZATIONS.md#tableflow). Skipped entirely when
CONFLUENT_GLOBAL_API_KEY/CONFLUENT_GLOBAL_API_SECRET aren't both set.

Notes:
- Both enable_tableflow and disable_tableflow block (by default) until the
  topic reaches a terminal state -- RUNNING, or confirmed gone -- so these
  tests wait on real Confluent Cloud control-plane calls, not just Flink SQL.
  Expect them to take noticeably longer than the rest of the functional suite.
- Naming/leftover-sweep rationale mirrors test_materialized_table.py: names
  are never reused across sessions (a dropped relation's topic and Tableflow
  config can outlive the catalog drop asynchronously), so every name lives in
  a reserved namespace and the first class of the session sweeps stale
  matches.
"""

import os
import re
import time

import pytest
from confluent_sql.exceptions import TableflowTopicNotFoundError

from dbt.tests.util import run_dbt, set_model_file
from tests.functional.adapter._helpers import (
    delete_statements_by_label,
    drop_any_relation,
    relation,
    sweep_stale_test_relations,
    sweep_stale_test_statements,
)
from tests.functional.adapter.fixtures import ConfluentFixtures

pytestmark = pytest.mark.skipif(
    not (os.getenv("CONFLUENT_GLOBAL_API_KEY") and os.getenv("CONFLUENT_GLOBAL_API_SECRET")),
    reason=(
        "Tableflow requires a Global API key -- set CONFLUENT_GLOBAL_API_KEY/"
        "CONFLUENT_GLOBAL_API_SECRET to run these tests"
    ),
)

_RUN_TAG = format(int(time.time()), "08x")
_TEST_RELATION_RE = re.compile(r"^dbttest_tf_(?:table|src|switch)_(?P<tag>[0-9a-f]{8})$")

_TABLEFLOW_CONFIG = "{'formats': 'ICEBERG', 'storage': {'kind': 'Managed'}}"


def _statement_label(dbt_profile_data):
    return dbt_profile_data["test"]["outputs"]["default"]["statement_label"]


def _get_tableflow(project, name):
    """Live Tableflow state for `name`'s backing topic, or None if not enabled."""
    with project.adapter.connection_named("tableflow_check"):
        conn = project.adapter.connections.get_thread_connection()
        try:
            return conn.handle.get_tableflow(name)
        except TableflowTopicNotFoundError:
            return None


def _disable_tableflow_best_effort(project, name):
    with project.adapter.connection_named("tableflow_teardown"):
        conn = project.adapter.connections.get_thread_connection()
        try:
            conn.handle.disable_tableflow(name)
        except TableflowTopicNotFoundError:
            pass


@pytest.fixture(scope="session")
def sweep_leftovers_once():
    """Session-lifetime one-shot gate for the leftover sweep -- see
    test_materialized_table.py's identical fixture for the full rationale."""
    swept = False

    def sweep(project):
        nonlocal swept
        if swept:
            return
        swept = True
        sweep_stale_test_relations(project, _TEST_RELATION_RE, _RUN_TAG)
        sweep_stale_test_statements(project)

    return sweep


class _TableflowFixtures(ConfluentFixtures):
    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        # Suppress the per-test statement cleanup; class_clean_up below (each
        # class defines its own) handles teardown once, after disabling
        # Tableflow -- disabling first keeps the drop from racing an active
        # materialization, per confluent_sql's own recommendation.
        yield

    @pytest.fixture(autouse=True, scope="class")
    def sweep_leftovers(self, project, sweep_leftovers_once):
        sweep_leftovers_once(project)


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
    TABLE = f"dbttest_tf_table_{_RUN_TAG}"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {f"{self.TABLE}.sql": TABLE}

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        _disable_tableflow_best_effort(project, self.TABLE)
        if drop_any_relation(project, self.TABLE):
            delete_statements_by_label(project, _statement_label(dbt_profile_data))

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
    SRC = f"dbttest_tf_src_{_RUN_TAG}"
    MT = f"dbttest_tf_switch_{_RUN_TAG}"

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

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        _disable_tableflow_best_effort(project, self.MT)
        if drop_any_relation(project, self.MT):
            delete_statements_by_label(project, _statement_label(dbt_profile_data))
        project.run_sql(f"drop table if exists {self.SRC}")

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
