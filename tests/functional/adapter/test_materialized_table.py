"""Functional tests for the materialized_table materialization.

materialized_table is declarative: every run re-asserts the definition with
CREATE OR ALTER MATERIALIZED TABLE and lets Flink reconcile it —
- new relation -> create,
- any change (columns, WITH options, query logic) -> evolve in place (note:
  an evolution resets a stateful query's processing state — the MT here is a
  stateless projection, which evolves seamlessly; see MATERIALIZATIONS.md),
- unchanged -> server-side no-op (the server diffs the submitted spec;
  probe-verified against a stateful positive control),
- --full-refresh -> DROP MATERIALIZED TABLE then recreate,
- existing regular table/view (materialization switch) -> guarded: plain run
  errors with guidance, --full-refresh drops through the regular path first,
- reverse switch (regular model over a leftover MT) -> the drift check's
  IS_MATERIALIZED detection errors on a plain run; --full-refresh drops the
  MT via drop_relation's IS_MATERIALIZED pre-check.
Config is validated (fail-fast) for unsupported options and start_mode; the
shared distributed_by validation (delegated to validate_distributed_by_config)
is exercised here only for end-to-end wiring — its per-case behavior lives in
the pure-Python tests/unit/test_validate_distributed_by_config.py.

Notes:
- ConfluentFixtures forces models +full_refresh=True, which would make every run a
  recreate. Classes that exercise the in-place evolution / no-op paths override
  project_config_update to drop that flag.
- Each class uses unique relation names, suffixed with a per-session tag: the
  schema (Kafka cluster) is shared, and a dropped relation's Kafka topic and
  Schema Registry schemas outlive the catalog drop asynchronously (minutes to
  a day-plus). Reusing a name across runs races that deletion — observed as a
  lingering topic resurfacing as an inferred table that trips the switch
  guard, as a recreate binding to the lingering topic's old schema ("Column
  types of query result and sink ... do not match"), and as an in-flight
  deletion making an existence check pass and then evaporate.
- Because names are never reused, leftovers from failed teardowns or
  hard-killed runs would accumulate forever. Every name therefore lives in a
  reserved namespace (`dbttest_` prefix + fixed stem + hex epoch-seconds tag)
  and the first class of each session sweeps stale matches — see
  _MTFixtures.sweep_leftovers and _helpers.sweep_stale_test_relations.
- Re-running within Flink's brief establishment window is transiently rejected
  ("being modified") and retried by the adapter (unit-tested in
  tests/unit/test_add_query_retry.py); empirically the back-to-back re-runs here
  never hit that window, so the tests run unquarantined.
"""

import re
import time

import pytest

from dbt.tests.util import relation_from_name, run_dbt, set_model_file
from tests.functional.adapter._helpers import (
    delete_statements_by_label,
    drop_any_relation,
    get_result_by_name,
    relation,
    sweep_stale_test_relations,
    sweep_stale_test_statements,
)
from tests.functional.adapter.fixtures import ConfluentFixtures

# Suffix for every relation name in this module, fresh per pytest session:
# reusing a name across runs races Kafka's asynchronous topic deletion from
# the previous run's teardown (see the module docstring). Hex epoch-seconds
# rather than random hex so the leftover sweep can tell how old a stale name
# is and leave concurrent sessions' relations alone.
_RUN_TAG = format(int(time.time()), "08x")

# The reserved name shape for every relation in this module. The sweeper
# deletes ANY stale catalog object matching it, so it must never overlap a
# name a human or another tool would plausibly pick: literal reserved prefix,
# one of the fixed stems, and the hex session tag.
_TEST_RELATION_RE = re.compile(
    r"^dbttest_(?:mt|src)_(?:create|noop|alter|recreate|swguard|revswitch)_(?P<tag>[0-9a-f]{8})$"
)

_leftovers_swept = False

# A bounded faker source (number-of-rows) so the MT refresh settles quickly.
SOURCE = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '5',
        'number-of-rows': '100',
        'changelog.mode': 'append',
    }
) }}
order_id BIGINT,
price DECIMAL(10, 2),
order_time TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,
PRIMARY KEY(`order_id`) NOT ENFORCED
"""

MT = """
{{ config(
    materialized='materialized_table',
    distributed_by={'columns': ['order_id'], 'buckets': 4},
    start_mode='RESUME_OR_FROM_BEGINNING',
    with={'key.format': 'avro-registry', 'value.format': 'avro-registry'},
) }}
select order_id, price from {{ ref('__SOURCE__') }}
"""

MT_ADDED_COLUMN = """
{{ config(
    materialized='materialized_table',
    distributed_by={'columns': ['order_id'], 'buckets': 4},
    start_mode='RESUME_OR_FROM_BEGINNING',
    with={'key.format': 'avro-registry', 'value.format': 'avro-registry'},
) }}
select order_id, price, order_time from {{ ref('__SOURCE__') }}
"""


def _models(src, mt, mt_sql=MT):
    return {f"{src}.sql": SOURCE, f"{mt}.sql": mt_sql.replace("__SOURCE__", src)}


def _statement_label(dbt_profile_data):
    return dbt_profile_data["test"]["outputs"]["default"]["statement_label"]


class _MTFixtures(ConfluentFixtures):
    """Base for materialized_table test classes.

    Overrides the per-test clean_up to do nothing: MT cleanup happens once, at
    class teardown, which drops the MT first (waiting out the transient
    establishing state), then deletes statements, then drops the source.
    """

    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        yield

    @pytest.fixture(autouse=True, scope="class")
    def sweep_leftovers(self, project):
        # Once per pytest session (dbt's `project` fixture is class-scoped,
        # so this is a class fixture behind a module flag): reclaim relations
        # and statements leaked by previous sessions' failed teardowns or
        # hard-killed runs. Old-tag names are never recreated, so sweeping
        # them cannot race anything this session does.
        global _leftovers_swept
        if not _leftovers_swept:
            _leftovers_swept = True
            sweep_stale_test_relations(project, _TEST_RELATION_RE, _RUN_TAG)
            sweep_stale_test_statements(project)
        yield

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        # Only delete statements once the relation is confirmed gone; if the
        # drop kept failing, leave the statements too as a debugging trail —
        # a later session's sweep_leftovers reclaims the lot once it ages
        # past the concurrency gate. drop_any_relation rather than the MT
        # drop because a class can end with the name held by a regular table
        # (e.g. the switch-guard error path).
        if drop_any_relation(project, self.MT):
            delete_statements_by_label(project, _statement_label(dbt_profile_data))
        project.run_sql(f"drop table if exists {self.SRC}")


class TestMaterializedTable(_MTFixtures):
    """Happy path: the MT is created and is queryable."""

    NAME = "mattable"
    SRC = f"dbttest_src_create_{_RUN_TAG}"
    MT = f"dbttest_mt_create_{_RUN_TAG}"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT)

    def test_materialized_table_created(self, project):
        results = run_dbt(["run"])
        names = {r.node.name for r in results}
        assert {self.SRC, self.MT} == names
        for r in results:
            assert r.status.name == "Success"

        rel = relation_from_name(project.adapter, self.MT)
        project.run_sql(f"select order_id, price from {rel} limit 1", fetch="one")

        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 2


class TestMaterializedTableUnchangedRerunNoop(_MTFixtures):
    """Re-running an unchanged MT is a server-side no-op: dbt re-asserts the
    same CREATE OR ALTER and the server diffs the spec, leaving the table,
    its data, and its query state untouched."""

    NAME = "matnoop"
    SRC = f"dbttest_src_noop_{_RUN_TAG}"
    MT = f"dbttest_mt_noop_{_RUN_TAG}"

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh so the second run is a plain re-run.
        return {"name": self.NAME, "models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT)

    def test_unchanged_rerun_is_noop(self, project):
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)

        # Second, unchanged run: re-assert the same definition; succeeds as a no-op.
        results = run_dbt(["run", "-s", self.MT])
        assert all(r.status.name == "Success" for r in results)


class TestMaterializedTableEvolvesInPlace(_MTFixtures):
    """A changed definition is applied in place via CREATE OR ALTER (no drop),
    preserving the materialized table's data/topic. (A column change is verifiable;
    query-logic changes go through the identical path.)"""

    NAME = "matevolve"
    SRC = f"dbttest_src_alter_{_RUN_TAG}"
    MT = f"dbttest_mt_alter_{_RUN_TAG}"

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh so the change is applied in place rather
        # than via drop+recreate.
        return {"name": self.NAME, "models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT)

    def test_query_change_evolves_in_place(self, project):
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)

        # Add a column -> CREATE OR ALTER evolves the table in place.
        set_model_file(
            project, relation(project, self.MT), MT_ADDED_COLUMN.replace("__SOURCE__", self.SRC)
        )
        results = run_dbt(["run", "-s", self.MT])
        assert all(r.status.name == "Success" for r in results)

        # The added column is present -> the table evolved.
        rel = relation_from_name(project.adapter, self.MT)
        project.run_sql(f"select order_id, price, order_time from {rel} limit 1", fetch="one")


class TestMaterializedTableFullRefreshRecreates(_MTFixtures):
    """--full-refresh drops the MT and recreates it from scratch (e.g. to change
    distribution, which can't be altered in place)."""

    NAME = "matfr"
    SRC = f"dbttest_src_recreate_{_RUN_TAG}"
    MT = f"dbttest_mt_recreate_{_RUN_TAG}"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT)

    def test_full_refresh_recreates(self, project):
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)

        # --full-refresh must drop and recreate without error.
        results = run_dbt(["run", "--full-refresh", "-s", self.MT])
        assert all(r.status.name == "Success" for r in results)


# -- Materialization switch --

TABLE_BEFORE_SWITCH = """
{{ config(materialized='table') }}
select order_id, price from {{ ref('__SOURCE__') }}
"""

# No distributed_by, on purpose: under --full-refresh the regular table is
# dropped and the MT created under the same name while the old topic's
# deletion is still in flight, and the create only tolerates the lingering
# topic if the partition count matches. Keeping both objects on Confluent's
# default distribution (6 buckets) keeps their topics compatible ("a topic
# with the same name already exists with different partitions" otherwise).
MT_AFTER_SWITCH = """
{{ config(materialized='materialized_table') }}
select order_id, price from {{ ref('__SOURCE__') }}
"""


class TestMaterializedTableSwitchGuard(_MTFixtures):
    """A model that already exists as a regular table cannot be converted to a
    materialized table in place (Confluent restriction): a plain run fails with
    a clear error before any DDL; --full-refresh drops the regular table (and
    its statements) through the regular drop path and creates the MT."""

    NAME = "matswitch"
    SRC = f"dbttest_src_swguard_{_RUN_TAG}"
    MT = f"dbttest_mt_swguard_{_RUN_TAG}"

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh: the guard only triggers on a plain run.
        return {"name": self.NAME, "models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT, TABLE_BEFORE_SWITCH)

    def test_switch_guard(self, project):
        # Build as a regular table first.
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)

        # Re-point the model at the materialized_table materialization.
        set_model_file(
            project, relation(project, self.MT), MT_AFTER_SWITCH.replace("__SOURCE__", self.SRC)
        )

        # Plain run: the switch is rejected with guidance, before any DDL.
        results = run_dbt(["run", "-s", self.MT], expect_pass=False)
        r = get_result_by_name(results, self.MT)
        assert r is not None
        assert r.status.name == "Error"
        assert "cannot be converted" in r.message
        assert "--full-refresh" in r.message

        # --full-refresh: the regular table is dropped and the MT created.
        results = run_dbt(["run", "--full-refresh", "-s", self.MT])
        assert all(r.status.name == "Success" for r in results)
        row = project.run_sql(
            "select IS_MATERIALIZED from INFORMATION_SCHEMA.`TABLES` "
            f"where TABLE_SCHEMA = '{project.test_schema}' and TABLE_NAME = '{self.MT}'",
            fetch="one",
        )
        assert row is not None and row[0][0] == "YES"


class TestMaterializedTableReverseSwitch(_MTFixtures):
    """A model that already exists as a materialized table cannot be adopted
    by the drop-and-recreate materializations: a plain run as `table` must
    fail with a dedicated error (an MT reports TABLE_TYPE='BASE TABLE', so
    without the IS_MATERIALIZED check the drift check would pass and the run
    would silently skip, leaving Flink maintaining the old defining query);
    --full-refresh drops the MT via drop_relation's IS_MATERIALIZED pre-check
    (a plain DROP TABLE would be silently accepted but phantom-drop the MT,
    blocking the recreate) and creates the regular table."""

    NAME = "matrevswitch"
    SRC = f"dbttest_src_revswitch_{_RUN_TAG}"
    MT = f"dbttest_mt_revswitch_{_RUN_TAG}"

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh: the guard only triggers on a plain run.
        return {"name": self.NAME, "models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        # Same shape as the forward switch test, mirrored: both models keep
        # the default distribution so the recreate under the same name
        # tolerates the dropped MT's lingering topic.
        yield _models(self.SRC, self.MT, MT_AFTER_SWITCH)

    def test_reverse_switch(self, project):
        # Build as a materialized table first.
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)

        # Re-point the model at the regular table materialization.
        set_model_file(
            project,
            relation(project, self.MT),
            TABLE_BEFORE_SWITCH.replace("__SOURCE__", self.SRC),
        )

        # Plain run: rejected with guidance, before any DDL against the MT.
        results = run_dbt(["run", "-s", self.MT], expect_pass=False)
        r = get_result_by_name(results, self.MT)
        assert r is not None
        assert r.status.name == "Error"
        assert "materialized table" in r.message
        assert "--full-refresh" in r.message

        # --full-refresh: the MT is dropped (fallback path) and a regular
        # table is created in its place.
        results = run_dbt(["run", "--full-refresh", "-s", self.MT])
        assert all(r.status.name == "Success" for r in results)
        row = project.run_sql(
            "select IS_MATERIALIZED from INFORMATION_SCHEMA.`TABLES` "
            f"where TABLE_SCHEMA = '{project.test_schema}' and TABLE_NAME = '{self.MT}'",
            fetch="one",
        )
        assert row is not None and row[0][0] == "NO"


# -- Invalid config models --

MT_INVALID_FRESHNESS = """
{{ config(materialized='materialized_table', freshness_interval='INTERVAL ''1'' HOUR') }}
select order_id, price from {{ ref('src_inval') }}
"""

MT_INVALID_START_MODE = """
{{ config(materialized='materialized_table', start_mode='LATEST') }}
select order_id, price from {{ ref('src_inval') }}
"""

# Representative wiring case: an invalid distributed_by mapping must surface the
# shared validate_distributed_by_config error through the MT materialization. The
# exhaustive per-shape cases live in tests/unit/test_validate_distributed_by_config.py.
MT_INVALID_DISTRIBUTED_BY = """
{{ config(
    materialized='materialized_table',
    distributed_by={'columns': ['order_id'], 'buckets': 0},
    with={'key.format': 'avro-registry', 'value.format': 'avro-registry'},
) }}
select order_id, price from {{ ref('src_inval') }}
"""


class TestMaterializedTableInvalidConfig(ConfluentFixtures):
    """Config validation fails fast with a clear compiler error before any DDL."""

    NAME = "matinval"
    SRC = "src_inval"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "src_inval.sql": SOURCE,
            "mt_freshness.sql": MT_INVALID_FRESHNESS,
            "mt_start_mode.sql": MT_INVALID_START_MODE,
            "mt_dist.sql": MT_INVALID_DISTRIBUTED_BY,
        }

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        delete_statements_by_label(project, _statement_label(dbt_profile_data))
        project.run_sql(f"drop table if exists {self.SRC}")

    def test_invalid_configs_error(self, project):
        results = run_dbt(["run"], expect_pass=False)

        def msg(name):
            r = get_result_by_name(results, name)
            assert r is not None, f"{name} not in results"
            assert r.status.name == "Error", f"{name} expected Error, got {r.status.name}"
            return r.message

        assert "not supported by the 'materialized_table'" in msg("mt_freshness")
        assert "Supported config options are: distributed_by, with, start_mode" in msg(
            "mt_freshness"
        )
        assert "not a valid value for 'start_mode'" in msg("mt_start_mode")
        assert "must be a positive integer" in msg("mt_dist")
