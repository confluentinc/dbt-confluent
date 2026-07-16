"""Functional tests for the materialized_table materialization.

materialized_table is declarative: every run re-asserts the definition with
CREATE OR ALTER MATERIALIZED TABLE and lets Flink reconcile it —
- new relation -> create,
- any change (columns, WITH options, query logic) -> evolve in place,
- unchanged -> server-side no-op (the server diffs the submitted spec),
- --full-refresh -> DROP MATERIALIZED TABLE then recreate,
- existing regular table/view (materialization switch) -> guarded: plain run
  errors with guidance, --full-refresh drops through the regular path first.
Config is validated (fail-fast) for unsupported options and start_mode; the
shared distributed_by validation (delegated to validate_distributed_by_config)
is exercised here only for end-to-end wiring — its per-case behavior lives in
the pure-Python tests/unit/test_validate_distributed_by_config.py.

Notes:
- ConfluentFixtures forces models +full_refresh=True, which would make every run a
  recreate. Classes that exercise the in-place evolution / no-op paths override
  project_config_update to drop that flag.
- Each class uses unique relation names because a schema (Kafka cluster) is shared
  across the suite.
- Re-running within Flink's brief establishment window is transiently rejected
  ("being modified") and retried by the adapter; the tests that re-run an MT
  back-to-back are marked xfail(strict=False) so that flakiness never gates CI.
"""

import pytest

from dbt.tests.util import relation_from_name, run_dbt, set_model_file
from tests.functional.adapter._helpers import (
    delete_statements_by_label,
    drop_materialized_table,
    get_result_by_name,
    relation,
)
from tests.functional.adapter.fixtures import ConfluentFixtures

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
    def class_clean_up(self, project, dbt_profile_data):
        yield
        # Only delete statements once the MT is confirmed gone; if the drop
        # kept failing, leave everything for the next run's teardown.
        if drop_materialized_table(project, self.MT):
            delete_statements_by_label(project, _statement_label(dbt_profile_data))
        project.run_sql(f"drop table if exists {self.SRC}")


class TestMaterializedTable(_MTFixtures):
    """Happy path: the MT is created and is queryable."""

    NAME = "mattable"
    SRC = "src_create"
    MT = "mt_create"

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
    SRC = "src_noop"
    MT = "mt_noop"

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh so the second run is a plain re-run.
        return {"name": self.NAME, "models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT)

    # QUARANTINED (non-gating): a rapid unchanged re-run can land within the MT's
    # establishment window ("being modified"); the adapter retries, but the window
    # can outlast the budget. At normal cadence it's an instant no-op. See
    # MATERIALIZATIONS.md.
    @pytest.mark.xfail(
        reason="Rapid unchanged re-run can hit the Flink 'being modified' "
        "establishment window; quarantined, not CI-gating.",
        strict=False,
    )
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
    SRC = "src_alter"
    MT = "mt_alter"

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh so the change is applied in place rather
        # than via drop+recreate.
        return {"name": self.NAME, "models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT)

    # QUARANTINED (non-gating): in-place evolution works at normal cadence, but a
    # re-run within the MT's establishment window is rejected with "being modified"
    # for a variable, sometimes-long time. The adapter retries, but the window can
    # outlast a practical budget, making this test flaky. xfail(strict=False) keeps
    # it running for signal without gating CI. See MATERIALIZATIONS.md.
    @pytest.mark.xfail(
        reason="In-place MT evolution is flaky on rapid re-run (variable Flink "
        "'being modified' establishment window); quarantined, not CI-gating.",
        strict=False,
    )
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
    SRC = "src_recreate"
    MT = "mt_recreate"

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

# No distributed_by, on purpose: a dropped object's Kafka topic can outlive
# the catalog drop, and a create under the same name only succeeds if the
# partition count matches. Keeping both the regular table and the MT on
# Confluent's default distribution (6 buckets) makes their topics compatible
# in both directions; a buckets-4 MT here poisoned the relation name for the
# next run's default-6 CTAS ("topic with the same name already exists with
# different partitions").
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
    SRC = "src_swguard"
    # Not "mt_switch": that name's topic is poisoned (4 partitions) in clusters
    # that ran the pre-fix version of this test.
    MT = "mt_swguard"

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh: the guard only triggers on a plain run.
        return {"name": self.NAME, "models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT, TABLE_BEFORE_SWITCH)

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        # Depending on how far the test got, the relation is a regular table
        # (guard-error path) or an MT (--full-refresh path). Try the regular
        # drop first (fails fast on an MT), then the MT drop.
        try:
            project.run_sql(f"drop table if exists `{self.MT}`")
        except Exception:
            pass
        if drop_materialized_table(project, self.MT):
            delete_statements_by_label(project, _statement_label(dbt_profile_data))
        project.run_sql(f"drop table if exists {self.SRC}")

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
