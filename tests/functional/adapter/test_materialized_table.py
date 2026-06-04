"""Functional tests for the materialized_table materialization.

Materialized tables use CREATE OR ALTER MATERIALIZED TABLE. Because issuing
CREATE OR ALTER against an *unchanged* MT never converges (Flink leaves it
PENDING), the materialization only issues it when there is a real change:
- new relation -> create,
- detected drift (columns/options) -> CREATE OR ALTER (alter in place),
- --full-refresh -> DROP MATERIALIZED TABLE then recreate,
- otherwise (unchanged) -> skip.
Config is validated (fail-fast) for unsupported options, start_mode, and buckets.

Notes:
- ConfluentFixtures forces models +full_refresh=True, which would make every run a
  recreate. Classes that need to exercise the skip / alter-in-place paths override
  project_config_update to drop that flag.
- Each class uses unique relation names because a schema (Kafka cluster) is shared
  across the suite.
"""

import time

import pytest

from dbt.tests.util import relation_from_name, run_dbt, set_model_file
from tests.functional.adapter.fixtures import ConfluentFixtures


def relation(project, name):
    return project.adapter.Relation.create(identifier=name)


def get_result_by_name(results, name):
    for result in results:
        if result.node.name == name:
            return result
    return None


def _delete_label_statements(project, dbt_profile_data):
    label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
    with project.adapter.connection_named("cleanup"):
        conn = project.adapter.connections.get_thread_connection()
        for stmt in conn.handle.list_statements(label=label):
            project.adapter.delete_statement(stmt.name)


def _drop_mt(project, name, attempts=16, interval=10):
    """Best-effort drop of a materialized table; returns True if it's gone.

    Waits out the transient state ("being modified" / "Could not execute
    DropTable") that occurs while a prior CREATE OR ALTER is still establishing.
    Critically, teardown must drop the MT *before* any statement is deleted: an
    MT stays tied to its defining statement, so deleting that statement while the
    table still exists orphans (wedges) it. The caller therefore only deletes
    statements when this returns True.
    """
    for i in range(attempts):
        try:
            project.run_sql(f"drop materialized table `{name}`")
            return True
        except Exception as e:
            msg = str(e).lower()
            if "does not exist" in msg:
                return True
            if i < attempts - 1 and ("being modified" in msg or "could not execute" in msg):
                time.sleep(interval)
                continue
            return False  # give up; do NOT delete statements (would wedge it)


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
    distributed_by='order_id',
    buckets=4,
    start_mode='RESUME_OR_FROM_BEGINNING',
    with={'key.format': 'avro-registry', 'value.format': 'avro-registry'},
) }}
select order_id, price from {{ ref('__SOURCE__') }}
"""

MT_ADDED_COLUMN = """
{{ config(
    materialized='materialized_table',
    distributed_by='order_id',
    buckets=4,
    start_mode='RESUME_OR_FROM_BEGINNING',
    with={'key.format': 'avro-registry', 'value.format': 'avro-registry'},
) }}
select order_id, price, order_time from {{ ref('__SOURCE__') }}
"""


def _models(src, mt, mt_sql=MT):
    return {f"{src}.sql": SOURCE, f"{mt}.sql": mt_sql.replace("__SOURCE__", src)}


class _MTFixtures(ConfluentFixtures):
    """Base for materialized_table test classes.

    Overrides the per-test clean_up so it does NOT delete statements: an MT stays
    tied to its defining CREATE OR ALTER statement, so deleting that statement
    orphans (wedges) the table. Class teardown instead drops the MT first (which
    waits out the transient establishing state), then deletes statements once the
    table is gone, then drops the source.
    """

    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        yield

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        # Drop the MT first; only delete statements if it's actually gone
        # (deleting the defining statement of a still-present MT wedges it).
        if _drop_mt(project, self.MT):
            _delete_label_statements(project, dbt_profile_data)
        project.run_sql(f"drop table if exists {self.SRC}")


class TestMaterializedTable(_MTFixtures):
    """Happy path: the MT is created and is queryable."""

    NAME = "mattable"
    SRC = "src_happy"
    MT = "mt_happy"

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


class TestMaterializedTableUnchangedRerunSkips(_MTFixtures):
    """Re-running an unchanged MT (without --full-refresh) must SKIP, not issue a
    CREATE OR ALTER (which would never converge)."""

    NAME = "matskip"
    SRC = "src_skip"
    MT = "mt_skip"

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh so the second run exercises the skip path.
        return {"name": self.NAME, "models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT)

    def test_unchanged_rerun_skips(self, project):
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)

        # Second, unchanged run: the MT must be skipped (no CREATE OR ALTER).
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)
        mt = get_result_by_name(results, self.MT)
        assert mt is not None
        assert mt.message == "SKIP"


class TestMaterializedTableEvolvesInPlace(_MTFixtures):
    """A column change is detected as drift and applied in place via
    CREATE OR ALTER (no drop), preserving the materialized table's data/topic."""

    NAME = "matevolve"
    SRC = "src_evolve"
    MT = "mt_evolve"

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh so the change goes through the alter
        # (drift) path rather than drop+recreate.
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

        # Add a column -> drift detected -> CREATE OR ALTER evolves in place.
        set_model_file(
            project, relation(project, self.MT), MT_ADDED_COLUMN.replace("__SOURCE__", self.SRC)
        )
        results = run_dbt(["run", "-s", self.MT])
        assert all(r.status.name == "Success" for r in results)

        # The added column is present -> the table evolved.
        rel = relation_from_name(project.adapter, self.MT)
        project.run_sql(f"select order_id, price, order_time from {rel} limit 1", fetch="one")


class TestMaterializedTableFullRefreshRecreates(_MTFixtures):
    """--full-refresh drops the MT and recreates it (the way to apply changes
    drift detection can't see, e.g. query logic or distribution)."""

    NAME = "matfr"
    SRC = "src_fr"
    MT = "mt_fr"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT)

    def test_full_refresh_recreates(self, project):
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)

        # --full-refresh must drop and recreate without error.
        results = run_dbt(["run", "--full-refresh", "-s", self.MT])
        assert all(r.status.name == "Success" for r in results)


# -- Invalid config models --

MT_INVALID_FRESHNESS = """
{{ config(materialized='materialized_table', freshness_interval='INTERVAL ''1'' HOUR') }}
select order_id, price from {{ ref('src_inval') }}
"""

MT_INVALID_START_MODE = """
{{ config(materialized='materialized_table', start_mode='LATEST') }}
select order_id, price from {{ ref('src_inval') }}
"""

MT_INVALID_BUCKETS = """
{{ config(
    materialized='materialized_table',
    distributed_by='order_id',
    buckets=0,
    with={'key.format': 'avro-registry', 'value.format': 'avro-registry'},
) }}
select order_id, price from {{ ref('src_inval') }}
"""

MT_BUCKETS_NO_DIST = """
{{ config(materialized='materialized_table', buckets=4) }}
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
            "mt_buckets.sql": MT_INVALID_BUCKETS,
            "mt_buckets_no_dist.sql": MT_BUCKETS_NO_DIST,
        }

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        _delete_label_statements(project, dbt_profile_data)
        project.run_sql(f"drop table if exists {self.SRC}")

    def test_invalid_configs_error(self, project):
        results = run_dbt(["run"], expect_pass=False)

        def msg(name):
            r = get_result_by_name(results, name)
            assert r is not None, f"{name} not in results"
            assert r.status.name == "Error", f"{name} expected Error, got {r.status.name}"
            return r.message

        assert "not supported by the 'materialized_table'" in msg("mt_freshness")
        assert "Supported config options are: distributed_by, buckets, with, start_mode" in msg(
            "mt_freshness"
        )
        assert "not a valid value for 'start_mode'" in msg("mt_start_mode")
        assert "must be a positive integer" in msg("mt_buckets")
        assert "requires 'distributed_by'" in msg("mt_buckets_no_dist")
