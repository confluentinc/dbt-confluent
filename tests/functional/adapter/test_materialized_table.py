"""Functional tests for the materialized_table materialization.

Covers the declarative lifecycle — create, in-place evolve, server-side
no-op, --full-refresh recreate (see MATERIALIZATIONS.md) — plus both
materialization-switch guards (regular relation under an MT model, and a
leftover MT under a regular model). Config validation is exercised only for
end-to-end wiring; per-case behavior lives in the pure-Python tests under
tests/unit/.

Notes:
- ConfluentFixtures forces models +full_refresh=True, which would make every run a
  recreate. Classes that exercise the in-place evolution / no-op paths override
  project_config_update to drop that flag.
- Each class uses unique relation names, suffixed with a per-session tag: the
  schema (Kafka cluster) is shared, a dropped relation's Kafka topic outlives
  the catalog drop asynchronously (minutes to a day-plus), and its Schema
  Registry subjects are not deleted at all. Reusing a name across runs races
  that teardown: a recreate can bind to the lingering topic's old schema
  ("Column types of query result and sink ... do not match"), and an
  in-flight deletion can make an existence check pass and then evaporate.
- Because names are never reused, leftovers from failed teardowns or
  hard-killed runs would accumulate forever. Every name therefore lives in a
  reserved namespace (`dbttest_` prefix + fixed stem + hex epoch-seconds tag)
  and the first class of each session sweeps stale matches — see
  _MTFixtures.sweep_leftovers and _helpers.sweep_stale_test_relations.
- Re-running within Flink's brief establishment window is transiently rejected
  ("being modified") and retried by the adapter (unit-tested in
  tests/unit/test_execute_query_with_retry.py); the back-to-back re-runs here
  don't hit that window in practice, so the tests run unquarantined.
"""

import re
import time

import pytest

from dbt.tests.util import relation_from_name, run_dbt, set_model_file
from tests.functional.adapter._helpers import (
    capture_submitted_statement_properties,
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
    r"^dbttest_(?:mt|src)_(?:create|noop|alter|recreate|swguard|revswitch|contract"
    r"|contractorder|stmtprops|stmtpropstuned|stmtpropsplain)_(?P<tag>[0-9a-f]{8})$"
)

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

# start_mode deliberately uses an argument-bearing form: the argument is only
# lexically checked client-side and passed through verbatim (see
# render_start_mode), so the server must accept the rendered interval literal —
# a bare keyword would leave that path untested. RESUME_OR_FROM_NOW(now minus
# 7 days) covers all test data (generated seconds earlier) and keeps the same
# RESUME_* resume-on-evolution semantics as the server default, so the
# lifecycle assertions are unaffected.
MT = """
{{ config(
    materialized='materialized_table',
    distributed_by={'columns': ['order_id'], 'buckets': 4},
    start_mode="RESUME_OR_FROM_NOW(INTERVAL '7' DAY)",
    with={'key.format': 'avro-registry', 'value.format': 'avro-registry'},
) }}
select order_id, price from {{ ref('__SOURCE__') }}
"""

MT_ADDED_COLUMN = """
{{ config(
    materialized='materialized_table',
    distributed_by={'columns': ['order_id'], 'buckets': 4},
    start_mode="RESUME_OR_FROM_NOW(INTERVAL '7' DAY)",
    with={'key.format': 'avro-registry', 'value.format': 'avro-registry'},
) }}
select order_id, price, order_time from {{ ref('__SOURCE__') }}
"""


def _models(src, mt, mt_sql=MT):
    return {f"{src}.sql": SOURCE, f"{mt}.sql": mt_sql.replace("__SOURCE__", src)}


def _statement_label(dbt_profile_data):
    return dbt_profile_data["test"]["outputs"]["default"]["statement_label"]


@pytest.fixture(scope="session")
def sweep_leftovers_once():
    """Session-lifetime one-shot gate for the leftover sweep.

    The sweep needs dbt's `project` fixture, which is class-scoped, so it
    can't run from a session fixture (or a pytest_sessionstart hook) directly.
    Instead each class's sweep_leftovers fixture passes its project in, and
    the closure ensures only the first call actually sweeps.
    """
    swept = False

    def sweep(project):
        nonlocal swept
        if swept:
            return
        swept = True
        sweep_stale_test_relations(project, _TEST_RELATION_RE, _RUN_TAG)
        sweep_stale_test_statements(project)

    return sweep


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
    def sweep_leftovers(self, project, sweep_leftovers_once):
        # Once per pytest session (this class fixture bridges the class-scoped
        # `project` to the session-scoped one-shot gate): reclaim relations
        # and statements leaked by previous sessions' failed teardowns or
        # hard-killed runs. Old-tag names are never recreated, so sweeping
        # them cannot race anything this session does.
        sweep_leftovers_once(project)

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

        # Regression for #81: a model with no contract enforced must keep
        # rendering a plain `AS SELECT` — no explicit column-definition or
        # PRIMARY KEY block should be emitted.
        ddl_rows = project.run_sql(f"SHOW CREATE MATERIALIZED TABLE {self.MT}", fetch="all")
        ddl = ddl_rows[0][0]
        assert "PRIMARY KEY" not in ddl.upper(), (
            f"Materialized table without a contract should not have a PRIMARY KEY:\n{ddl}"
        )

        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 2


# Feature test for #81 — a materialized_table model with an enforced dbt
# contract must render an explicit column-definition block ahead of
# DISTRIBUTED BY/WITH/AS SELECT, including a PRIMARY KEY (...) NOT ENFORCED
# constraint, so the resulting table's key can back snapshot queries (see
# MATERIALIZATIONS.md#materialized-table).
MT_CONTRACT_PK_COLUMN = "order_id"

MT_WITH_CONTRACT = """
{{ config(
    materialized='materialized_table',
    contract={'enforced': true},
) }}
select order_id, price from {{ ref('__SOURCE__') }}
"""

MT_CONTRACT_MODELS_YML = """
models:
  - name: __MT__
    constraints:
      - type: primary_key
        columns: [__PK_COLUMN__]
        expression: "NOT ENFORCED"
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
      - name: price
        data_type: decimal(10,2)
"""


class TestMaterializedTableWithContract(_MTFixtures):
    """Feature test for #81: a materialized_table model with
    `contract={'enforced': true}` and a `primary_key` constraint declared in
    its schema.yml gets an explicit column-definition block and a
    `PRIMARY KEY (...) NOT ENFORCED` clause in the emitted DDL, matching
    Confluent's `CREATE MATERIALIZED TABLE (cols..., PRIMARY KEY(...) NOT
    ENFORCED) DISTRIBUTED BY (...) WITH (...) AS SELECT ...` grammar."""

    NAME = "matcontract"
    SRC = f"dbttest_src_contract_{_RUN_TAG}"
    MT = f"dbttest_mt_contract_{_RUN_TAG}"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            f"{self.SRC}.sql": SOURCE,
            f"{self.MT}.sql": MT_WITH_CONTRACT.replace("__SOURCE__", self.SRC),
            "models.yml": MT_CONTRACT_MODELS_YML.replace("__MT__", self.MT).replace(
                "__PK_COLUMN__", MT_CONTRACT_PK_COLUMN
            ),
        }

    def test_contract_renders_primary_key(self, project):
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results), (
            "dbt run failed for a materialized_table with an enforced contract"
        )

        ddl_rows = project.run_sql(f"SHOW CREATE MATERIALIZED TABLE {self.MT}", fetch="all")
        ddl = ddl_rows[0][0]
        assert f"PRIMARY KEY (`{MT_CONTRACT_PK_COLUMN}`) NOT ENFORCED" in ddl, (
            f"Expected primary key constraint not found in materialized table DDL:\n{ddl}"
        )


# Regression for the PR #84 review: an enforced contract must re-project the
# AS SELECT to match schema.yml's *declared* column order, not just validate
# names/types (get_assert_columns_equivalent does the latter but is
# order-blind). order_id is the PK, so it must stay first in both the select
# and schema.yml -- Flink requires key columns at the beginning of the table
# schema, an unrelated rule (see MATERIALIZATIONS.md's distributed_by column
# ordering note). The regression is in the other two columns: schema.yml
# declares price before order_time, while the model's select does the
# opposite -- without re-projecting (get_select_subquery, mirroring `table`'s
# create.sql), Flink binds the AS SELECT positionally against the declared
# column list, pairing the TIMESTAMP order_time value with the DECIMAL price
# column and vice versa.
MT_CONTRACT_REORDER_SQL = """
{{ config(
    materialized='materialized_table',
    contract={'enforced': true},
) }}
select order_id, order_time, price from {{ ref('__SOURCE__') }}
"""

MT_CONTRACT_REORDERED_MODELS_YML = """
models:
  - name: __MT__
    constraints:
      - type: primary_key
        columns: [__PK_COLUMN__]
        expression: "NOT ENFORCED"
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
      - name: price
        data_type: decimal(10,2)
      - name: order_time
        data_type: timestamp(3)
"""


class TestMaterializedTableContractColumnReorder(_MTFixtures):
    """schema.yml's declared column order must win over the model SQL's
    select order in the emitted DDL's AS SELECT, not just its column
    definitions."""

    NAME = "matcontractorder"
    SRC = f"dbttest_src_contractorder_{_RUN_TAG}"
    MT = f"dbttest_mt_contractorder_{_RUN_TAG}"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            f"{self.SRC}.sql": SOURCE,
            f"{self.MT}.sql": MT_CONTRACT_REORDER_SQL.replace("__SOURCE__", self.SRC),
            "models.yml": MT_CONTRACT_REORDERED_MODELS_YML.replace("__MT__", self.MT).replace(
                "__PK_COLUMN__", MT_CONTRACT_PK_COLUMN
            ),
        }

    def test_contract_select_matches_declared_column_order(self, project):
        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results), (
            "dbt run failed for a materialized_table whose schema.yml column "
            "order differs from its model SQL's select order -- the AS SELECT "
            "is likely being bound positionally instead of by declared name"
        )

        rel = relation_from_name(project.adapter, self.MT)
        rows = project.run_sql(
            f"select order_id, price, order_time from {rel} limit 1", fetch="one"
        )
        order_id, price, order_time = rows[0]
        assert isinstance(order_id, int), (
            f"order_id should be a BIGINT, got {order_id!r} -- columns were "
            "likely bound positionally instead of by declared name"
        )
        assert price is not None and order_time is not None, (
            "price/order_time should not be null -- columns were likely bound "
            "positionally instead of by declared name"
        )


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
    would silently skip, leaving Flink maintaining the old defining query).

    --full-refresh drops the MT via drop_relation's IS_MATERIALIZED pre-check
    (a plain DROP TABLE would be silently accepted but phantom-drop the MT,
    blocking the recreate), but the recreate itself is then blocked by the
    platform: dropping a relation does not delete its Schema Registry
    subjects, and the `table` snapshot CTAS registers a keyless schema while
    the MT registered a keyed one, so the lingering '<name>-value' subject is
    incompatible. The adapter surfaces this with recovery guidance appended
    (delete the subject, or use a different relation name); this test pins
    that error and that the MT itself was still dropped. If Confluent starts
    deleting subjects on drop, the expect_pass=False run below will start
    succeeding — revisit then (the switch would just work)."""

    NAME = "matrevswitch"
    SRC = f"dbttest_src_revswitch_{_RUN_TAG}"
    MT = f"dbttest_mt_revswitch_{_RUN_TAG}"

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Drop the forced +full_refresh: the guard only triggers on a plain run.
        return {"name": self.NAME, "models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        # Same shape as the forward switch test, mirrored.
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

        # --full-refresh: the MT is dropped (via drop_relation's pre-check),
        # then the snapshot CTAS is rejected against the MT's lingering
        # Schema Registry subject (see class docstring) and the adapter
        # surfaces the enriched, actionable error.
        results = run_dbt(["run", "--full-refresh", "-s", self.MT], expect_pass=False)
        r = get_result_by_name(results, self.MT)
        assert r is not None
        assert r.status.name == "Error"
        assert "Schema Registry subject" in r.message
        assert "delete the lingering subject" in r.message

        # The MT itself is gone — the guard and the MT drop routing worked;
        # only the platform-blocked recreate failed.
        row = project.run_sql(
            "select IS_MATERIALIZED from INFORMATION_SCHEMA.`TABLES` "
            f"where TABLE_SCHEMA = '{project.test_schema}' and TABLE_NAME = '{self.MT}'",
            fetch="one",
        )
        assert not row


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

# Representative wiring case: materialized_table now also runs through the
# generic MATERIALIZATION_CONFIG_KEYS check (validate_materialization_config),
# same as every other materialization -- `connector` is a real dbt-confluent
# key, just not one materialized_table reads. Exhaustive per-key/per-
# materialization coverage lives in tests/unit/test_validate_materialization_config.py.
MT_INVALID_UNSUPPORTED_KEY = """
{{ config(materialized='materialized_table', connector='faker') }}
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
            "mt_unsupported_key.sql": MT_INVALID_UNSUPPORTED_KEY,
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

        unsupported_msg = msg("mt_unsupported_key")
        assert "connector" in unsupported_msg
        assert "materialized_table" in unsupported_msg


# -- Statement properties --

IDLE_TIMEOUT_PROPERTY = "sql.tables.scan.idle-timeout"
IDLE_TIMEOUT_VALUE = "30 s"

# Same distributed_by/with shape as MT above (a known-working combination for
# this materialization) -- materialized_table has a single statement (the
# CREATE OR ALTER itself) to target, unlike streaming_table's separate DDL +
# long-running INSERT.
MT_TUNED_STATEMENT_PROPERTIES = f"""
{{{{ config(
    materialized='materialized_table',
    distributed_by={{'columns': ['order_id'], 'buckets': 4}},
    with={{'key.format': 'avro-registry', 'value.format': 'avro-registry'}},
    statement_properties={{'{IDLE_TIMEOUT_PROPERTY}': '{IDLE_TIMEOUT_VALUE}'}},
) }}}}
select order_id, price from {{{{ ref('__SOURCE__') }}}}
"""

MT_PLAIN_STATEMENT_PROPERTIES = """
{{ config(
    materialized='materialized_table',
    distributed_by={'columns': ['order_id'], 'buckets': 4},
    with={'key.format': 'avro-registry', 'value.format': 'avro-registry'},
) }}
select order_id, price from {{ ref('__SOURCE__') }}
"""


class TestMaterializedTableStatementProperties(_MTFixtures):
    """`statement_properties` is wired into materialized_table's single DDL
    statement (there's no separate INSERT to target, unlike streaming_table
    -- see tests/functional/adapter/test_statement_properties.py).

    That DDL statement is submitted under a unique per-run name and reaped by
    the driver the instant the CREATE OR ALTER completes (see
    materialized_table.sql / MATERIALIZATIONS.md#deterministic-statement-names),
    so a post-run get_statement() lookup would always 404. Its properties are
    captured mid-flight instead, via capture_submitted_statement_properties.
    """

    NAME = "mtstmtprops"
    SRC = f"dbttest_src_stmtprops_{_RUN_TAG}"
    MT_TUNED = f"dbttest_mt_stmtpropstuned_{_RUN_TAG}"
    MT_PLAIN = f"dbttest_mt_stmtpropsplain_{_RUN_TAG}"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            f"{self.SRC}.sql": SOURCE,
            f"{self.MT_TUNED}.sql": MT_TUNED_STATEMENT_PROPERTIES.replace("__SOURCE__", self.SRC),
            f"{self.MT_PLAIN}.sql": MT_PLAIN_STATEMENT_PROPERTIES.replace("__SOURCE__", self.SRC),
        }

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        for mt in (self.MT_TUNED, self.MT_PLAIN):
            drop_any_relation(project, mt)
        delete_statements_by_label(project, _statement_label(dbt_profile_data))
        project.run_sql(f"drop table if exists {self.SRC}")

    def test_statement_properties_land_on_the_ddl_statement(self, project, monkeypatch):
        captured = capture_submitted_statement_properties(monkeypatch)

        results = run_dbt(["run"])
        assert all(r.status.name == "Success" for r in results)

        # Flink statement names are sanitized (underscores -> hyphens; see
        # dbt.adapters.confluent.naming.sanitize_statement_name), so match
        # against the hyphenated form of each relation name.
        tuned_marker = self.MT_TUNED.replace("_", "-")
        plain_marker = self.MT_PLAIN.replace("_", "-")

        [tuned_properties] = [
            properties for name, properties in captured.items() if tuned_marker in name
        ]
        assert tuned_properties.get(IDLE_TIMEOUT_PROPERTY) == IDLE_TIMEOUT_VALUE, (
            f"Expected '{IDLE_TIMEOUT_PROPERTY}' on {self.MT_TUNED}'s DDL statement, "
            f"got properties: {tuned_properties}"
        )

        # Control: a model that never sets `statement_properties` must NOT
        # show our configured value for the same key. Without this, the
        # assertion above could pass by coincidence if Flink's default
        # idle-timeout ever happened to equal IDLE_TIMEOUT_VALUE, rather than
        # because our plumbing actually threaded the configured value through.
        [plain_properties] = [
            properties for name, properties in captured.items() if plain_marker in name
        ]
        assert plain_properties.get(IDLE_TIMEOUT_PROPERTY) != IDLE_TIMEOUT_VALUE, (
            f"{self.MT_PLAIN}'s DDL statement (no statement_properties configured) reports "
            f"{IDLE_TIMEOUT_PROPERTY}={IDLE_TIMEOUT_VALUE!r}, the same value the tuned model "
            f"configures explicitly -- that assertion wouldn't actually prove our config took "
            f"effect. Got properties: {plain_properties}"
        )
