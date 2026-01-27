import os
from argparse import Namespace
from textwrap import dedent

import pytest
from dbt_common.events.event_manager_client import cleanup_event_logger
from fixtures import ConfluentFixtures

from dbt.deprecations import reset_deprecations
from dbt.events.logging import setup_event_logger
from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.fixtures.project import TestProjInfo
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    check_result_nodes_by_name,
    relation_from_name,
    run_dbt,
)


class TestSingularTestsConfluent(ConfluentFixtures, BaseSingularTests):
    pass


class TestEmptyConfluent(ConfluentFixtures):
    pass


class TestSingularTestsEphemeralConfluent(ConfluentFixtures, BaseSingularTestsEphemeral):
    NAME = "singular_test_ephemeral"

    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        """Overrides to handle flink sql and confluent cloud quirks.
        We need to explicitly set schema instead of using {{ target.schema }} in `schema.yml`.
        We also need to avoid creating nested "WITH" in "CREATE VIEW", because Flink SQL
        does not support that.
        """

        ephemeral_simple_sql = dedent("""\
            {{ config(materialized="ephemeral") }}
            select name, id from {{ ref('base') }} where id is not null""")

        return {
            "ephemeral.sql": ephemeral_simple_sql,
            "passing_model.sql": files.test_ephemeral_passing_sql,
            "failing_model.sql": files.test_ephemeral_failing_sql,
            "schema.yml": schema_yml,
        }


class TestEphemeralConfluent(ConfluentFixtures, BaseEphemeral):
    NAME = "ephemeral"

    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        return {
            "ephemeral.sql": files.base_ephemeral_sql,
            "view_model.sql": files.ephemeral_view_sql,
            "table_model.sql": files.ephemeral_table_sql,
            "schema.yml": schema_yml,
        }


class TestGenericTestsConfluent(ConfluentFixtures, BaseGenericTests):
    NAME = "generic_tests"

    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        return {
            "view_model.sql": files.base_view_sql,
            "table_model.sql": files.base_table_sql,
            "schema.yml": schema_yml,
            "schema_view.yml": files.generic_test_view_yml,
            "schema_table.yml": files.generic_test_table_yml,
        }


class TestSimpleMaterializationsConfluent(ConfluentFixtures, BaseSimpleMaterializations):
    NAME = "base"

    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        return {
            "view_model.sql": files.base_view_sql,
            "table_model.sql": files.base_table_sql,
            "swappable.sql": files.base_materialized_var_sql,
            "schema.yml": schema_yml,
        }

    def test_base(self, project):
        """Override the test to avoid incremental materialization.

        In general, dbt suggests not modifying the test itself.
        Here, we are interested in all the things tested, except
        the incremental materialization which we do not support.
        So this test is a copy/paste of the original, up to the
        last step, which is commented here.
        """
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 3

        # names exist in result nodes
        check_result_nodes_by_name(results, ["view_model", "table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(project.adapter, ["base", "view_model", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to view
        if project.test_config.get("require_full_refresh", False):  # required for BigQuery
            results = run_dbt(
                ["run", "--full-refresh", "-m", "swappable", "--vars", "materialized_var: view"]
            )
        else:
            results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: view"])
        assert len(results) == 1

        # check relation types, swappable is view
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "view",
        }
        check_relation_types(project.adapter, expected)

        # DO NOT TEST INCREMENTAL STRATEGY
        # # run_dbt changing materialized_var to incremental
        # results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: incremental"])
        # assert len(results) == 1

        # # check relation types, swappable is table
        # expected = {
        #     "base": "table",
        #     "view_model": "view",
        #     "table_model": "table",
        #     "swappable": "table",
        # }
        # check_relation_types(project.adapter, expected)


@pytest.mark.skip("This adapter does not support incremental materialization.")
class TestIncrementalConfluent(BaseIncremental):
    pass


@pytest.mark.skip("Snapshots not supported - Flink SQL lacks MERGE/UPDATE capabilities.")
class TestSnapshotCheckColsConfluent(BaseSnapshotCheckCols):
    pass


@pytest.mark.skip("Snapshots not supported - Flink SQL lacks MERGE/UPDATE capabilities.")
class TestSnapshotTimestampConfluent(BaseSnapshotTimestamp):
    pass


@pytest.mark.skip("The adapter does not support creating and dropping schemas.")
class TestBaseAdapterMethodConfluent(BaseAdapterMethod):
    pass
