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


class TestSingularTestsConfluent(ConfluentFixtures, BaseSingularTests):
    pass


class TestEmptyConfluent(ConfluentFixtures, BaseEmpty):
    pass


class TestSingularTestsEphemeralConfluent(ConfluentFixtures, BaseSingularTestsEphemeral):
    def get_relations_to_cleanup(self):
        """Specify relations created by this test that need cleanup."""
        return ["base", "passing_model", "failing_model"]

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

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return self.get_project_config_update("singular_tests_ephemeral", unique_schema)


class TestIncrementalConfluent(ConfluentFixtures, BaseIncremental):
    def get_relations_to_cleanup(self):
        """Specify relations created by this test that need cleanup."""
        return ["base", "added", "incremental"]

    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        return {"incremental.sql": files.incremental_sql, "schema.yml": schema_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return self.get_project_config_update("incremental", unique_schema)


@pytest.mark.skip("Snapshots not supported - Flink SQL lacks MERGE/UPDATE capabilities.")
class TestSnapshotCheckColsConfluent(ConfluentFixtures, BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return self.get_project_config_update("snapshot_strategy_check_cols", unique_schema)


@pytest.mark.skip("Snapshots not supported - Flink SQL lacks MERGE/UPDATE capabilities.")
class TestSnapshotTimestampConfluent(ConfluentFixtures, BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return self.get_project_config_update("snapshot_strategy_timestamp", unique_schema)


@pytest.mark.skip("The adapter does not support creating and dropping schemas.")
class TestBaseAdapterMethodConfluent(BaseAdapterMethod):
    pass


# @pytest.mark.skip("This adapter does not support renaming tables.")
class TestSimpleMaterializationsConfluent(ConfluentFixtures, BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        return {
            "view_model.sql": files.base_view_sql,
            "table_model.sql": files.base_table_sql,
            "swappable.sql": files.base_materialized_var_sql,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return self.get_project_config_update("base", unique_schema)


@pytest.mark.skip("This adapter does not support renaming tables.")
class TestEphemeralConfluent(ConfluentFixtures, BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        return {
            "ephemeral.sql": files.base_ephemeral_sql,
            "view_model.sql": files.ephemeral_view_sql,
            "table_model.sql": files.ephemeral_table_sql,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return self.get_project_config_update("ephemeral", unique_schema)


@pytest.mark.skip("This adapter does not support renaming tables.")
class TestGenericTestsConfluent(ConfluentFixtures, BaseGenericTests):
    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        return {
            "view_model.sql": files.base_view_sql,
            "table_model.sql": files.base_table_sql,
            "schema.yml": schema_yml,
            "schema_view.yml": files.generic_test_view_yml,
            "schema_table.yml": files.generic_test_table_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return self.get_project_config_update("generic_tests", unique_schema)
