import os
from argparse import Namespace
from textwrap import dedent

import pytest
from dbt_common.events.event_manager_client import cleanup_event_logger

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


class ConfluentFixtures:
    """
    Use this class to replace dbt tests' fixtures with custom ones that work with this adapter.
    """

    def get_project_config_update(self, name, schema):
        return {
            "name": name,
            "models": {
                "+materialized": "view",
                # Here we need to specify the schema, or tests will
                # receive a Relation with an empty string as default.
                "+schema": schema,
            },
            "seeds": {
                # Here we need to specify the schema, or tests will
                # receive a Relation with an empty string as default.
                "+schema": schema,
                "+full_refresh": True,
            },
        }

    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix):
        """
        Overrides the dbt-tests `schema` fixture.

        A `schema` in confluent cloud is an entire Kafka cluster.
        Since we don't want to create a cluster ad hoc for each test run,
        we expect it to be already present, and the name should be passed
        as an env var. The same env var is used in the test profile fixture.
        """
        dbname = os.getenv("CONFLUENT_TEST_DBNAME")
        if not dbname:
            raise ValueError("CONFLUENT_TEST_DBNAME env var needs to be set")
        return dbname

    def _drop_relation_if_exists(self, project, identifier):
        """Helper to drop a relation if it exists."""
        with project.adapter.connection_named("__test_cleanup"):
            relation = project.adapter.Relation.create(
                database=project.database,
                schema=project.test_schema,
                identifier=identifier,
                type="table"  # Try as table first
            )
            existing = project.adapter.get_relation(
                database=project.database,
                schema=project.test_schema,
                identifier=identifier
            )
            if existing:
                try:
                    project.adapter.drop_relation(existing)
                except Exception as e:
                    print(f"Failed to drop {identifier}: {e}")

    def get_relations_to_cleanup(self):
        return []

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        """
        Since we can't drop schemas, we need to manually drop specific tables/views
        created by each test to ensure a clean state for the next test.

        Each test class should override get_relations_to_cleanup() to specify
        what it creates.
        """
        yield
        # Drop known test relations - each test can override this list
        for identifier in self.get_relations_to_cleanup():
            self._drop_relation_if_exists(project, identifier)

    @pytest.fixture(scope="class")
    def project_setup(
        self,
        initialization,
        clean_up_logging,
        project_root,
        profiles_root,
        request,
        unique_schema,
        profiles_yml,
        adapter,
        shared_data_dir,
        test_data_dir,
        logs_dir,
        test_config,
    ):
        # Override the fixture so it doesn't try to create and drop the schema.
        # The implementation should be the same as upstream, only without the
        # create_schema and drop_schema commands.
        log_flags = Namespace(
            LOG_PATH=logs_dir,
            LOG_FORMAT="json",
            LOG_FORMAT_FILE="json",
            USE_COLORS=False,
            USE_COLORS_FILE=False,
            LOG_LEVEL="info",
            LOG_LEVEL_FILE="debug",
            DEBUG=False,
            LOG_CACHE_EVENTS=False,
            QUIET=False,
            LOG_FILE_MAX_BYTES=1000000,
        )
        setup_event_logger(log_flags)
        orig_cwd = os.getcwd()
        os.chdir(project_root)
        project = TestProjInfo(
            project_root=project_root,
            profiles_dir=profiles_root,
            adapter_type=adapter.type(),
            test_dir=request.fspath.dirname,
            shared_data_dir=shared_data_dir,
            test_data_dir=test_data_dir,
            test_schema=unique_schema,
            database=adapter.config.credentials.database,
            test_config=test_config,
        )

        yield project

        os.chdir(orig_cwd)
        cleanup_event_logger()
        reset_deprecations()

    @pytest.fixture(scope="class")
    def schema_yml(self, unique_schema):
        return dedent(f"""
            version: 2
            sources:
              - name: raw
                schema: "{unique_schema}"
                tables:
                  - name: seed
                    identifier: "{{{{ var('seed_name', 'base') }}}}"
        """)


class TestSingularTestsConfluent(ConfluentFixtures, BaseSingularTests):
    pass


class TestEmptyConfluent(ConfluentFixtures, BaseEmpty):
    pass


class TestSingularTestsEphemeralConfluent(ConfluentFixtures, BaseSingularTestsEphemeral):
    def get_relations_to_cleanup(self):
        """Specify relations created by this test that need cleanup."""
        return ["base"]

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


@pytest.mark.skip("This adapter does not support renaming tables.")
class TestSimpleMaterializationsConfluent(ConfluentFixtures, BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self, schema_yaml):
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
