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

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        """
        The original fixture tries to delete the test schema, make this a no-op.
        """
        yield

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


class TestSingularTestsConfluent(ConfluentFixtures, BaseSingularTests):
    pass


class TestEmptyConfluent(ConfluentFixtures, BaseEmpty):
    pass


@pytest.mark.skip(
    "This test needs the ability to create and drop schemas, this adapter does not support that."
)
class TestBaseAdapterMethodConfluent(BaseAdapterMethod):
    pass


@pytest.mark.skip(
    "This test needs the ability to rename tables, this adapter does not support that. "
    "One of the three tests in here that uses views does pass."
)
class TestSimpleMaterializationsConfluent(ConfluentFixtures, BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self, unique_schema):
        return {
            "view_model.sql": files.base_view_sql,
            "table_model.sql": files.base_table_sql,
            "swappable.sql": files.base_materialized_var_sql,
            "schema.yml": dedent(f"""\
                version: 2
                sources:
                  - name: raw
                    schema: "{unique_schema}"
                    tables:
                      - name: seed
                        identifier: "{{{{ var('seed_name', 'base') }}}}" """),
        }

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "name": "base",
            "models": {
                "+materialized": "view",
                # Here we need to specify the schema, or tests will
                # receive a Relation with an empty string as default.
                "+schema": unique_schema,
            },
            "seeds": {
                # Here we need to specify the schema, or tests will
                # receive a Relation with an empty string as default.
                "+schema": unique_schema,
                "+full_refresh": True,
            },
        }


class TestSingularTestsEphemeralConfluent(ConfluentFixtures, BaseSingularTestsEphemeral):
    @pytest.fixture(scope="class")
    def models(self, unique_schema):
        """Override models fixture to explicitly set schema instead of using {{ target.schema }}"""
        return {
            "ephemeral.sql": files.ephemeral_with_cte_sql,
            "passing_model.sql": files.test_ephemeral_passing_sql,
            "failing_model.sql": files.test_ephemeral_failing_sql,
            "schema.yml": dedent(f"""\
                version: 2
                sources:
                  - name: raw
                    schema: "{unique_schema}"
                    tables:
                      - name: seed
                        identifier: "{{{{ var('seed_name', 'base') }}}}" """),
        }

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "name": "singular_tests_ephemeral",
            "models": {
                # Here we need to specify the schema, or tests will
                # receive a Relation with an empty string as default.
                "+schema": unique_schema,
            },
            "seeds": {
                # Here we need to specify the schema, or tests will
                # receive a Relation with an empty string as default.
                "+schema": unique_schema,
                "+full_refresh": True,
            },
        }


class TestEphemeralConfluent(ConfluentFixtures, BaseEphemeral):
    pass


class TestIncrementalConfluent(ConfluentFixtures, BaseIncremental):
    pass


class TestGenericTestsConfluent(ConfluentFixtures, BaseGenericTests):
    pass


class TestSnapshotCheckColsConfluent(ConfluentFixtures, BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampConfluent(ConfluentFixtures, BaseSnapshotTimestamp):
    pass
