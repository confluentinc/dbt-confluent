import os
from argparse import Namespace
import pytest
from dbt.tests.util import run_dbt
from dbt.events.logging import setup_event_logger
from dbt.tests.fixtures.project import TestProjInfo
from dbt_common.events.event_manager_client import cleanup_event_logger
from dbt.deprecations import reset_deprecations


# seeds/my_seed.csv
# Add a duplicate row so we can check it fails uniqueness test.
my_seed_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
4,Nolan2,1976-05-06T20:21:35
""".lstrip()

# models/my_model.sql
my_model_sql = """
select * from {{ ref('my_seed') }}
"""

# models/my_model.yml
my_model_yml = """
version: 2
models:
  - name: my_model
    columns:
      - name: id
        data_tests:
          - unique # this one will fail
          - not_null
"""


class TestExample:
    """
    Methods in this class will be of two types:
    1. Fixtures defining the dbt "project" for this test case.
       These are scoped to the class, and reused for all tests in the class.
    2. Actual tests, whose names begin with 'test_'.
       These define sequences of dbt commands and 'assert' statements.
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

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "name": "example",
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
            },
        }

    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": my_seed_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model.yml": my_model_yml,
        }

    @pytest.fixture(scope="class")
    def project_setup(
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
        # breakpoint()
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
        # Return whatever is needed later in tests but can only come from fixtures, so we can keep
        # the signatures in the test signature to a minimum.
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
        # project.check_schema_exists()

        yield project

        os.chdir(orig_cwd)
        cleanup_event_logger()
        reset_deprecations()


    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run_seed_test(self, project, caplog):
        """
        Seed, then run, then test. We expect one of the tests to fail
        An alternative pattern is to use pytest "xfail" (see below)
        """
        caplog.set_level("INFO")
        results = run_dbt(["seed", "--full-refresh"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 1
        results = run_dbt(["test"], expect_pass=False)  # expect failing test
        assert len(results) == 2
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["fail", "pass"]

    @pytest.mark.xfail
    def test_build(self, project):
        """Expect a failing test"""
        # do it all
        _results = run_dbt(["build", "--full-refresh"])
