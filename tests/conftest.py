import os
from argparse import Namespace
from textwrap import dedent

import pytest
from dbt_common.events.event_manager_client import cleanup_event_logger

from dbt.deprecations import reset_deprecations
from dbt.events.logging import setup_event_logger
from dbt.tests.fixtures.project import TestProjInfo

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
    environment = os.getenv("CONFLUENT_ENV_ID")
    organization_id = os.getenv("CONFLUENT_ORG_ID")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION")
    dbname = os.getenv("CONFLUENT_TEST_DBNAME")
    host = f"https://flink.{cloud_region}.{cloud_provider}.confluent.cloud"
    target = {
        "type": "confluent",
        "threads": 1,
        "host": host,
        "cloud_provider": cloud_provider,
        "cloud_region": cloud_region,
        "compute_pool_id": compute_pool_id,
        "organization_id": organization_id,
        "flink_api_key": flink_api_key,
        "flink_api_secret": flink_api_secret,
        "database": environment,
        "schema": dbname,
        "test_schema": dbname,
    }
    return target


@pytest.fixture(scope="class")
def unique_schema(request, prefix):
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


@pytest.fixture(scope="class")
def schema_yml(unique_schema):
    return dedent(f"""
        version: 2
        sources:
          - name: raw
            schema: "{unique_schema}"
            tables:
              - name: seed
                identifier: "{{{{ var('seed_name', 'base') }}}}"
    """)


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
