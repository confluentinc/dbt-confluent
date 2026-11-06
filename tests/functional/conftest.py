import os
from argparse import Namespace
from textwrap import dedent

import pytest
from dbt_common.events.event_manager_client import cleanup_event_logger

from dbt.deprecations import reset_deprecations
from dbt.events.logging import setup_event_logger
from dbt.tests.fixtures.project import TestProjInfo
from tests.functional.adapter.isolation import ISOLATION_SLUG_ENV_VAR, record_failure

# Env vars without which no functional test can build its profile. Credentials are handled
# separately (see `missing_functional_env`): either API key pair will do. Other optional ones
# (the compute pools: without CONFLUENT_COMPUTE_POOL_ID statements run in the environment's
# default pool) are not listed: the tests needing them skip themselves.
REQUIRED_FUNCTIONAL_ENV_VARS = (
    "CONFLUENT_TEST_DBNAME",
    "CONFLUENT_ENV_ID",
    "CONFLUENT_ORG_ID",
    "CONFLUENT_CLOUD_PROVIDER",
    "CONFLUENT_CLOUD_REGION",
)

CREDENTIAL_ENV_VAR_PAIRS = (
    ("CONFLUENT_GLOBAL_API_KEY", "CONFLUENT_GLOBAL_API_SECRET"),
    ("CONFLUENT_FLINK_API_KEY", "CONFLUENT_FLINK_API_SECRET"),
)


def missing_functional_env() -> list[str]:
    """Descriptions of what is missing for functional tests to run; empty if they can."""
    missing = [name for name in REQUIRED_FUNCTIONAL_ENV_VARS if not os.getenv(name)]
    if not any(all(os.getenv(name) for name in pair) for pair in CREDENTIAL_ENV_VAR_PAIRS):
        missing.append(
            " and ".join(CREDENTIAL_ENV_VAR_PAIRS[0])
            + " (or CONFLUENT_FLINK_API_KEY and CONFLUENT_FLINK_API_SECRET)"
        )
    return missing


SERIAL_ONLY_MESSAGE = (
    "{nodeid} is marked `serial`: it cannot run under pytest-xdist (it observes the whole "
    "shared environment, so other workers' relations make it fail, or it uses fixed "
    "relation names that concurrent classes would clobber).\n"
    "To run the parallel-safe tests in parallel:\n"
    '    pytest tests/functional -n auto -m "not serial"\n'
    "To run just the serial-only tests (no -n):\n"
    "    pytest tests/functional -m serial"
)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    """Fail `serial` tests immediately, before any fixture or cluster work, when this
    process is an xdist worker."""
    if item.get_closest_marker("serial") and hasattr(item.config, "workerinput"):
        pytest.fail(SERIAL_ONLY_MESSAGE.format(nodeid=item.nodeid), pytrace=False)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """Track failed classes so `class_clean_up` can honor `--keep-failed-resources`, and
    print how to use it with every failure."""
    report = (yield).get_result()
    record_failure(item, report)


@pytest.fixture(scope="class")
def unique_schema(request, prefix):
    """
    Overrides the dbt-tests `schema` fixture.

    A `schema` in confluent cloud is an entire Kafka cluster.
    Since we don't want to create a cluster ad hoc for each test run,
    we expect it to be already present, and the name should be passed
    as an env var. The same env var is used in the test profile fixture.

    Every functional test class depends on this fixture (directly or via the profile and
    project fixtures), so it is also where the functional suite skips, rather than errors, when
    the environment is not configured.
    """
    missing = missing_functional_env()
    if missing:
        pytest.skip(f"functional tests require env vars: {', '.join(missing)}")
    return os.environ["CONFLUENT_TEST_DBNAME"]


@pytest.fixture(scope="class")
def dbt_profile_target(unique_schema):
    """The profile dictionary, used to write out profiles.yml for tests."""
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
    # A Global key works against every route this driver touches, including
    # Tableflow's control-plane routes (see MATERIALIZATIONS.md#tableflow),
    # so it's preferred here whenever configured -- confluent_sql.connect()
    # accepts either pair, but warns (and silently prefers Global anyway) if
    # both are given. Falls back to the Flink-region pair when no Global key
    # is configured (Tableflow's own tests are skipped in that case).
    global_api_key = os.getenv("CONFLUENT_GLOBAL_API_KEY")
    global_api_secret = os.getenv("CONFLUENT_GLOBAL_API_SECRET")
    environment = os.getenv("CONFLUENT_ENV_ID")
    organization_id = os.getenv("CONFLUENT_ORG_ID")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION")
    host = f"https://flink.{cloud_region}.{cloud_provider}.confluent.cloud"
    target = {
        "type": "confluent",
        "threads": 1,
        "host": host,
        "cloud_provider": cloud_provider,
        "cloud_region": cloud_region,
        "compute_pool_id": compute_pool_id,
        "organization_id": organization_id,
        "database": environment,
        "schema": unique_schema,
        "test_schema": unique_schema,
    }
    if global_api_key and global_api_secret:
        target["global_api_key"] = global_api_key
        target["global_api_secret"] = global_api_secret
    else:
        target["flink_api_key"] = flink_api_key
        target["flink_api_secret"] = flink_api_secret
    return target


@pytest.fixture(scope="class")
def schema_yml(unique_schema):
    """Default schema.yml where we need to specify the schema.

    Sources bypass `generate_alias_name`, so the seed's identifier applies the class's relation
    namespace itself, to match the namespaced seed relation (see isolation.py)."""
    return dedent(f"""
        version: 2
        sources:
          - name: raw
            schema: "{unique_schema}"
            tables:
              - name: seed
                identifier: "{{{{ env_var('{ISOLATION_SLUG_ENV_VAR}', '') }}}}{{{{ var('seed_name', 'base') }}}}"
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
