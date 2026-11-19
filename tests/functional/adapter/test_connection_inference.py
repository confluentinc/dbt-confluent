"""Functional tests for a profile that omits `organization_id`, `cloud_provider` and
`cloud_region`, leaving the driver to infer them (confluent-sql 0.9.0, #310/#311).

With a Global API key or `auth: oauth` the driver infers the organization (when the credential
reaches exactly one) and the cloud settings (from the Kafka cluster named by the profile's
`dbname`), and checks any that are supplied. These tests open connections straight through the
connection manager rather than via a dbt project: all that is under test is what the driver
makes of the profile, and the failure cases never get as far as running anything. One class at
the end runs a real `dbt run` from a minimal profile.

The OAuth tests open a browser, so they need `RUN_OAUTH_TESTS` set (see
`skip_unless_oauth_tests_enabled`). The driver shares one OAuth login per process and refuses a
later connection that names a different organization, so each OAuth class starts and ends with
`oauth_discard_login()`. They are also marked `serial`: a login listens on a fixed local port,
and xdist's `loadscope` can put two classes of this module on different workers, whose logins
would collide (`PORT_IN_USE`). Run them without `-n` (`pytest -m serial`).

Which tests can run depends on the account:
- An account that reaches several organizations can't infer one: the tests that expect inference
  skip, and `test_ambiguous_organization_is_explained` runs instead. A single-organization
  account does the reverse.
- The tests that supply `organization_id` work for either.
"""

import os
from uuid import uuid4

import pytest
from confluent_sql import oauth_discard_login
from confluent_sql.exceptions import (
    AmbiguousOrganizationError,
    ClusterMismatchError,
    OperationalError,
    StatementNotFoundError,
)
from confluent_sql.execution_mode import ExecutionMode

from dbt.adapters.confluent.connections import ConfluentConnectionManager, ConfluentCredentials
from dbt.adapters.contracts.connection import Connection
from dbt.adapters.exceptions import FailedToConnectError
from dbt.tests.util import run_dbt
from tests.functional.adapter._helpers import get_result_by_name
from tests.functional.adapter.fixtures import ConfluentFixtures
from tests.functional.adapter.isolation import class_hash_for, statement_prefix

GLOBAL_KEY = os.getenv("CONFLUENT_GLOBAL_API_KEY")
GLOBAL_SECRET = os.getenv("CONFLUENT_GLOBAL_API_SECRET")
# Required by every functional test (see REQUIRED_FUNCTIONAL_ENV_VARS): what inference must find.
ORG_ID = os.getenv("CONFLUENT_ORG_ID")
CLOUD_PROVIDER = os.getenv("CONFLUENT_CLOUD_PROVIDER")
CLOUD_REGION = os.getenv("CONFLUENT_CLOUD_REGION")

requires_global_key = pytest.mark.skipif(
    not (GLOBAL_KEY and GLOBAL_SECRET),
    reason="Needs CONFLUENT_GLOBAL_API_KEY and CONFLUENT_GLOBAL_API_SECRET",
)

GLOBAL_KEY_PROFILE = {"global_api_key": GLOBAL_KEY, "global_api_secret": GLOBAL_SECRET}
OAUTH_PROFILE = {"auth": "oauth"}


def open_connection(**profile):
    """Open a real driver connection, through the connection manager, for a profile pointed at
    the test environment and cluster. `profile` supplies the credentials and whichever of
    `organization_id` / `cloud_provider` / `cloud_region` the test supplies: none by default."""
    credentials = ConfluentCredentials.from_dict(
        {
            "environment_id": os.environ["CONFLUENT_ENV_ID"],
            "dbname": os.environ["CONFLUENT_TEST_DBNAME"],
            "compute_pool_id": os.getenv("CONFLUENT_COMPUTE_POOL_ID"),
            **profile,
        }
    )
    connection = Connection(type="confluent", name="inference_test", credentials=credentials)
    return ConfluentConnectionManager.open(connection)


def skip_if_ambiguous_organization(open_it):
    """Run `open_it()`, skipping the test if the account reaches several organizations (so no
    organization can be inferred for it). Returns what `open_it()` returned."""
    try:
        return open_it()
    except FailedToConnectError as e:
        if isinstance(e.__cause__, AmbiguousOrganizationError):
            pytest.skip(f"Account reaches several organizations, so none can be inferred: {e}")
        raise


@pytest.fixture
def opened(unique_schema):
    """Collects connections opened by a test, closing them afterwards.

    Depends on `unique_schema` for its side effect: that fixture is where the functional suite
    skips, rather than errors, when the environment isn't configured. Without it, these
    connection-only tests would try to connect with whatever happened to be set (the Global key
    alone, say) instead of skipping."""
    connections = []

    def open_and_track(**profile):
        connection = open_connection(**profile)
        connections.append(connection)
        return connection

    yield open_and_track

    for connection in connections:
        connection.handle.close()


@pytest.fixture(scope="class")
def clean_oauth_login(skip_unless_oauth_tests_enabled):
    """Start and finish a test class without a login in the process, so neither an earlier
    class's identity leaks in nor this class's leaks out. A class's tests otherwise share one
    login (one browser sign-in). Connections must already be closed, as
    `oauth_discard_login()` requires: each test's `opened` teardown does that, and a dbt
    project fixture depending on this one is torn down first."""
    oauth_discard_login()
    yield
    oauth_discard_login()


@pytest.fixture
def statement_name(request) -> str:
    """A statement name in this test class's isolation namespace, so a statement leaked by a
    killed run is swept as stale by a later session like any other test statement."""
    class_hash = class_hash_for(request.module.__name__, request.cls.__name__)
    return f"{statement_prefix(class_hash=class_hash)}{uuid4()}"


def assert_can_query(handle, statement_name: str) -> None:
    """Run a statement over `handle`. The Flink endpoint embeds the organization and the cloud
    settings, so this proves what was inferred is actually usable and not just reported. The
    statement is deleted afterwards; the namespaced name is the backstop if the run dies before
    that."""
    cursor = handle.cursor(mode=ExecutionMode.SNAPSHOT)
    try:
        cursor.execute("SELECT 1", statement_name=statement_name)
        assert [tuple(row) for row in cursor.fetchall()] == [(1,)]
    finally:
        try:
            handle.delete_statement(statement_name)
        except StatementNotFoundError:
            pass  # never created, or already gone


class _InferenceTests:
    """What a Global key and an OAuth login both do, as the driver now treats them alike.
    Subclasses supply the credentials as `PROFILE`."""

    PROFILE: dict

    def test_everything_omitted_is_inferred(self, opened, statement_name):
        """No organization and no cloud settings: all found by the driver."""
        connection = skip_if_ambiguous_organization(lambda: opened(**self.PROFILE))

        assert connection.handle.organization_id == ORG_ID
        assert_can_query(connection.handle, statement_name)

    def test_ambiguous_organization_is_explained(self, opened):
        """The counterpart of the test above, for an account that reaches several
        organizations: the driver refuses to guess, and the error says which profile field to
        set and lists the candidates. Skips for a single-organization account, where there is
        nothing ambiguous, so each kind of account runs one of the pair."""
        try:
            opened(**self.PROFILE)
        except FailedToConnectError as e:
            cause = e.__cause__
            assert isinstance(cause, AmbiguousOrganizationError)
            assert "`organization_id`" in str(e)
            assert ORG_ID in cause.organization_ids, "the test organization must be a candidate"
            assert all(org_id in str(e) for org_id in cause.organization_ids)
        else:
            pytest.skip("Account reaches a single organization, so there is nothing ambiguous")

    def test_supplied_organization_id_is_accepted(self, opened, statement_name):
        connection = opened(**self.PROFILE, organization_id=ORG_ID)

        assert connection.handle.organization_id == ORG_ID
        assert_can_query(connection.handle, statement_name)

    def test_cloud_settings_are_inferred_from_the_schema(self, opened, statement_name):
        """Only `organization_id` supplied: the provider and region come from the Kafka cluster
        named by the profile's `dbname`, and the resulting endpoint serves queries."""
        connection = opened(**self.PROFILE, organization_id=ORG_ID)

        # TODO(confluent-sql#322): once the driver exposes the resolved values, assert them
        # directly (`connection.handle.cloud_provider == CLOUD_PROVIDER`, likewise region)
        # rather than inferring them from a query that happens to succeed. Same in the next test.
        assert_can_query(connection.handle, statement_name)

    @pytest.mark.parametrize("supplied", ["cloud_provider", "cloud_region"])
    def test_half_a_cloud_pair_is_completed(self, opened, statement_name, supplied):
        value = {"cloud_provider": CLOUD_PROVIDER, "cloud_region": CLOUD_REGION}[supplied]

        connection = opened(**self.PROFILE, organization_id=ORG_ID, **{supplied: value})

        assert_can_query(connection.handle, statement_name)

    def test_wrong_cloud_region_is_explained(self, opened):
        """A supplied setting is checked against the cluster, but only when the other half of
        the pair is omitted: a complete provider+region pair is trusted without a lookup. The
        error names the profile field and both values."""
        wrong_region = "nowhere-1"

        with pytest.raises(FailedToConnectError) as exc_info:
            opened(**self.PROFILE, organization_id=ORG_ID, cloud_region=wrong_region)

        cause = exc_info.value.__cause__
        assert isinstance(cause, ClusterMismatchError)
        assert cause.parameters == ["cloud_region"]
        message = str(exc_info.value)
        assert "`cloud_region`" in message
        assert wrong_region in message
        assert CLOUD_REGION in message


@requires_global_key
class TestGlobalKeyInference(_InferenceTests):
    PROFILE = GLOBAL_KEY_PROFILE


@pytest.mark.serial
@pytest.mark.usefixtures("clean_oauth_login")
class TestOAuthInference(_InferenceTests):
    PROFILE = OAUTH_PROFILE

    def test_unreachable_organization_id_is_rejected_by_name(self, opened):
        """The point of still passing `organization_id`: a clear error naming the organization,
        not 'environment not found'."""
        bogus = "00000000-0000-0000-0000-000000000000"

        # Drop the login the earlier tests established: a process holds a single organization
        # identity, so otherwise this would fail the driver's one-identity guard
        # (InterfaceError) instead of being checked against the login. No connection is open
        # here (the earlier tests' were closed), as discarding requires. The failed login
        # leaves nothing behind, and the class fixture discards again at the end regardless.
        oauth_discard_login()

        with pytest.raises(OperationalError, match=bogus):
            opened(**OAUTH_PROFILE, organization_id=bogus)


class _DbtRunFromMinimalProfile(ConfluentFixtures):
    """The whole user path: a profiles.yml with no `organization_id`, `cloud_provider` or
    `cloud_region` at all. Subclasses supply the credentials via `dbt_profile_target`."""

    EXPECTED_AUTH: str

    @pytest.fixture(scope="class")
    def models(self):
        return {"inferred_table.sql": "{{ config(materialized='table') }}\nselect 1 as id"}

    def test_run_succeeds_and_everything_is_inferred(self, project):
        credentials = project.adapter.config.credentials
        assert credentials.auth == self.EXPECTED_AUTH
        assert credentials.organization_id is None
        assert credentials.cloud_provider is None
        assert credentials.cloud_region is None

        results = run_dbt(["run"])

        result = get_result_by_name(results, "inferred_table")
        assert result is not None, "inferred_table not found in run results"
        assert result.message == "Phase.COMPLETED"
        with project.adapter.connection_named("check_inferred"):
            handle = project.adapter.connections.get_thread_connection().handle
            assert handle.organization_id == ORG_ID


def minimal_profile(dbt_profile_target, credentials: dict) -> dict:
    """`dbt_profile_target` with the organization, cloud settings and all credentials removed,
    then `credentials` put back. Skips the class, from the fixture that builds the profile (so
    the skip comes before the `project` fixture runs dbt), if the account can't infer."""
    omitted = (
        "organization_id",
        "cloud_provider",
        "cloud_region",
        "host",
        "global_api_key",
        "global_api_secret",
        "flink_api_key",
        "flink_api_secret",
    )
    target = {k: v for k, v in dbt_profile_target.items() if k not in omitted}
    skip_if_ambiguous_organization(lambda: open_connection(**credentials).handle.close())
    return {**target, **credentials}


@requires_global_key
class TestGlobalKeyDbtRunFromMinimalProfile(_DbtRunFromMinimalProfile):
    NAME = "inference_global"
    EXPECTED_AUTH = "api_key"

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        return minimal_profile(dbt_profile_target, GLOBAL_KEY_PROFILE)


@pytest.mark.serial
class TestOAuthDbtRunFromMinimalProfile(_DbtRunFromMinimalProfile):
    NAME = "inference_oauth"
    EXPECTED_AUTH = "oauth"

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, clean_oauth_login):
        """Depending on `clean_oauth_login` (which skips unless OAuth tests are enabled) means
        the gate trips, and any stale login is discarded, before the project opens a browser;
        it is also torn down after the project's connections close."""
        return minimal_profile(dbt_profile_target, OAUTH_PROFILE)
