"""Functional tests for an omitted `organization_id` (#237).

Under `auth: oauth` or with a Global API key the driver infers the organization, and a supplied
one is verified against it. These open connections straight through the connection manager
rather than via a dbt project: all that is under test is what the driver makes of the profile's
`organization_id`, and the failure cases never get as far as running anything.

The OAuth tests open a browser, so they need `RUN_OAUTH_TESTS` set (see `skip_unless_oauth_tests_enabled`).
The driver shares one OAuth login per process and refuses a later connection that names a
different organization, so these tests reset it with `oauth_discard_login()` to isolate cases.

Inference only works out to the test organization for an account that has just that one: a
Global key that sees several organizations makes the driver refuse to guess, and an OAuth login
uses the user's default organization, which may not be the test one. The tests that omit
`organization_id` therefore skip, rather than fail, in either case (see
`skip_unless_organization_inferred`). The tests that supply one are unaffected.
"""

import os
from uuid import uuid4

import pytest
from confluent_sql import oauth_discard_login
from confluent_sql.exceptions import InterfaceError, OperationalError, StatementNotFoundError
from confluent_sql.execution_mode import ExecutionMode

from dbt.adapters.confluent.connections import ConfluentConnectionManager, ConfluentCredentials
from dbt.adapters.contracts.connection import Connection
from dbt.tests.util import run_dbt
from tests.functional.adapter._helpers import get_result_by_name
from tests.functional.adapter.fixtures import ConfluentFixtures
from tests.functional.adapter.isolation import class_hash_for, statement_prefix

GLOBAL_KEY = os.getenv("CONFLUENT_GLOBAL_API_KEY")
GLOBAL_SECRET = os.getenv("CONFLUENT_GLOBAL_API_SECRET")
ORG_ID = os.getenv("CONFLUENT_ORG_ID")

requires_global_key_and_org = pytest.mark.skipif(
    not (GLOBAL_KEY and GLOBAL_SECRET and ORG_ID),
    reason="Needs CONFLUENT_GLOBAL_API_KEY, CONFLUENT_GLOBAL_API_SECRET and CONFLUENT_ORG_ID",
)
requires_org = pytest.mark.skipif(not ORG_ID, reason="Needs CONFLUENT_ORG_ID")

# The driver's refusal to infer an organization when its credential sees several. It raises a
# plain OperationalError, so the message text is all there is to match on.
MULTIPLE_ORGANIZATIONS_ERROR = "Multiple organizations"


def open_connection(**profile):
    """Open a real driver connection for a profile pointed at the test environment, with
    `profile` supplying the auth settings and (or not) `organization_id`."""
    credentials = ConfluentCredentials.from_dict(
        {
            "environment_id": os.getenv("CONFLUENT_ENV_ID", "env-unset"),
            "dbname": os.getenv("CONFLUENT_TEST_DBNAME", "unset"),
            "cloud_provider": os.getenv("CONFLUENT_CLOUD_PROVIDER", "aws"),
            "cloud_region": os.getenv("CONFLUENT_CLOUD_REGION", "us-west-2"),
            "compute_pool_id": os.getenv("CONFLUENT_COMPUTE_POOL_ID"),
            **profile,
        }
    )
    connection = Connection(type="confluent", name="org_id_test", credentials=credentials)
    return ConfluentConnectionManager.open(connection)


def inferred_organization_id(handle) -> str:
    """`handle.organization_id`, skipping the test if the account belongs to several
    organizations. Opening the connection doesn't raise that: a global key's omitted organization
    is only inferred on first access of this property."""
    try:
        return handle.organization_id
    except OperationalError as e:
        if MULTIPLE_ORGANIZATIONS_ERROR in str(e):
            pytest.skip(f"Account belongs to several organizations, so none can be inferred: {e}")
        raise


def skip_unless_organization_inferred(**profile) -> None:
    """Skip the calling fixture's tests unless `profile`, with no `organization_id`, infers the
    test organization (`CONFLUENT_ORG_ID`): not for a Global key that sees several organizations
    (skipped by `inferred_organization_id`), and not for an OAuth login whose default organization is a
    different one. For the dbt-run classes, where either would otherwise surface from deep inside
    the `project` fixture's setup rather than from a test body. The connection is closed straight
    away."""
    handle = open_connection(**profile).handle
    try:
        inferred = inferred_organization_id(handle)
    finally:
        handle.close()
    if inferred != ORG_ID:
        pytest.skip(f"Inferred organization {inferred} is not the test organization {ORG_ID}")


@pytest.fixture
def opened():
    """Collects connections opened by a test, closing them afterwards."""
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
    """Run a statement over `handle`. The Flink base URL embeds the organization id, so this
    proves the inferred id is actually usable and not just reported. The statement is deleted
    afterwards; the namespaced name is the backstop if the run dies before that."""
    cursor = handle.cursor(mode=ExecutionMode.SNAPSHOT)
    try:
        cursor.execute("SELECT 1", statement_name=statement_name)
        assert [tuple(row) for row in cursor.fetchall()] == [(1,)]
    finally:
        try:
            handle.delete_statement(statement_name)
        except StatementNotFoundError:
            pass  # never created, or already gone


GLOBAL_KEY_PROFILE = {"global_api_key": GLOBAL_KEY, "global_api_secret": GLOBAL_SECRET}


@requires_global_key_and_org
class TestGlobalKey:
    def test_omitted_organization_id_is_inferred(self, opened, statement_name):
        connection = opened(**GLOBAL_KEY_PROFILE)

        assert inferred_organization_id(connection.handle) == ORG_ID
        assert_can_query(connection.handle, statement_name)

    def test_omitted_organization_id_is_rejected_when_ambiguous(self, opened):
        """The counterpart of the test above, for an account in several organizations: the
        driver refuses to guess. Skips for a single-organization account, where there is
        nothing ambiguous to reject, so each kind of account runs one of the pair."""
        connection = opened(**GLOBAL_KEY_PROFILE)

        try:
            _ = connection.handle.organization_id
        except OperationalError as e:
            assert MULTIPLE_ORGANIZATIONS_ERROR in str(e)
        else:
            pytest.skip("Account belongs to a single organization, so there is nothing ambiguous")

    def test_supplied_organization_id_is_used(self, opened, statement_name):
        connection = opened(**GLOBAL_KEY_PROFILE, organization_id=ORG_ID)

        assert connection.handle.organization_id == ORG_ID
        assert_can_query(connection.handle, statement_name)


class _DbtRunWithoutOrganizationId(ConfluentFixtures):
    """The whole user path: a profiles.yml with no `organization_id` at all. Subclasses supply
    the auth settings via `dbt_profile_target`."""

    EXPECTED_AUTH: str

    @pytest.fixture(scope="class")
    def models(self):
        return {"org_inferred_table.sql": "{{ config(materialized='table') }}\nselect 1 as id"}

    def test_run_succeeds_and_organization_is_inferred(self, project):
        credentials = project.adapter.config.credentials
        assert credentials.organization_id is None
        assert credentials.auth == self.EXPECTED_AUTH

        results = run_dbt(["run"])

        result = get_result_by_name(results, "org_inferred_table")
        assert result is not None, "org_inferred_table not found in run results"
        assert result.message == "Phase.COMPLETED"
        with project.adapter.connection_named("check_org"):
            handle = project.adapter.connections.get_thread_connection().handle
            assert handle.organization_id == ORG_ID


@requires_global_key_and_org
class TestGlobalKeyDbtRunWithoutOrganizationId(_DbtRunWithoutOrganizationId):
    NAME = "org_inference_global"
    EXPECTED_AUTH = "api_key"

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        target = {k: v for k, v in dbt_profile_target.items() if k != "organization_id"}
        assert "global_api_key" in target, "needs the Global key pair, not the Flink one"
        skip_unless_organization_inferred(**GLOBAL_KEY_PROFILE)
        return target


@requires_org
class TestOAuthDbtRunWithoutOrganizationId(_DbtRunWithoutOrganizationId):
    NAME = "org_inference_oauth"
    EXPECTED_AUTH = "oauth"

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, clean_oauth_login):
        """No organization and no API keys. Depending on `clean_oauth_login` (which skips
        unless OAuth tests are enabled) means the gate trips, and any stale login is discarded,
        before the project opens a browser; it is also torn down after the project's connections
        close."""
        keys = ("organization_id", "global_api_key", "global_api_secret")
        target = {k: v for k, v in dbt_profile_target.items() if k not in keys}
        target.pop("flink_api_key", None)
        target.pop("flink_api_secret", None)
        skip_unless_organization_inferred(auth="oauth")
        return {**target, "auth": "oauth"}


class TestFlinkKey:
    def test_omitted_organization_id_is_rejected(self):
        """A Flink-region key can't discover the organization, so the driver refuses up front
        (before any network call, hence the dummy key)."""
        with pytest.raises(InterfaceError, match="Organization ID is required"):
            open_connection(flink_api_key="unused", flink_api_secret="unused")


@requires_org
@pytest.mark.usefixtures("clean_oauth_login")
class TestOAuth:
    def test_omitted_organization_id_is_inferred(self, opened):
        connection = opened(auth="oauth")

        # Omitted, the login's default organization is used, which for a user in several
        # organizations needn't be the test one.
        if connection.handle.organization_id != ORG_ID:
            pytest.skip("The login's default organization is not the test organization")
        assert connection.handle.organization_id == ORG_ID

    def test_matching_organization_id_is_accepted(self, opened):
        connection = opened(auth="oauth", organization_id=ORG_ID)

        assert connection.handle.organization_id == ORG_ID

    def test_unreachable_organization_id_is_rejected_by_name(self, opened):
        """The point of still passing `organization_id`: a clear error naming the org, not
        'environment not found'."""
        bogus = "00000000-0000-0000-0000-000000000000"

        # Drop the login the earlier tests established: a process holds a single organization
        # identity, so otherwise this would fail the driver's one-identity guard
        # (InterfaceError) instead of being checked against the login. No connection is open
        # here (the earlier tests' were closed), as discarding requires. The failed login
        # leaves nothing behind, and the class fixture discards again at the end regardless.
        oauth_discard_login()

        with pytest.raises(OperationalError, match=bogus):
            opened(auth="oauth", organization_id=bogus)
