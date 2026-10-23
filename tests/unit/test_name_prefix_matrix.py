"""statement_name_prefix and resource_name_prefix: every name the adapter builds from them.

Each matrix case is a profile dict; an unset prefix is a missing key, so the real defaults
apply.
"""

import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import jinja2
import pytest
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from dbt.adapters.confluent.connections import ConfluentConnectionManager, ConfluentCredentials
from dbt.adapters.confluent.impl import ConfluentAdapter

MACRO_FILE = (
    Path(__file__).resolve().parents[2]
    / "dbt/include/confluent/macros/get_custom_name/get_custom_alias.sql"
)
BASE_PROFILE = {"database": "env-123", "schema": "lkc-123", "organization_id": "org-123"}
FIXED_UUID = uuid.UUID("12345678-1234-5678-1234-567812345678")

# (profile keys, generated statement, explicit statement_name, fallback prefix, topic)
# Generated names are for model "orders" in project "shop"; the explicit statement_name
# is "pinned-insert"; the fallback is followed by FIXED_UUID.
CASES = {
    "both set": (
        {"statement_name_prefix": "team-", "resource_name_prefix": "alice-"},
        "alice-team-shop-orders",
        "alice-pinned-insert",
        "alice-team-",
        "alice_orders",
    ),
    "both unset": (
        {},
        "dbt-shop-orders",
        "pinned-insert",
        "dbt-",
        "orders",
    ),
    "statement prefix only": (
        {"statement_name_prefix": "team-"},
        "team-shop-orders",
        "pinned-insert",
        "team-",
        "orders",
    ),
    "resource prefix only": (
        {"resource_name_prefix": "alice-"},
        "alice-dbt-shop-orders",
        "alice-pinned-insert",
        "alice-dbt-",
        "alice_orders",
    ),
    "resource prefix, statement prefix explicitly empty": (
        {"statement_name_prefix": "", "resource_name_prefix": "alice-"},
        "alice-shop-orders",
        "alice-pinned-insert",
        "alice-",
        "alice_orders",
    ),
}


def _credentials(profile_keys):
    return ConfluentCredentials.from_dict({**BASE_PROFILE, **profile_keys})


def _adapter(credentials):
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)  # bypass __init__
    adapter.config = SimpleNamespace(credentials=credentials)
    adapter.get_relation = MagicMock()
    return adapter


def _fallback_statement_name(credentials, monkeypatch):
    """The name add_query gives a statement submitted without one."""
    monkeypatch.setattr("dbt.adapters.confluent.connections.fire_event", MagicMock())
    monkeypatch.setattr("dbt.adapters.confluent.connections.uuid.uuid4", lambda: FIXED_UUID)
    execute = MagicMock()
    monkeypatch.setattr("dbt.adapters.confluent.connections._execute_query_with_retry", execute)
    connection = MagicMock()
    connection.name = "model.shop.orders"
    connection.credentials = credentials
    manager = ConfluentConnectionManager.__new__(ConfluentConnectionManager)  # bypass __init__
    manager.get_thread_connection = MagicMock(return_value=connection)
    manager.get_response = MagicMock(return_value=MagicMock(query_id="query-1"))
    manager.add_query("select 1", auto_begin=False)
    return execute.call_args.kwargs["statement_name"]


def _topic_name(credentials, custom_alias_name=None):
    env = jinja2.Environment()
    # Stand-in for dbt's default: the custom alias if set, else the node name.
    env.globals.update(
        target=credentials,
        default__generate_alias_name=lambda custom, node: custom or node.name,
    )
    module = env.from_string(MACRO_FILE.read_text()).make_module()
    return module.confluent__generate_alias_name(
        custom_alias_name, SimpleNamespace(name="orders")
    ).strip()


@pytest.mark.parametrize(
    ("profile_keys", "generated", "explicit", "fallback_prefix", "topic"),
    CASES.values(),
    ids=CASES.keys(),
)
class TestNamePrefixMatrix:
    def test_generated_statement_name(
        self, profile_keys, generated, explicit, fallback_prefix, topic
    ):
        adapter = _adapter(_credentials(profile_keys))
        assert adapter.get_statement_name("orders", "shop") == generated
        assert adapter.get_statement_name("orders", "shop", suffix="-ddl") == f"{generated}-ddl"

    def test_explicit_statement_name(
        self, profile_keys, generated, explicit, fallback_prefix, topic
    ):
        adapter = _adapter(_credentials(profile_keys))
        name = adapter.get_statement_name(
            "orders", "shop", statement_name_override="pinned-insert"
        )
        assert name == explicit

    def test_fallback_statement_name(
        self, monkeypatch, profile_keys, generated, explicit, fallback_prefix, topic
    ):
        name = _fallback_statement_name(_credentials(profile_keys), monkeypatch)
        assert name == f"{fallback_prefix}{FIXED_UUID}"

    def test_topic_name(self, profile_keys, generated, explicit, fallback_prefix, topic):
        credentials = _credentials(profile_keys)
        # statement_name_prefix never reaches topic names; only resource_name_prefix does
        assert _topic_name(credentials) == topic
        assert _topic_name(credentials, "adopted_orders") == topic.replace(
            "orders", "adopted_orders"
        )

    @pytest.mark.parametrize(("version", "suffix"), [("", ""), (".v2", "_v2")])
    def test_unit_test_looks_up_topic(
        self, profile_keys, generated, explicit, fallback_prefix, topic, version, suffix
    ):
        adapter = _adapter(_credentials(profile_keys))
        adapter.get_tested_model_relation(f"model.shop.orders{version}", "env", "cluster")
        adapter.get_relation.assert_called_once_with("env", "cluster", f"{topic}{suffix}")


class TestNamePrefixDetails:
    """Cases the matrix doesn't express."""

    def test_explicit_statement_name_with_suffix(self):
        adapter = _adapter(_credentials({"resource_name_prefix": "alice-"}))
        name = adapter.get_statement_name(
            "orders", "shop", suffix="-check", statement_name_override="base"
        )
        assert name == "alice-base-check"

    def test_statement_names_are_sanitized(self):
        adapter = _adapter(_credentials({}))
        name = adapter.get_statement_name("my_model", "my_project")
        # '_' is illegal in statement names: replaced with '-' plus a hash suffix
        assert name.startswith("dbt-my-project-my-model-")

    @pytest.mark.parametrize(
        ("profile_keys", "expected"),
        [({}, "orders-v2"), ({"resource_name_prefix": "alice-"}, "alice_orders-v2")],
    )
    def test_only_prefix_hyphens_become_underscores(self, profile_keys, expected):
        assert _topic_name(_credentials(profile_keys), "orders-v2") == expected

    def test_missing_unit_test_relation_names_lookup(self):
        adapter = _adapter(_credentials({"resource_name_prefix": "alice-"}))
        adapter.get_relation.return_value = None
        with pytest.raises(DbtDatabaseError, match="'alice_orders'"):
            adapter.get_tested_model_relation("model.shop.orders", "env", "cluster")


class TestResourceNamePrefixValidation:
    @pytest.mark.parametrize("prefix", ["a", "eng-a-", "dbt-adapter-test-", "0x", "a" * 64])
    def test_valid_prefix_accepted(self, prefix):
        assert _credentials({"resource_name_prefix": prefix}).resource_name_prefix == prefix

    @pytest.mark.parametrize(
        "prefix",
        [
            "eng_a_",  # '_' is legal in topics but not in statement names
            "Alice-",  # uppercase is legal in topics but not in statement names
            "eng.a.",  # '.' is legal in topics but not in statement names
            "-eng",  # statement names must start alphanumeric
            "eng a",
            "eng/a",
            "a" * 65,  # over the length cap
        ],
    )
    def test_invalid_prefix_rejected(self, prefix):
        with pytest.raises(DbtRuntimeError, match="Invalid resource_name_prefix"):
            _credentials({"resource_name_prefix": prefix})
