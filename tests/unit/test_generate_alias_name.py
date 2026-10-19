"""Unit tests for confluent__generate_alias_name."""

from pathlib import Path
from types import SimpleNamespace

import jinja2
import pytest

MACRO_FILE = (
    Path(__file__).resolve().parents[2]
    / "dbt/include/confluent/macros/get_custom_name/get_custom_alias.sql"
)


def _generate_alias_name(prefix, custom_alias_name, node_name="orders"):
    env = jinja2.Environment()
    # Stand-in for dbt's default: the custom alias if set, else the node name.
    env.globals.update(
        target=SimpleNamespace(resource_name_prefix=prefix),
        default__generate_alias_name=lambda custom, node: custom or node.name,
    )
    module = env.from_string(MACRO_FILE.read_text()).make_module()
    return module.confluent__generate_alias_name(
        custom_alias_name, SimpleNamespace(name=node_name)
    ).strip()


@pytest.mark.parametrize(
    ("prefix", "custom_alias_name", "expected"),
    [
        ("", None, "orders"),
        ("", "adopted_orders", "adopted_orders"),
        ("eng-a-", None, "eng_a_orders"),
        ("eng-a-", "adopted_orders", "eng_a_adopted_orders"),
    ],
)
def test_prefix_applied_to_generated_and_custom_alias(prefix, custom_alias_name, expected):
    assert _generate_alias_name(prefix, custom_alias_name) == expected


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        # Hyphens in the alias itself are left alone, with or without a prefix
        ("", "orders-v2"),
        ("eng-a-", "eng_a_orders-v2"),
    ],
)
def test_only_prefix_hyphens_replaced(prefix, expected):
    assert _generate_alias_name(prefix, "orders-v2") == expected
