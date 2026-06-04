"""Unit tests for the `get_distributed_by_clause` Jinja macro.

Confluent auto-assigns a default bucket count to every distributed table, so
`SHOW CREATE TABLE` on a live table always reports `INTO N BUCKETS` regardless
of whether the adapter emitted it. The only place we can verify that the macro
*omits* `INTO N BUCKETS` when the user leaves `buckets` unset is by rendering
the macro directly — so this test renders the real macro source rather than a
copy, with the smallest `config`/`adapter` stand-ins the macro touches.
"""

from pathlib import Path

import jinja2

MACRO_FILE = (
    Path(__file__).resolve().parents[2]
    / "dbt/include/confluent/macros/materializations/models/helpers.sql"
)


class _Config:
    """Stand-in for dbt's `config`; only `distributed_by` is read by the macro."""

    def __init__(self, dist):
        self._dist = dist

    def get(self, key, default=None):
        return self._dist if key == "distributed_by" else default


class _Adapter:
    @staticmethod
    def quote(identifier):
        return f"`{identifier}`"


def _render_clause(dist):
    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    env.globals["config"] = _Config(dist)
    env.globals["adapter"] = _Adapter()
    module = env.from_string(MACRO_FILE.read_text()).make_module(vars=env.globals)
    return str(module.get_distributed_by_clause()).strip()


class TestGetDistributedByClause:
    def test_omits_into_buckets_when_unset(self):
        """The documented "omit buckets, let Confluent choose" path must not
        emit an `INTO ... BUCKETS` fragment."""
        rendered = _render_clause({"columns": ["order_id"]})
        assert "DISTRIBUTED BY HASH(`order_id`)" in rendered
        assert "BUCKETS" not in rendered

    def test_renders_into_buckets_when_set(self):
        rendered = _render_clause({"columns": ["order_id"], "buckets": 4})
        assert "DISTRIBUTED BY HASH(`order_id`) INTO 4 BUCKETS" in rendered

    def test_multiple_columns_quoted_and_comma_joined(self):
        rendered = _render_clause({"columns": ["a", "b"]})
        assert "DISTRIBUTED BY HASH(`a`, `b`)" in rendered

    def test_unset_config_renders_nothing(self):
        assert _render_clause(None) == ""
