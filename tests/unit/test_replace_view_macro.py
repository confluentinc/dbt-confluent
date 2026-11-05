"""Unit tests for the SQL the `confluent__get_replace_view_sql` macro renders (#74)."""

from pathlib import Path
from types import SimpleNamespace

import jinja2

REPLACE_VIEW_MACROS = (
    Path(__file__).parents[2] / "dbt/include/confluent/macros/relations/views/replace.sql"
)


class TestGetReplaceViewSqlMacro:
    def render(self, sql, sql_header=None):
        """The flattened SQL `confluent__get_replace_view_sql` renders for `sql`."""
        environment = jinja2.Environment()
        environment.globals["config"] = SimpleNamespace(
            get=lambda key, default=None: {"sql_header": sql_header}.get(key, default)
        )
        module = environment.from_string(REPLACE_VIEW_MACROS.read_text()).make_module()
        relation = SimpleNamespace(render=lambda: "`env-1`.`cluster_0`.`my_view`")
        return " ".join(str(module.confluent__get_replace_view_sql(relation, sql)).split())

    def test_alters_the_view_in_place(self):
        assert self.render("select 1 as id") == (
            "alter view `env-1`.`cluster_0`.`my_view` as ( select 1 as id );"
        )

    def test_sql_header_comes_first(self):
        assert self.render("select 1 as id", sql_header="set 'a' = 'b';") == (
            "set 'a' = 'b'; alter view `env-1`.`cluster_0`.`my_view` as ( select 1 as id );"
        )
