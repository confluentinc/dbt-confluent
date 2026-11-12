"""Tests for dbt.adapters.confluent.statement_sql.runs_model_sql."""

import time

import pytest

from dbt.adapters.confluent.statement_sql import runs_model_sql

# Fixtures and builders

REL = "`env-1`.`cluster_0`.`big_orders`"
DEFAULT_COMMENT = '/* {"app": "dbt", "dbt_version": "1.11.8", "node_id": "model.p.m"} */'
M = "select order_id, amount\nfrom `env-1`.`cluster_0`.`orders`\nwhere amount > 100"

DEMO_STATEMENT = (
    '/* {"app": "dbt", "dbt_version": "1.11.8", "profile_name": "gh121_demo", "target_name": '
    '"dev", "node_id": "model.gh121_demo.st_000"} */\n'
    "INSERT INTO `env-g22j51`.`cluster_0`.`gh121_demo_st_000` (order_id, price, order_time)\n"
    "    \n"
    "    select order_id, price, order_time\n"
    "    from (\n"
    "        \n"
    "select order_id, price, order_time from `env-g22j51`.`cluster_0`.`gh121_demo_faker_source`\n"
    "    ) as model_subq"
)
DEMO_MODEL_SQL = (
    "\nselect order_id, price, order_time from `env-g22j51`.`cluster_0`.`gh121_demo_faker_source`"
)


def _new_shape(
    sql, cols="order_id, amount", comment=DEFAULT_COMMENT, append=False, alias="model_subq"
):
    """Render the 0.5.0+ INSERT shape (with subquery wrapper)."""
    body = (
        f"INSERT INTO {REL} ({cols})\n"
        f"    select {cols}\n"
        f"    from (\n"
        f"        {sql}\n"
        f"    ) as {alias}"
    )
    if comment:
        if append:
            return f"{body}\n{comment}"
        else:
            return f"{comment}\n{body}"
    return body


def _old_shape(sql, comment=DEFAULT_COMMENT, append=False):
    """Render the pre-0.5.0 INSERT shape (direct SQL)."""
    body = f"INSERT INTO {REL} {sql}"
    if comment:
        if append:
            return f"{body}\n{comment}"
        else:
            return f"{comment}\n{body}"
    return body


# Test data

MATCHES = {  # id -> (stored statement, model SQL); runs_model_sql must be True
    "new_shape": (_new_shape(M), M),
    "new_shape_comment_appended": (_new_shape(M, append=True), M),
    "new_shape_comment_appended_semicolon": (_new_shape(M, append=True) + ";", M),
    "new_shape_no_comment": (_new_shape(M, comment=""), M),
    "old_shape": (_old_shape(M), M),
    "old_shape_comment_appended": (_old_shape(M, append=True), M),
    "yml_reorder": (_new_shape(M, cols="amount, order_id"), M),
    "backticked_columns": (_new_shape(M, cols="`a(b`, `Order`"), M),
    "model_outer_whitespace": (_new_shape(M), "\n  " + M + "\n"),
    "other_alias": (_new_shape(M, alias="__dbt_subq"), M),
    "old_shape_model_ends_in_comment": (
        _old_shape(M + " /* note */", append=True),
        M + " /* note */",
    ),
    "comment_inside_model": (
        _new_shape("select 1 /* c */ as v, '/* x */' as w"),
        "select 1 /* c */ as v, '/* x */' as w",
    ),
    "custom_comment_with_open": (_new_shape(M, comment="/* team /* x */"), M),
    "crlf_both": (_new_shape(M.replace("\n", "\r\n")), M.replace("\n", "\r\n")),
    "non_ascii": (_new_shape("select 'µs é 漢字' as v"), "select 'µs é 漢字' as v"),
    "real_dbt_insert_gh121_demo": (DEMO_STATEMENT, DEMO_MODEL_SQL),
}
CHANGES = {  # id -> (stored statement, model SQL); runs_model_sql must be False
    "where_value_changed": (_new_shape(M), M.replace("100", "0")),
    "trailing_where_removed": (_new_shape(M), M.rsplit("\n", 1)[0]),
    "trailing_where_removed_old_shape": (_old_shape(M), M.rsplit("\n", 1)[0]),
    "leading_cte_removed": (_new_shape("with x as (select 1)\n" + M), M),
    "case_changed_in_literal": (_new_shape("select 'abc' as v"), "select 'ABC' as v"),
    "crlf_vs_lf": (_new_shape(M.replace("\n", "\r\n")), M),
    "hand_written_adopted": (
        f"INSERT INTO {REL} SELECT order_id, amount FROM orders WHERE amount > 100",
        M,
    ),
    "show_tables": ("SHOW TABLES", M),
    "empty": ("", M),
    "real_dbt_insert_model_changed": (DEMO_STATEMENT, DEMO_MODEL_SQL + " WHERE price > 1"),
}


# Test cases


@pytest.mark.parametrize("stored, model_sql", MATCHES.values(), ids=MATCHES.keys())
def test_statement_runs_model_sql(stored, model_sql):
    assert runs_model_sql(stored, model_sql) is True


@pytest.mark.parametrize("stored, model_sql", CHANGES.values(), ids=CHANGES.keys())
def test_changed_sql_is_reported(stored, model_sql):
    assert runs_model_sql(stored, model_sql) is False


def test_regex_characters_match_literally():
    sql = r"select REGEXP_EXTRACT(s, '\d+$') as n, 'a|b' as p, '.*' as q from t"
    assert runs_model_sql(_new_shape(sql), sql) is True
    assert runs_model_sql(_new_shape(sql), sql.replace("\\d", "d")) is False


def test_near_cap_statement_is_fast():
    sql = ("select a from t where " + " or ".join(f"a = {i}" for i in range(12000)))[:125000]
    start = time.perf_counter()
    assert runs_model_sql(_new_shape(sql), sql) is True
    assert runs_model_sql(_new_shape(sql), sql[:-5]) is False
    assert time.perf_counter() - start < 2.0
