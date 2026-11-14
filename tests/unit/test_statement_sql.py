"""Tests for dbt.adapters.confluent.statement_sql.runs_model_sql."""

import time

import pytest
import sqlparse.keywords
import sqlparse.lexer
from sqlparse import tokens as T

from dbt.adapters.confluent.statement_sql import (
    FLINK_SQL_REGEX,
    LEXER,
    RESERVED_KEYWORDS,
    normalize_sql,
    runs_model_sql,
)

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
    "crlf_vs_lf": (_new_shape(M.replace("\n", "\r\n")), M),
    "non_ascii": (_new_shape("select 'µs é 漢字' as v"), "select 'µs é 漢字' as v"),
    "real_dbt_insert_gh121_demo": (DEMO_STATEMENT, DEMO_MODEL_SQL),
    "comment_added": (_new_shape(M), M.replace("select", "select -- c\n")),
    "block_comment_added": (_new_shape(M), M.replace("select", "select /* c */")),
    "re_indented": (_new_shape(M.replace("\n", "\n    ")), M),
    "operator_spacing": (_new_shape("select a from t where a>1"), "select a from t where a > 1"),
    "comma_spacing": (_new_shape("select round(x,2) from t"), "select round(x, 2) from t"),
    "keyword_case_select": (_new_shape("select a from t"), "SELECT a FROM t"),
    "keyword_case_from": (_new_shape("SELECT a FROM t"), "select a from t"),
    "keyword_case_where": (
        _new_shape("select a from t where a > 1"),
        "SELECT a FROM t WHERE a > 1",
    ),
    "reserved_function_case_count": (
        _new_shape("select count(*) as n from t"),
        "SELECT COUNT(*) AS n FROM t",
    ),
    "reserved_function_case_sum": (_new_shape("select sum(a) from t"), "SELECT SUM(a) FROM t"),
    "reserved_function_case_cast": (
        _new_shape("select cast(a as int) from t"),
        "SELECT CAST(a AS INT) FROM t",
    ),
    "null_case": (
        _new_shape("select a from t where b is null"),
        "SELECT a FROM t WHERE b IS NULL",
    ),
    "group_by_whitespace": (
        _new_shape("select a from t group\n   by a"),
        "select a from t group by a",
    ),
    "array_spacing": (_new_shape("select arr[ 1 ] from t"), "select arr[1] from t"),
    "old_shape_semicolon_moved": (
        _old_shape("select a from t;", append=True),
        "select a from t",
    ),
    "like_case": (
        _new_shape("select a from t where b like 'x%'"),
        "SELECT a FROM t WHERE b LIKE 'x%'",
    ),
    "not_like_spacing": (
        _new_shape("select a from t where b not like 'x%'"),
        "SELECT a FROM t WHERE b NOT  LIKE 'x%'",
    ),
    "backslash_literal_spacing": (
        _new_shape("select   replace(p, '\\', '/')   from t"),
        "select replace(p, '\\', '/') from t",
    ),
    "double_slash_comment_edited": (
        _new_shape("select a // old note\nfrom t"),
        "select a // new note\nfrom t",
    ),
}
CHANGES = {  # id -> (stored statement, model SQL); runs_model_sql must be False
    "where_value_changed": (_new_shape(M), M.replace("100", "0")),
    "trailing_where_removed": (_new_shape(M), M.rsplit("\n", 1)[0]),
    "trailing_where_removed_old_shape": (_old_shape(M), M.rsplit("\n", 1)[0]),
    "leading_cte_removed": (_new_shape("with x as (select 1)\n" + M), M),
    "case_changed_in_literal": (_new_shape("select 'abc' as v"), "select 'ABC' as v"),
    "hand_written_adopted": (
        f"INSERT INTO {REL} SELECT order_id, amount FROM orders WHERE amount > 100",
        M,
    ),
    "show_tables": ("SHOW TABLES", M),
    "empty": ("", M),
    "real_dbt_insert_model_changed": (DEMO_STATEMENT, DEMO_MODEL_SQL + " WHERE price > 1"),
    "literal_whitespace": (_new_shape("select 'a b' as v"), "select 'a  b' as v"),
    "type_case": (_new_shape("select type from t"), "select Type from t"),
    "name_case": (_new_shape("select name from t"), "select NAME from t"),
    "backticked_reserved_case": (_new_shape("select `select` from t"), "select `SELECT` from t"),
    "round_case": (_new_shape("select round(a, 2) from t"), "select ROUND(a, 2) from t"),
    "lag_case": (
        _new_shape("select lag(a) over (order by b) from t"),
        "select LAG(a) OVER (ORDER BY b) from t",
    ),
    "desc_case": (
        _new_shape("select a from t order by a desc"),
        "select a from t order by a DESC",
    ),
    "select_reordered": (_new_shape("select a, b from t"), "select b, a from t"),
    "optional_as": (_new_shape("select a b from t"), "select a as b from t"),
    "hint_value_changed": (
        _new_shape("select /*+ OPTIONS('x'='1') */ a from t"),
        "select /*+ OPTIONS('x'='2') */ a from t",
    ),
    "hint_spacing": (
        _new_shape("select /*+ OPTIONS('x'='1') */ a from t"),
        "select /*+ OPTIONS('x' = '1') */ a from t",
    ),
    "hint_removed": (_new_shape("select /*+ OPTIONS('x'='1') */ a from t"), "select a from t"),
    "comment_in_literal": (_new_shape("select '-- a' from t"), "select '-- b' from t"),
    "minus_one_spacing": (_new_shape("select -1 as v"), "select - 1 as v"),
    "match_recognize_quantifier": (
        _new_shape(
            "select * from t match_recognize (measures A as a pattern (B+) define B as true)"
        ),
        "select * from t match_recognize (measures A as a pattern (B*) define B as true)",
    ),
    "planted_adoption_insert": (
        f"INSERT INTO {REL} SELECT order_id, price FROM orders WHERE order_id > 0",
        "SELECT order_id, price FROM orders",
    ),
    "literal_after_backslash": (
        _new_shape("select replace(p, '\\', '/'), 'select  x' from t"),
        "select replace(p, '\\', '/'), 'SELECT x' from t",
    ),
    "alias_after_backslash_comment_literal": (
        _new_shape("select replace(p, '\\', '/'), '-- x' as b from t"),
        "select replace(p, '\\', '/'), '-- x' as c from t",
    ),
    "alias_after_regexp_replace": (
        _new_shape("select regexp_replace(s, '\\\\', '/'), 'x' as b from t"),
        "select regexp_replace(s, '\\\\', '/'), 'x' as c from t",
    ),
    "bracket_change": (
        _new_shape("select m [']'] , '-- x' as b from t"),
        "select m [']'] , '-- x' as c from t",
    ),
    "qualified_name_case": (_new_shape("select t.value from t"), "select t.VALUE from t"),
    "double_slash_comment_hides_change": (
        _new_shape("select a // the /* thing\nfrom t /*+ OPTIONS('x'='1') */"),
        "select a // the /* thing\nfrom u /*+ OPTIONS('x'='2') */",
    ),
    "http_url_in_literal": (
        _new_shape("select 'http://a' from t"),
        "select 'http://b' from t",
    ),
    "dollar_quote_hides_change": (
        _new_shape("select $a$, 'cost $a$', '-- z' as b from t"),
        "select $a$, 'cost $a$', '-- z' as c from t",
    ),
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
    assert time.perf_counter() - start < 5.0


def test_reserved_keyword_list():
    assert len(RESERVED_KEYWORDS) == 303
    assert "NULL" in RESERVED_KEYWORDS
    assert "SELECT" in RESERVED_KEYWORDS
    assert "COUNT" in RESERVED_KEYWORDS
    assert "TYPE" not in RESERVED_KEYWORDS
    assert "NAME" not in RESERVED_KEYWORDS
    assert "MORE" not in RESERVED_KEYWORDS
    assert "CURRENT_USER" not in RESERVED_KEYWORDS
    assert "USER" not in RESERVED_KEYWORDS
    assert "LAG" not in RESERVED_KEYWORDS
    assert "LEAD" not in RESERVED_KEYWORDS
    assert "ROUND" not in RESERVED_KEYWORDS
    assert "ASC" not in RESERVED_KEYWORDS
    assert "DESC" not in RESERVED_KEYWORDS


def test_normalize_sql():
    assert (
        normalize_sql("select a -- c\nfrom t /*+ OPTIONS('x'='1') */")
        == "SELECT a FROM t /*+ OPTIONS('x'='1') */"
    )
    assert normalize_sql("select `select`, type from t") == "SELECT `select` , type FROM t"
    assert normalize_sql("select a from t group\n   by a") == "SELECT a FROM t GROUP BY a"
    assert normalize_sql("select a // note\nfrom t") == "SELECT a FROM t"
    assert (
        normalize_sql("select a // the /* thing\nfrom t /*+ OPTIONS('x'='1') */")
        == "SELECT a FROM t /*+ OPTIONS('x'='1') */"
    )
    assert (
        normalize_sql("select a // don't\nfrom t where b = 'x'") == "SELECT a FROM t WHERE b = 'x'"
    )
    assert normalize_sql("select a from t // end") == "SELECT a FROM t"
    assert normalize_sql("select 'http://a' as u from t") == "SELECT 'http://a' AS u FROM t"


def test_sqlparse_lexing_assumptions():
    """Pin behavior normalize_sql relies on, so a sqlparse upgrade that changes it fails."""
    # Shared default lexer behavior
    tokens = list(sqlparse.lexer.tokenize("/*+ OPTIONS('a'='b') */"))
    assert any(ttype is sqlparse.tokens.Comment.Multiline.Hint for ttype, _ in tokens)

    tokens = list(sqlparse.lexer.tokenize("/* + x */"))
    assert all(ttype is not sqlparse.tokens.Comment.Multiline.Hint for ttype, _ in tokens), (
        "/* + ... */ must not be a hint"
    )

    tokens = list(sqlparse.lexer.tokenize("'it''s -- x'"))
    assert len([t for t in tokens if t[0] in sqlparse.tokens.String]) == 1

    tokens = list(sqlparse.lexer.tokenize("`we``ird -- col`"))
    assert len([t for t in tokens if t[0] in sqlparse.tokens.Name]) == 1

    tokens = list(sqlparse.lexer.tokenize("'C:\\'"))
    string_tokens = [val for ttype, val in tokens if ttype in sqlparse.tokens.String]
    assert len(string_tokens) == 1

    tokens = list(sqlparse.lexer.tokenize("# x"))
    assert any(ttype in sqlparse.tokens.Comment for ttype, _ in tokens)

    tokens = list(sqlparse.lexer.tokenize("GROUP\n  BY"))
    assert any(ttype in sqlparse.tokens.Keyword for ttype, _ in tokens)

    tokens = list(sqlparse.lexer.tokenize("type"))
    assert any(ttype in sqlparse.tokens.Keyword for ttype, _ in tokens)

    tokens = list(sqlparse.lexer.tokenize("round"))
    assert any(ttype in sqlparse.tokens.Name for ttype, _ in tokens)

    tokens = list(sqlparse.lexer.tokenize("like"))
    assert any(ttype is sqlparse.tokens.Operator.Comparison for ttype, _ in tokens)

    # Private lexer behavior
    tokens = list(LEXER.get_tokens("select 'C:\\' as a, 'x' as b"))
    string_values = [val for ttype, val in tokens if ttype in sqlparse.tokens.String]
    assert len(string_values) == 2
    assert string_values[0] == "'C:\\'"
    assert string_values[1] == "'x'"

    assert len(FLINK_SQL_REGEX) == len(sqlparse.keywords.SQL_REGEX) - 1
    assert FLINK_SQL_REGEX[0] == (r"//.*?(\r\n|\r|\n|$)", T.Comment.Single)
    for regex, _ in FLINK_SQL_REGEX:
        assert r"|\\'" not in regex and r'|\\"' not in regex and r"[^\\]" not in regex
    assert any(r"|\\'" in r for r, _ in sqlparse.keywords.SQL_REGEX)
    assert any(r'|\\"' in r for r, _ in sqlparse.keywords.SQL_REGEX)
    dollar_rule = r"((?<![\w\"\$])\$(?:[_A-ZÀ-Ü]\w*)?\$)[\s\S]*?\1"
    assert dollar_rule not in [r for r, _ in FLINK_SQL_REGEX]

    # Shared default lexer unchanged
    default_tokens = list(sqlparse.lexer.tokenize("select 'C:\\' as a, 'x' as b"))
    default_strings = [val for ttype, val in default_tokens if ttype in sqlparse.tokens.String]
    assert len(default_strings) == 1
    assert default_strings[0] == "'C:\\' as a, '"
