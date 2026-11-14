"""Unit tests for statement_sql: Flink-faithful SQL normalization and the INSERT shape match.

The cases come from the lexical table in the design: every kind of token Flink's lexer reads, and
every boundary that decides where a comment, string or name starts and ends. `test_tokens` pins
exact token lists; `test_same` and `test_different` cover what needs a comparison (dropped
comments, folded keywords, and changes that must stay visible).
"""

import pytest

from dbt.adapters.confluent.statement_sql import (
    RESERVED_KEYWORDS,
    normalize_sql,
    runs_model_sql,
)

# Rows marked "order" fail if TOKEN_KINDS is reordered so a prefixed kind follows `word`.
TOKENS = [
    pytest.param("a\t\r\n\fb", ["a", "b"], id="T1-whitespace-kinds"),
    pytest.param("a --x\rb", ["a", "b"], id="T2-cr-ends-line-comment"),
    pytest.param("a //x", ["a"], id="T3-slash-comment-at-eof"),
    pytest.param("a /* x */b", ["a", "b"], id="T4-block-comment"),
    pytest.param("/***/ -- */ a", ["a"], id="T5-formal-comment-takes-next-char"),
    pytest.param("/**/a", ["a"], id="T6-empty-block-comment"),
    pytest.param("a /* x", ["a"], id="T7-unterminated-comment-runs-to-eof"),
    pytest.param(
        "/*+ X('k'='v') */",
        ["/*+", "X", "(", "'k'", "=", "'v'", ")", "*/"],
        id="T8-hint-contents-are-sql",
    ),
    pytest.param("'it''s' 'C:\\'", ["'it''s'", "'C:\\'"], id="T9-string-quote-and-backslash"),
    pytest.param("E'a\\'b' e'x'", ["E'a\\'b'", "e'x'"], id="T10-order-escaped-string"),
    pytest.param(
        "N'a' X'0A' U&'\\0041' _latin1'a' n 'a'",
        ["N'a'", "X'0A'", "U&'\\0041'", "_latin1'a'", "n", "'a'"],
        id="T11-order-prefixed-strings",
    ),
    pytest.param(
        'U&"a b" `x``y` "z"',
        ['U&"a b"', "`x``y`", '"', "z", '"'],
        id="T12-order-quoted-names",
    ),
    pytest.param(
        "1 1.5 .5 1. 1e5 1.5E-3 1..2 1e",
        ["1", "1.5", ".5", "1.", "1e5", "1.5E-3", "1.", ".2", "1", "e"],
        id="T13-numbers",
    ),
    pytest.param("$a a$b _x éa x1", ["$a", "a$b", "_x", "éa", "x1"], id="T14-words"),
    pytest.param(
        "<= >= <> != -> || => .. << */",
        ["<=", ">=", "<>", "!=", "->", "||", "=>", "..", "<<", "*/"],
        id="T15-multi-char-operators",
    ),
    pytest.param("a<-1", ["a", "<", "-", "1"], id="T16-no-such-operator"),
    pytest.param("a*/b", ["a", "*/", "b"], id="T17-comment-end-is-a-token"),
    pytest.param(
        "m['k'], a[1]",
        ["m", "[", "'k'", "]", ",", "a", "[", "1", "]"],
        id="T18-brackets",
    ),
    pytest.param("# @ \\ ~ \u00a0", ["#", "@", "\\", "~", "\u00a0"], id="T19-rejected-chars"),
    pytest.param("'abc", ["'", "abc"], id="T20-unterminated-string"),
    pytest.param("`ab", ["`", "ab"], id="T21-unterminated-quoted-name"),
    pytest.param("", [], id="T22-empty"),
    pytest.param(";", [], id="T22-only-semicolon"),
    pytest.param("'😀 café' é😀", ["'😀 café'", "é😀"], id="T23-non-ascii-and-non-bmp"),
]

SAME = [
    pytest.param("select a -- x\nfrom t", "select a from t", id="S1-line-comment"),
    pytest.param("select a --x\rfrom t", "select a from t", id="S2-line-comment-cr"),
    pytest.param("select a from t -- end", "select a from t", id="S3-line-comment-at-eof"),
    pytest.param("select a // x\nfrom t", "select a from t", id="S4-slash-comment"),
    pytest.param("select /* x\n y */ a from t", "select a from t", id="S5-block-comment"),
    pytest.param("select /** doc */ a from t", "select a from t", id="S6-formal-comment"),
    pytest.param("select/**/a from t", "select a from t", id="S7-empty-block-comment"),
    pytest.param(
        "select a from t /*+ OPTIONS('k'='v') */",
        "select a from t /*+OPTIONS( 'k' = 'v' /* c */ -- d\n)*/",
        id="S8-hint-layout-and-comments",
    ),
    pytest.param(
        "select 'a'\n'b' from t", "select 'a'\n\n   'b' from t", id="S9-continued-string"
    ),
    pytest.param(
        "select a from t where b is not null group by a",
        "SELECT a FROM t WHERE b IS NOT NULL GROUP BY a",
        id="S10-keyword-case",
    ),
    pytest.param("select count(*) from t", "select COUNT(*) from t", id="S11-reserved-function"),
    pytest.param("select a from t group by a", "select a from t GROUP\n  BY a", id="S12-group-by"),
    pytest.param(
        "select a from t union all select a from u",
        "select a from t UNION   ALL select a from u",
        id="S13-union-all",
    ),
    pytest.param(
        "select a from t where b not like 'x'",
        "select a from t where b NOT\tLIKE 'x'",
        id="S14-not-like",
    ),
    pytest.param("select a from t;", "select a from t", id="S15-trailing-semicolon"),
    pytest.param("select null from t", "select NULL from t", id="S16-null"),
    pytest.param(
        "select a, -- x\r\n  b\r\nfrom t\r\n", "select a, b from t", id="S17-crlf-with-comments"
    ),
]

DIFFERENT = [
    pytest.param("select 4 // 2 from t", "select 4 / 2 from t", id="D1-slash-slash-is-a-comment"),
    pytest.param(
        "select a // the /* thing\nfrom t /*+ OPTIONS('x'='1') */",
        "select a // the /* thing\nfrom u /*+ OPTIONS('x'='2') */",
        id="D2-block-start-inside-slash-comment",
    ),
    pytest.param(
        "select /* a /* b */ x from t",
        "select /* a /* b */ y from t",
        id="D3-comments-dont-nest",
    ),
    pytest.param(
        "select /***/ -- */ a from t",
        "select /***/ -- */ b from t",
        id="D4-formal-comment-takes-next-char",
    ),
    pytest.param(
        "select a from t /*+ OPTIONS('k'='v') */",
        "select a from t /*+ OPTIONS('k'='w') */",
        id="D5-hint-option-value",
    ),
    pytest.param(
        "select a from t /*+ OPTIONS('k'='v') */",
        "select a from t /* + OPTIONS('k'='v') */",
        id="D6-hint-vs-comment",
    ),
    pytest.param("select 'abc' from t", "select 'ABC' from t", id="D7-string-case"),
    pytest.param("select 'a  b' from t", "select 'a b' from t", id="D8-string-whitespace"),
    pytest.param(
        "select 'a -- b', c from t", "select 'a -- b', d from t", id="D9-comment-inside-string"
    ),
    pytest.param(
        "select 'C:\\', a, '--', b from t",
        "select 'C:\\', a, '--', c from t",
        id="D10-backslash-is-not-an-escape",
    ),
    pytest.param(
        "select E'it\\'s -- ', a from t",
        "select E'it\\'s -- ', b from t",
        id="D11-escaped-string",
    ),
    pytest.param("select 'a''b' from t", "select 'a'\n'b' from t", id="D12-quote-vs-continued"),
    pytest.param("select N'abc' from t", "select N'ABC' from t", id="D13-prefixed-string"),
    pytest.param("select `SELECT` from t", "select `select` from t", id="D14-quoted-name-case"),
    pytest.param(
        "select `it's`, a from t", "select `it's`, b from t", id="D15-quote-inside-quoted-name"
    ),
    pytest.param(
        'select U&"a--b", x from t', 'select U&"a--b", y from t', id="D16-unicode-quoted-name"
    ),
    pytest.param("select Amount from t", "select amount from t", id="D17-identifier-case"),
    pytest.param("select type from t", "select TYPE from t", id="D18-non-reserved-keyword"),
    pytest.param(
        "select round(a, 2) from t",
        "select ROUND(a, 2) from t",
        id="D19-non-reserved-function",
    ),
    pytest.param("select t.value from t", "select t.VALUE from t", id="D20-after-dot"),
    pytest.param("select t . value from t", "select t . VALUE from t", id="D21-spaced-dot"),
    pytest.param("select select$x from t", "select SELECT$x from t", id="D22-dollar-word"),
    pytest.param("select éselect from t", "select éSELECT from t", id="D23-non-ascii-word"),
    pytest.param("select 1.5e3 from t", "select 1.5 e3 from t", id="D24-number-vs-alias"),
    pytest.param("select 1e5 from t", "select 1E5 from t", id="D25-number-case"),
]


@pytest.mark.parametrize("sql,expected", TOKENS)
def test_tokens(sql, expected):
    assert normalize_sql(sql) == expected


@pytest.mark.parametrize("a,b", SAME)
def test_same(a, b):
    assert normalize_sql(a) == normalize_sql(b)


@pytest.mark.parametrize("a,b", DIFFERENT)
def test_different(a, b):
    assert normalize_sql(a) != normalize_sql(b)


# T20 and T21 are left out: two unterminated quotes would re-lex as one string, and invalid input
# never reaches the check.
VALID_INPUTS = [
    *(p.values[0] for p in TOKENS if not p.id.startswith(("T20", "T21"))),
    *(sql for p in SAME + DIFFERENT for sql in p.values),
]


@pytest.mark.parametrize("sql", VALID_INPUTS)
def test_normalize_is_idempotent(sql):
    """runs_model_sql matches the space-joined tokens, so joining them must not change how they
    lex."""
    tokens = normalize_sql(sql)
    assert normalize_sql(" ".join(tokens)) == tokens


MODEL = "select id, price from `env`.`lkc`.`src` where region = 'US'"
QUERY_COMMENT = '/* {"app": "dbt", "node_id": "model.p.m"} */\n'
# The 0.5.0+ INSERT, as streaming_table.sql renders it.
CURRENT = (
    f"{QUERY_COMMENT}INSERT INTO `env`.`lkc`.`tbl` (id, price)\n"
    f"    select id, price\n    from (\n        {MODEL}\n    ) as model_subq"
)
# The INSERT before 0.5.0.
LEGACY = f"{QUERY_COMMENT}INSERT INTO `env`.`lkc`.`tbl` {MODEL}"
# Characters that mean something in a regex, and a backslash, must match only themselves.
REGEX_MODEL = "select a from t where p like 'a.b*' or q = 'C:\\' or r in (1, 2)"


@pytest.mark.parametrize(
    "statement,model,expected",
    [
        pytest.param(CURRENT, MODEL, True, id="current-shape"),
        pytest.param(
            CURRENT.replace("(id, price)", "(price, id)").replace(
                "select id, price\n", "select price, id\n"
            ),
            MODEL,
            True,
            id="columns-reordered",
        ),
        pytest.param(
            CURRENT.replace("(id, price)", "(`id`, `price`)"), MODEL, True, id="quoted-columns"
        ),
        pytest.param(
            CURRENT,
            "SELECT id,price FROM `env`.`lkc`.`src` -- c\n WHERE region='US'",
            True,
            id="cosmetic-model-edit",
        ),
        pytest.param(LEGACY, MODEL, True, id="legacy-shape"),
        pytest.param(f"{LEGACY};", MODEL, True, id="legacy-trailing-semicolon"),
        pytest.param(
            f"INSERT INTO `env`.`lkc`.`tbl` {MODEL}\n{QUERY_COMMENT}",
            MODEL,
            True,
            id="query-comment-appended",
        ),
        pytest.param(CURRENT, MODEL.replace("'US'", "'EU'"), False, id="changed-literal"),
        pytest.param(
            f"INSERT INTO `env`.`lkc`.`tbl` SELECT * FROM ({MODEL})",
            MODEL,
            False,
            id="other-wrapper",
        ),
        pytest.param(
            f"INSERT INTO `env`.`lkc`.`tbl` SELECT id, price FROM x UNION ALL {MODEL}",
            MODEL,
            False,
            id="model-is-only-a-suffix",
        ),
        pytest.param(
            CURRENT.replace("select id, price\n", "select id + 1, price\n"),
            MODEL,
            False,
            id="expression-in-select-list",
        ),
        pytest.param(CURRENT.replace("as model_subq", "as other"), MODEL, False, id="other-alias"),
        pytest.param(f"INSERT INTO tbl {MODEL}", MODEL, True, id="unqualified-relation"),
        pytest.param(
            f"INSERT INTO `env`.`lkc`.`tbl` {REGEX_MODEL}",
            REGEX_MODEL,
            True,
            id="regex-characters-match-literally",
        ),
        pytest.param(
            f"INSERT INTO `env`.`lkc`.`tbl` {REGEX_MODEL}",
            REGEX_MODEL.replace("a.b*", "aXbb"),
            False,
            id="regex-characters-are-not-patterns",
        ),
        pytest.param("", MODEL, False, id="empty-statement"),
        pytest.param(MODEL, MODEL, False, id="no-insert"),
    ],
)
def test_runs_model_sql(statement, model, expected):
    assert runs_model_sql(statement, model) is expected


def test_reserved_keywords_list():
    """302 words Confluent rejects as unquoted identifiers, plus NULL. Words Confluent accepts as
    names must never be folded."""
    assert len(RESERVED_KEYWORDS) == 303
    assert "NULL" in RESERVED_KEYWORDS
    for name in ("TYPE", "NAME", "MORE", "USER", "CURRENT_USER", "ROUND"):
        assert name not in RESERVED_KEYWORDS
