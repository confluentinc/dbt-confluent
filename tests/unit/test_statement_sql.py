"""Unit tests for statement_sql: Flink-faithful tokenizing, and comparing a running statement's SQL
with the SQL the materialization would submit now.

The cases come from the lexical table in the design: every kind of token Flink's lexer reads, and
every boundary that decides where a comment, string or name starts and ends. `test_tokens` pins
exact token lists; `test_same` and `test_different` cover what needs a comparison: comments and
whitespace are ignored, and every other change, the case of keywords included, is kept.
"""

import time

import pytest

from dbt.adapters.confluent.statement_sql import TOKEN, normalize_sql, same_sql

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
    pytest.param(";", [";"], id="T22-semicolon-is-a-token"),
    pytest.param("'😀 café' é😀", ["'😀 café'", "é😀"], id="T23-non-ascii-and-non-bmp"),
    pytest.param(f"_{'x' * 16}'c'", [f"_{'x' * 16}'c'"], id="T24-charset-name-up-to-16-chars"),
    pytest.param(f"_{'x' * 17}'c'", [f"_{'x' * 17}", "'c'"], id="T24-longer-name-is-a-word"),
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
    pytest.param("select a from t group by a", "select a from t group\n  by a", id="S10-group-by"),
    pytest.param(
        "select a from t union all select a from u",
        "select a from t union   all select a from u",
        id="S11-union-all",
    ),
    pytest.param(
        "select a from t where b not like 'x'",
        "select a from t where b not\tlike 'x'",
        id="S12-not-like",
    ),
    pytest.param(
        "select a, -- x\r\n  b\r\nfrom t\r\n", "select a, b from t", id="S13-crlf-with-comments"
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
    pytest.param(
        "select a from t where b is not null",
        "SELECT a FROM t WHERE b IS NOT NULL",
        id="D18-keyword-case",
    ),
    pytest.param("select count(*) from t", "select COUNT(*) from t", id="D19-function-case"),
    pytest.param("select a from t;", "select a from t", id="D20-trailing-semicolon"),
    pytest.param("select 1.5e3 from t", "select 1.5 e3 from t", id="D21-number-vs-alias"),
    pytest.param("select 1e5 from t", "select 1E5 from t", id="D22-number-case"),
]


@pytest.mark.parametrize("sql,expected", TOKENS)
def test_tokens(sql, expected):
    assert normalize_sql(sql) == expected


def test_underscore_chain_is_linear():
    """A long chain of `_` names with no spaces once made the charset-prefix pattern rescan the
    rest of the chain at every name: quadratic, about 23 s for one text near Confluent's cap
    (131,072 characters). Capped, it's linear (well under a tenth of a second); the limit leaves
    room for slow machines."""
    chain = "SELECT " + "-".join(["_a"] * 43_000) + " FROM t"
    start = time.perf_counter()
    normalize_sql(chain)
    assert time.perf_counter() - start < 2


@pytest.mark.parametrize("a,b", SAME)
def test_same(a, b):
    assert normalize_sql(a) == normalize_sql(b)


@pytest.mark.parametrize("a,b", DIFFERENT)
def test_different(a, b):
    assert normalize_sql(a) != normalize_sql(b)


def test_punctuation_and_invalid_kinds():
    """Flink's one-character tokens lex as punctuation. A character Flink has no token for lexes as
    invalid, one character at a time, so finditer never skips it."""
    for char in "()[]{};.,=<>?:+-*/%|^&'\"":
        assert TOKEN.fullmatch(char).lastgroup == "punctuation", char
    for char in "#@\\~!`":
        assert TOKEN.fullmatch(char).lastgroup == "invalid", char


MODEL = "select id, price from `env`.`lkc`.`src` where region = 'US'"
QUERY_COMMENT = '/* {"app": "dbt", "node_id": "model.p.m"} */\n'
# The INSERT the streaming_table materialization renders, before dbt adds its query comment.
CURRENT = (
    "INSERT INTO `env`.`lkc`.`tbl` (id, price)\n"
    f"    select id, price\n    from (\n        {MODEL}\n    ) as model_subq"
)


@pytest.mark.parametrize(
    "statement,expected",
    [
        pytest.param(QUERY_COMMENT + CURRENT, True, id="query-comment-ignored"),
        pytest.param(CURRENT + "\n" + QUERY_COMMENT, True, id="appended-query-comment-ignored"),
        pytest.param(
            QUERY_COMMENT + " ".join(CURRENT.split()).replace("where", "-- note\nwhere"),
            True,
            id="layout-and-comments-ignored",
        ),
        pytest.param(CURRENT.replace("'US'", "'EU'"), False, id="changed-literal"),
        pytest.param(CURRENT.replace("where", "WHERE"), False, id="keyword-case"),
        pytest.param(
            CURRENT.replace("(id, price)", "(price, id)").replace(
                "select id, price\n", "select price, id\n"
            ),
            False,
            id="yml-column-reorder",
        ),
        pytest.param(f"INSERT INTO `env`.`lkc`.`tbl` {MODEL}", False, id="pre-0.5.0-insert"),
        pytest.param("", False, id="empty-statement"),
    ],
)
def test_same_sql(statement, expected):
    assert same_sql(statement, CURRENT) is expected


# Every valid-SQL input above: the token rows except T19 (characters Flink rejects) and T21 (an
# unterminated back-quoted name), every same/different pair, and the statements compared above.
VALID_SQL = [
    *(p.values[0] for p in TOKENS if not p.id.startswith(("T19", "T21"))),
    *(sql for p in SAME + DIFFERENT for sql in p.values),
    QUERY_COMMENT + CURRENT,
    CURRENT,
]


@pytest.mark.parametrize("sql", VALID_SQL)
def test_valid_sql_has_no_invalid_tokens(sql):
    """`invalid` only matches characters Flink has no token for, so valid SQL never produces one.
    If a pattern change lets a valid construct fall through to it, this fails."""
    assert [m.group() for m in TOKEN.finditer(sql) if m.lastgroup == "invalid"] == []
