"""Does a streaming_table's running statement execute the model's current SQL?

Confluent stores a statement's SQL exactly as submitted. For a streaming_table that's dbt's query
comment plus the INSERT the materialization renders, with the model's compiled SQL embedded. Both
sides are normalized first (`normalize_sql`): plain comments dropped (SQL hints kept, since Flink
applies them), tokens rejoined with single spaces, and the case of reserved keywords folded. Then
`runs_model_sql` checks that the normalized model SQL sits where one of the INSERT shapes
dbt-confluent has shipped puts it.

Maintenance:
- When the INSERT the streaming_table materialization renders changes shape, add the new shape to
  INSERT_SHAPES and keep the old ones: statements submitted by earlier releases are still running.
- Any change to how dbt-confluent compiles a model's SQL (relation rendering, quoting, macros used
  inside models) changes the comparison for every running statement, so users see SQL logic drift
  on their first run after upgrading. Ship such a change as a `breaking` changelog entry that
  explains the ways out (see MATERIALIZATIONS.md#query-logic-changes).
- RESERVED_KEYWORDS must only hold words Confluent rejects as unquoted identifiers: folding the
  case of a word that can also be a (case-sensitive) name would hide a real change.
  tests/functional/adapter/test_flink_sql_rules.py re-checks every word against Confluent.
"""

import re

from sqlparse import keywords
from sqlparse import tokens as T
from sqlparse.lexer import Lexer

# Keywords whose case is folded: the reserved words of Flink's SQL parser (Parser.tdd) that
# Confluent also rejects as unquoted identifiers (probe a6bd958e, 2026-11-13: 302 of the 549 words
# Flink 2.3 documents), plus NULL. Any unquoted occurrence of one must be the keyword, which Flink
# reads case-insensitively. Not here: non-reserved keywords such as TYPE or NAME (valid unquoted
# column names, and names are case-sensitive), MORE (reserved upstream, accepted by Confluent), and
# CURRENT_USER and the like (Confluent parses them as column names in a SELECT).
# fmt: off
RESERVED_KEYWORDS = frozenset(
    {
        "ABS", "ALL", "ALLOCATE", "ALLOW", "ALTER", "AND", "ANY", "ARE", "ARRAY", "AS",
        "ASENSITIVE", "ASYMMETRIC", "AT", "ATOMIC", "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN",
        "BIGINT", "BINARY", "BIT", "BLOB", "BOOLEAN", "BOTH", "BY", "BYTES", "CALL", "CALLED",
        "CARDINALITY", "CASCADED", "CASE", "CAST", "CEIL", "CEILING", "CHAR", "CHARACTER",
        "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOB", "CLOSE", "COALESCE", "COLLATE",
        "COLLECT", "COLUMN", "COMMIT", "CONDITION", "CONNECT", "CONSTRAINT", "CONTAINS", "CONVERT",
        "CORR", "CORRESPONDING", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE",
        "CUME_DIST", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURSOR", "CYCLE", "DATE", "DAY", "DEALLOCATE", "DEC",
        "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DENSE_RANK", "DEREF", "DESCRIBE",
        "DETERMINISTIC", "DISALLOW", "DISCONNECT", "DISTINCT", "DOUBLE", "DROP", "DYNAMIC", "EACH",
        "ELEMENT", "ELSE", "END", "EQUALS", "ESCAPE", "EVERY", "EXCEPT", "EXEC", "EXECUTE",
        "EXISTS", "EXP", "EXPLAIN", "EXTEND", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FILTER",
        "FIRST_VALUE", "FLOAT", "FLOOR", "FOR", "FOREIGN", "FREE", "FROM", "FULL", "FUNCTION",
        "FUSION", "GET", "GLOBAL", "GRANT", "GROUP", "GROUPING", "HAVING", "HOLD", "HOUR",
        "IDENTITY", "IMPORT", "IN", "INDICATOR", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT",
        "INTEGER", "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "IS", "JOIN", "LANGUAGE",
        "LARGE", "LAST_VALUE", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LN", "LOCAL",
        "LOCALTIME", "LOCALTIMESTAMP", "LOWER", "MATCH", "MAX", "MEMBER", "MERGE", "METHOD", "MIN",
        "MINUTE", "MOD", "MODIFIES", "MODULE", "MODULES", "MONTH", "MULTISET", "NATIONAL",
        "NATURAL", "NCHAR", "NCLOB", "NEW", "NEXT", "NO", "NONE", "NORMALIZE", "NOT", "NULL",
        "NULLIF", "NUMERIC", "OCTET_LENGTH", "OF", "OFFSET", "OLD", "ON", "ONLY", "OPEN", "OR",
        "ORDER", "OUT", "OUTER", "OVER", "OVERLAPS", "OVERLAY", "PARAMETER", "PARTITION",
        "PERCENTILE_CONT", "PERCENTILE_DISC", "PERCENT_RANK", "POSITION", "POWER", "PRECISION",
        "PREPARE", "PRIMARY", "PROCEDURE", "RANGE", "RANK", "RAW", "READS", "REAL", "RECURSIVE",
        "REF", "REFERENCES", "REFERENCING", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT",
        "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY", "RELEASE",
        "RESET", "RESULT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW",
        "ROWS", "ROW_NUMBER", "SAVEPOINT", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SELECT",
        "SENSITIVE", "SET", "SIMILAR", "SMALLINT", "SOME", "SPECIFIC", "SPECIFICTYPE", "SQL",
        "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQRT", "START", "STATEMENT", "STATIC",
        "STDDEV_POP", "STDDEV_SAMP", "STREAM", "STRING", "SUBMULTISET", "SUBSTRING", "SUM",
        "SYMMETRIC", "SYSTEM", "TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP",
        "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TINYINT", "TO", "TRAILING", "TRANSLATE",
        "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE", "UESCAPE", "UNION", "UNIQUE", "UNKNOWN",
        "UNNEST", "UPDATE", "UPPER", "UPSERT", "USING", "VALUE", "VALUES", "VARBINARY", "VARCHAR",
        "VARYING", "VAR_POP", "VAR_SAMP", "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW",
        "WITH", "WITHIN", "WITHOUT", "YEAR",
    }
)
# fmt: on
# sqlparse's default rules, adjusted to Flink's lexer (probes 356ad1c0, eb9c2bec): `//` starts a
# comment to the end of the line, a backslash isn't an escape inside quotes, and `[...]` isn't a
# bracketed name (Flink uses brackets only for array and map access), nor is `$tag$...$tag$` a
# dollar-quoted string (a Postgres form Flink doesn't have). Without this, `'C:\'` followed by
# another quote would swallow the SQL after it into a string, and a `/*` inside a `//` comment
# would swallow the SQL up to the next `*/`, hiding changes there. A private Lexer, so sqlparse's
# shared default instance (dbt-core uses it) is untouched.
FLINK_SQL_REGEX = [
    (r"//.*?(\r\n|\r|\n|$)", T.Comment.Single),
] + [
    (r"'(''|[^'])*'", ttype)
    if regex == r"'(''|\\'|[^'])*'"
    else (r'"(""|[^"])*"', ttype)
    if regex in (r'"(""|\\"|[^"])*"', r'(""|".*?[^\\]")')
    else (regex, ttype)
    for regex, ttype in keywords.SQL_REGEX
    if regex
    not in (
        r"(?<![\w\])])(\[[^\]\[]+\])",
        r"((?<![\w\"\$])\$(?:[_A-ZÀ-Ü]\w*)?\$)[\s\S]*?\1",
    )
]
LEXER = Lexer()
LEXER.default_initialization()
LEXER.set_SQL_REGEX(FLINK_SQL_REGEX)
# A word that can be folded: unquoted, letters, digits, underscores and `$` only.
BARE_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
# A relation name part after normalization: backticked, or bare. Tokens are joined with single
# spaces, so the dots between parts may have spaces around them.
NAME_PART = r"(?:`[^`]*`|[^\s`.()]+)"
RELATION = rf"{NAME_PART}(?:\s*\.\s*{NAME_PART})*"
# A column list: backticked names (which may hold parentheses) and anything but parentheses.
COLUMNS = r"(?:`[^`]*`|[^`()])*"

# Each shape matches a whole normalized statement; {sql} is where the normalized model SQL goes.
# Wrapper keywords are reserved, so normalization has uppercased them.
INSERT_SHAPES = (
    # 0.5.0+ (#228): INSERT INTO <t> (<cols>) select <cols> from ( <sql> ) as model_subq
    rf"INSERT\s+INTO\s+{RELATION}\s*\({COLUMNS}\)\s*SELECT\s+{COLUMNS}\s+FROM\s*\(\s*{{sql}}\s*\)"
    rf"\s*AS\s+\w+(?:\s*;)?",
    # Before 0.5.0: INSERT INTO <t> <sql>
    rf"INSERT\s+INTO\s+{RELATION}\s+{{sql}}(?:\s*;)?",
)


def normalize_sql(sql: str) -> str:
    """Return `sql` with the differences Flink ignores taken out, for comparing two statements.

    Uses sqlparse's lexer with Flink's quoting rules (LEXER), which finds where string literals,
    backticked names and comments begin and end. Plain comments are dropped; SQL hints
    (`/*+ ... */`) are kept as written. Tokens are rejoined with single spaces, so whitespace
    between them, including whether there is any around operators, commas and dots, doesn't
    matter; multi-word keywords (`GROUP BY`, `NOT LIKE`) lose their inner whitespace too. Words in
    RESERVED_KEYWORDS are uppercased, except next to a dot, where a word is part of a qualified
    name. Every other token (names, string literals, numbers, operators, hints) stays exactly as
    written.
    """
    tokens = [
        (ttype, value)
        for ttype, value in LEXER.get_tokens(sql)
        if ttype not in T.Whitespace
        and not (ttype in T.Comment and ttype not in T.Comment.Multiline.Hint)
    ]
    parts = []
    for index, (ttype, value) in enumerate(tokens):
        next_to_dot = (index > 0 and tokens[index - 1][1] == ".") or (
            index + 1 < len(tokens) and tokens[index + 1][1] == "."
        )
        words = value.split()
        if (
            (ttype in T.Keyword or ttype in T.Name or ttype in T.Operator.Comparison)
            and not next_to_dot
            and all(BARE_WORD.fullmatch(word) for word in words)
        ):
            value = " ".join(
                word.upper() if word.upper() in RESERVED_KEYWORDS else word for word in words
            )
        parts.append(value)
    return " ".join(parts)


def runs_model_sql(statement_sql: str, model_sql: str) -> bool:
    """Return True if `statement_sql`, a streaming_table statement's stored SQL, runs `model_sql`.

    Both are normalized (see normalize_sql); the normalized model SQL must then match exactly.
    Text that matches no known INSERT shape (an adopted or hand-written statement, say) returns
    False.
    """
    sql = re.escape(normalize_sql(model_sql))
    statement = normalize_sql(statement_sql)
    return any(
        re.fullmatch(shape.replace("{sql}", sql), statement, re.DOTALL) for shape in INSERT_SHAPES
    )
