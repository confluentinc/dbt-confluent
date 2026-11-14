"""Does a streaming_table's running statement execute the model's current SQL?

Confluent stores a statement's SQL exactly as submitted: dbt's query comment, then the INSERT the
streaming_table materialization renders around the model's compiled SQL. `runs_model_sql` splits
both into the tokens Flink's SQL parser reads (`normalize_sql`) and checks that the model's tokens
sit where one of the INSERT shapes dbt-confluent has shipped puts them.

Maintenance:
- When the streaming_table INSERT changes shape, add the new shape to INSERT_SHAPES and keep the
  old ones: statements submitted by earlier releases are still running.
- A change to how dbt-confluent compiles a model's SQL (relation rendering, quoting, macros used in
  models) changes the comparison for every running statement, so users see SQL logic drift on
  their first run after upgrading. Ship it as a `breaking` changelog entry.
- RESERVED_KEYWORDS must hold only words Confluent rejects as unquoted identifiers: folding the
  case of a word that can also be a (case-sensitive) name would hide a real change.
  tests/functional/adapter/test_reserved_keywords.py re-checks each word against Confluent.
"""

import re

# Words whose case is folded: the 302 reserved words of Flink's SQL parser that Confluent also
# rejects as unquoted identifiers (verified against Confluent, 2026-11-13), plus NULL. An unquoted
# one can only be the keyword, which Flink reads case-insensitively. Not here: non-reserved
# keywords such as TYPE or NAME (valid unquoted names, which are case-sensitive), MORE (reserved
# upstream, a name on Confluent), and CURRENT_USER and the like (Confluent parses them as names in
# a SELECT).
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

# An unquoted identifier or keyword. It also takes in any non-ASCII character, which Flink only
# accepts inside identifiers, so a word with one is never taken for a keyword.
WORD = r"[A-Za-z_$\x80-\U0010ffff][A-Za-z0-9_$\x80-\U0010ffff]*"
# A back-quoted name; a doubled back-tick stands for one.
BACK_QUOTED = r"`(?:[^`]|``)*`"
# The kinds of token Flink's lexer reads (Calcite's Parser.jj, in the back-tick quoting state
# Flink uses). At each position the first kind that matches wins, not the longest, so order
# matters in two places: the prefixed kinds (escaped_string, string, quoted_name) must come before
# word, or N'a' would lex as the word N and a string; and other must come last.
TOKEN_KINDS = (
    ("whitespace", r"[ \t\n\r\f]+"),
    # -- or // to the end of the line.
    ("line_comment", r"(?:--|//)[^\n\r]*"),
    # Up to the first */, no nesting. /** takes the character after it into the comment, so /***/
    # doesn't close itself. /*+ starts a hint instead (see operator).
    ("block_comment", r"/\*(?!\+)(?:\*[^/])?.*?(?:\*/|\Z)"),
    # E'...': a backslash escapes the next character.
    ("escaped_string", r"[eE]'(?:[^'\\]|\\.|'')*'"),
    # '...', optionally prefixed (N, X, U&, _charset). '' is a quote; a backslash is an ordinary
    # character.
    ("string", r"(?:[xXnN]|[uU]&|_[A-Za-z0-9][A-Za-z0-9:._-]*)?'(?:[^']|'')*'"),
    ("quoted_name", rf'[uU]&"(?:[^"]|"")*"|{BACK_QUOTED}'),
    ("number", r"(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"),
    ("word", WORD),
    # Multi-character operators. A hint's /*+ and */ are tokens: Flink reads the hint's contents
    # like any other SQL.
    ("operator", r"/\*\+|\*/|<=|>=|<>|!=|->|\|\||=>|\.\.|<<"),
    # Any other single character, punctuation included.
    ("other", r"."),
)
TOKEN = re.compile("|".join(f"(?P<{kind}>{pattern})" for kind, pattern in TOKEN_KINDS), re.DOTALL)
SKIPPED_KINDS = frozenset({"whitespace", "line_comment", "block_comment"})
# A name in the INSERT around the model's SQL, after normalize_sql.
NAME = rf"(?:{BACK_QUOTED}|{WORD})"
RELATION = rf"{NAME}(?: \. {NAME})*"
COLUMNS = rf"{NAME}(?: , {NAME})*"
# Each shape matches a whole normalized statement, tokens joined by single spaces; {sql} is where
# the normalized model SQL goes. The relation and column lists come from the model's config and
# yml, so any names match there (a yml column reorder isn't a logic change).
INSERT_SHAPES = (
    # 0.5.0+: INSERT INTO <t> (<cols>) select <cols> from ( <sql> ) as model_subq
    rf"INSERT INTO {RELATION} \( {COLUMNS} \) SELECT {COLUMNS} FROM \( {{sql}} \) AS model_subq",
    # Before 0.5.0: INSERT INTO <t> <sql>
    rf"INSERT INTO {RELATION} {{sql}}",
)


def normalize_sql(sql: str) -> list[str]:
    """Return the tokens Flink reads in `sql`, for comparing two statements.

    Whitespace and comments are dropped, and so is a trailing semicolon. Words in
    RESERVED_KEYWORDS are uppercased, except next to a dot, where a word is part of a qualified
    name. Every other token (names, quoted names, string literals, numbers, operators, hint
    delimiters) stays exactly as written.
    """
    matches = [m for m in TOKEN.finditer(sql) if m.lastgroup not in SKIPPED_KINDS]
    tokens = []
    for index, match in enumerate(matches):
        text = match.group()
        neighbors = matches[max(index - 1, 0) : index + 2]
        if (
            match.lastgroup == "word"
            and text.isascii()
            and text.upper() in RESERVED_KEYWORDS
            and all(neighbor.group() != "." for neighbor in neighbors)
        ):
            text = text.upper()
        tokens.append(text)
    if tokens[-1:] == [";"]:
        tokens.pop()
    return tokens


def runs_model_sql(statement_sql: str, model_sql: str) -> bool:
    """Return True if `statement_sql`, a streaming_table statement's stored SQL, runs `model_sql`.

    Both are normalized (see normalize_sql); the model's tokens must then fill the {sql} slot of
    one of INSERT_SHAPES exactly. Text that matches no shape (an adopted or hand-written
    statement, say) returns False.
    """
    sql = re.escape(" ".join(normalize_sql(model_sql)))
    statement = " ".join(normalize_sql(statement_sql))
    return any(re.fullmatch(shape.replace("{sql}", sql), statement) for shape in INSERT_SHAPES)
