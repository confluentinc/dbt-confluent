"""Does a streaming_table's running statement still run the SQL dbt would submit now?

Confluent stores a statement's SQL exactly as submitted: dbt's query comment, then the INSERT the
streaming_table materialization renders. `same_sql` compares that with the INSERT the
materialization renders on this run, ignoring only comments and whitespace, which Flink ignores
too. Everything else counts as a change, the case of keywords included.

Maintenance: any change to the INSERT the streaming_table materialization renders (its wrapper, or
how dbt-confluent compiles a model's SQL: relation rendering, quoting, macros used in models)
changes the comparison for every running statement, so users see SQL logic drift on their first run
after upgrading. Ship it as a `breaking` changelog entry.
"""

import re

# The kinds of token Flink's lexer reads (Calcite's Parser.jj, in the back-tick quoting state
# Flink uses). At each position the first kind that matches wins, not the longest, so order
# matters: the prefixed kinds (escaped_string, string, quoted_name) must come before word, or N'a'
# would lex as the word N and a string; and the one-character kinds, punctuation then invalid,
# must come last, after every kind that can start with the same character (--, /*, '...', .5,
# <=).
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
    # character. The charset name is capped at 16 characters: the longest Calcite accepts is 10
    # (ISO-8859-1), and without a cap a long chain of `_` names with no spaces (`_a-_a-...`) is
    # rescanned at every name, which is quadratic.
    ("string", r"(?:[xXnN]|[uU]&|_[A-Za-z0-9][A-Za-z0-9:._-]{0,15})?'(?:[^']|'')*'"),
    # U&"..." or `...` (a doubled back-tick stands for one).
    ("quoted_name", r'[uU]&"(?:[^"]|"")*"|`(?:[^`]|``)*`'),
    ("number", r"(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"),
    # An unquoted name or keyword. It also takes in any non-ASCII character, which Flink only
    # accepts inside names.
    ("word", r"[A-Za-z_$\x80-\U0010ffff][A-Za-z0-9_$\x80-\U0010ffff]*"),
    # Multi-character operators. A hint's /*+ and */ are tokens: Flink reads the hint's contents
    # like any other SQL.
    ("operator", r"/\*\+|\*/|<=|>=|<>|!=|->|\|\||=>|\.\.|<<"),
    # Flink's one-character separators and operators.
    ("punctuation", r"[()\[\]{};.,=<>?:+\-*/%|^&'\"]"),
    # Any character Flink has no token for (#, @, a lone back-tick, ...). It only appears in SQL
    # Flink rejects, which never reaches this comparison; without it, finditer would silently skip
    # the character instead.
    ("invalid", r"."),
)
TOKEN = re.compile("|".join(f"(?P<{kind}>{pattern})" for kind, pattern in TOKEN_KINDS), re.DOTALL)
SKIPPED_KINDS = frozenset({"whitespace", "line_comment", "block_comment"})


def normalize_sql(sql: str) -> list[str]:
    """Return the tokens Flink reads in `sql`, without the whitespace and comments it skips.

    Every token (keywords, names, quoted names, string literals, numbers, operators, hint
    delimiters) stays exactly as written, so two texts give the same tokens only if they differ
    in nothing but comments and whitespace.
    """
    return [match.group() for match in TOKEN.finditer(sql) if match.lastgroup not in SKIPPED_KINDS]


def same_sql(statement_sql: str, current_sql: str) -> bool:
    """Return True if `statement_sql`, a running statement's stored SQL, is `current_sql`, the SQL
    the materialization would submit now, apart from comments and whitespace (dbt's query comment
    included)."""
    return normalize_sql(statement_sql) == normalize_sql(current_sql)
