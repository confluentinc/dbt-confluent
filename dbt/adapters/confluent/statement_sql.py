"""Does a streaming_table's running statement execute the model's current SQL?

Confluent stores a statement's SQL exactly as submitted. For a streaming_table that's dbt's query
comment plus the INSERT the materialization renders, with the model's compiled SQL embedded
unchanged. `runs_model_sql` checks that the SQL sits in that INSERT where one of the INSERT shapes
dbt-confluent has shipped puts it. The model's SQL must match exactly; the query comment, the
whitespace between the wrapper's tokens, the relation name, the column lists and the subquery
alias are allowed to vary.

When the INSERT the streaming_table materialization renders changes shape, add the new shape to
INSERT_SHAPES and keep the old ones: statements submitted by earlier releases are still running.
"""

import re

# dbt's query comment: `/* ... */`, before the SQL or (with `query-comment: append: true`) after it,
# then the `;` dbt moves past it. dbt rejects a comment containing `*/`.
QUERY_COMMENT = r"/\*(?:(?!\*/).)*\*/"
# A relation name part: backticked, or bare.
NAME_PART = r"(?:`[^`]*`|[^\s`.()]+)"
RELATION = rf"{NAME_PART}(?:\.{NAME_PART})*"
# A column list: backticked names (which may hold parentheses) and anything but parentheses.
COLUMNS = r"(?:`[^`]*`|[^`()])*"
PREFIX = rf"\s*(?:{QUERY_COMMENT}\s*)?INSERT\s+INTO\s+{RELATION}"
SUFFIX = rf"\s*(?:{QUERY_COMMENT}\s*;?)?\s*"

# Each shape matches a whole stored statement; {sql} is where the model's compiled SQL goes.
INSERT_SHAPES = (
    # 0.5.0+ (#228): INSERT INTO <t> (<cols>) select <cols> from ( <sql> ) as model_subq
    PREFIX
    + rf"\s*\({COLUMNS}\)\s*select\s+{COLUMNS}\s+from\s*\(\s*{{sql}}\s*\)\s*as\s+\w+"
    + SUFFIX,
    # Before 0.5.0: INSERT INTO <t> <sql>
    PREFIX + r"\s+{sql}" + SUFFIX,
)


def runs_model_sql(statement_sql: str, model_sql: str) -> bool:
    """Return True if `statement_sql`, a streaming_table statement's stored SQL, runs `model_sql`.

    `model_sql` matches exactly, apart from whitespace at its two ends. Text that matches no known
    INSERT shape (an adopted or hand-written statement, say) returns False.
    """
    sql = re.escape(model_sql.strip())
    return any(
        re.fullmatch(shape.replace("{sql}", sql), statement_sql, re.DOTALL)
        for shape in INSERT_SHAPES
    )
