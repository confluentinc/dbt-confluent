"""Guard: every word statement_sql folds must still be reserved on Confluent.

statement_sql uppercases RESERVED_KEYWORDS before comparing a streaming_table's running SQL with
the model's. That's only safe for words Confluent rejects as unquoted names, because names are
case-sensitive. These dry-runs (validated, never stored) fail the test if Confluent starts
accepting one of the words as a name, so a Flink change fails here instead of silently hiding a
real change to someone's SQL.
"""

from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.statement_sql import RESERVED_KEYWORDS
from tests.functional.adapter.fixtures import ConfluentFixtures

# Rows to select from, without building a table.
ROWS = "(VALUES (1)) AS v(id)"


class TestReservedKeywords(ConfluentFixtures):
    NAME = "reservedkw"

    def _dry_run_error(self, project, sql):
        """Dry-run `sql` as a streaming query. Return None if Flink accepts it, or the error text
        (which carries Flink's message) if it rejects it."""
        try:
            project.adapter.connections.dry_run_schema(sql, execution_mode="streaming_query")
        except DbtDatabaseError as error:
            return str(error)
        return None

    def test_reserved_keywords_are_still_reserved(self, project):
        """Each word must be a parse error as an unquoted alias, and must not be read as a column
        name in a SELECT list. Collects every offender, so one run shows them all."""
        offenders = []
        with project.adapter.connection_named("reserved_keywords"):
            for word in sorted(RESERVED_KEYWORDS):
                error = self._dry_run_error(project, f"SELECT id AS {word} FROM {ROWS}")
                if error is None:
                    offenders.append(f"{word}: accepted as an alias")
                    continue
                if "SQL parse failed" not in error:
                    start = error.find("Dry-run failed")
                    detail = " ".join(error[max(start, 0) :].split())[:300]
                    offenders.append(f"{word}: failed without a parse error: {detail}")
                    continue
                error = self._dry_run_error(project, f"SELECT {word} FROM {ROWS}")
                if error is not None and "not found in any table" in error:
                    offenders.append(f"{word}: parsed as a column name in a SELECT list")
        assert not offenders, (
            f"{len(offenders)} word(s) in RESERVED_KEYWORDS no longer behave as reserved on "
            "Confluent. Remove them from RESERVED_KEYWORDS in "
            "dbt/adapters/confluent/statement_sql.py (an error that isn't Flink's may be "
            "transient; rerun to check):\n" + "\n".join(offenders)
        )
