"""Guards on the Flink SQL rules that the SQL logic check (GH-119) relies on.

`dbt.adapters.confluent.statement_sql` normalizes SQL before comparing it, using rules checked
against Confluent: which keywords are reserved (so their case can be folded), how comments, hints,
`#` and backslashes are lexed, and that identifiers are case-sensitive. Each test re-checks one
of those rules with `sql.dry-run` queries (validated and planned, never stored), so these tests
fail if Confluent changes a rule.
"""

import pytest
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.statement_sql import RESERVED_KEYWORDS
from dbt.tests.util import relation_from_name, run_dbt
from tests.functional.adapter.fixtures import ConfluentFixtures
from tests.functional.adapter.isolation import physical_name

# `type` is in Flink's documented keyword list, but Confluent accepts it as an unquoted name.
RULES_TABLE = """
{{ config(materialized='table') }}
select cast(1 as int) as id, cast('x' as string) as type
"""


class TestFlinkSqlRules(ConfluentFixtures):
    NAME = "sqlrules"

    @pytest.fixture(scope="class")
    def models(self):
        return {"rules_table.sql": RULES_TABLE}

    @pytest.fixture(scope="class")
    def rules_table(self, project):
        """Build rules_table once and return its rendered name for the dry-run queries."""
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)
        return str(relation_from_name(project.adapter, physical_name("rules_table")))

    def _dry_run_error(self, project, sql):
        """Dry-run `sql` as a streaming query on the open connection. Return None if Flink
        accepts it, or the error text (which carries Flink's message) if it rejects it."""
        try:
            project.adapter.connections.dry_run_schema(sql, execution_mode="streaming_query")
        except DbtDatabaseError as error:
            return str(error)
        return None

    def _flink_detail(self, error):
        """The part of an error text from Flink's message on, on one line. The text starts with
        the submitted SQL, which carries dbt's query comment."""
        start = error.find("Dry-run failed")
        return " ".join(error[max(start, 0) :].split())[:300]

    def test_reserved_keywords_still_reserved(self, project, rules_table):
        """Every word in RESERVED_KEYWORDS must still be rejected as an unquoted alias, and must
        not be parsed as a column name in a SELECT list. Otherwise folding its case could hide a
        change to a (case-sensitive) name. Collects every offending word, so one run shows all
        of them."""
        offenders = []
        with project.adapter.connection_named("reserved_keywords"):
            for word in sorted(RESERVED_KEYWORDS):
                error = self._dry_run_error(project, f"SELECT id AS {word} FROM {rules_table}")
                if error is None:
                    offenders.append(f"{word}: accepted as an alias")
                    continue
                if "SQL parse failed" not in error:
                    offenders.append(
                        f"{word}: as an alias, failed without a parse error: "
                        f"{self._flink_detail(error)}"
                    )
                    continue
                error = self._dry_run_error(project, f"SELECT {word} FROM {rules_table}")
                if error is not None and "not found in any table" in error:
                    offenders.append(f"{word}: parsed as a column name in a SELECT list")
        assert not offenders, (
            f"{len(offenders)} word(s) in RESERVED_KEYWORDS no longer behave as reserved on "
            "Confluent. Remove them from RESERVED_KEYWORDS in "
            "dbt/adapters/confluent/statement_sql.py (an error that isn't Flink's may be "
            "transient; rerun to check):\n" + "\n".join(offenders)
        )

    def test_lexer_rules(self, project, rules_table):
        """Flink reads `/*+ ... */` hints (a bogus option value is rejected), `/* + ... */` is a
        plain comment, `#` is a lexical error, a backslash isn't an escape in a string, and block
        comments don't nest. Collects every broken rule, so one run shows all of them."""
        t = rules_table
        accepted = {
            "a valid hint": (
                f"SELECT id FROM {t} /*+ OPTIONS('scan.startup.mode'='latest-offset') */"
            ),
            "a bogus option inside `/* + ... */` (a plain comment)": (
                f"SELECT id FROM {t} /* + OPTIONS('scan.startup.mode'='no-such-mode') */"
            ),
            "a string ending in a backslash": f"SELECT 'C:\\' AS p FROM {t}",
        }
        parse_errors = {
            "`#`": f"SELECT id # x\nFROM {t}",
            "a backslash before a quote inside a string": f"SELECT 'a\\'b' AS p FROM {t}",
            "a nested block comment": f"SELECT /* a /* b */ c */ id FROM {t}",
        }
        broken = []
        with project.adapter.connection_named("lexer_rules"):
            for case, sql in accepted.items():
                error = self._dry_run_error(project, sql)
                if error is not None:
                    broken.append(f"{case}: rejected: {self._flink_detail(error)}")
            # A bogus value in a real hint is rejected, but not by the parser: Flink applies
            # the hint's options to the table and fails to create the source.
            error = self._dry_run_error(
                project, f"SELECT id FROM {t} /*+ OPTIONS('scan.startup.mode'='no-such-mode') */"
            )
            if error is None:
                broken.append("a bogus option inside a hint: accepted (hint not applied)")
            elif "Dry-run failed" not in error or "SQL parse failed" in error:
                broken.append(
                    "a bogus option inside a hint: not rejected by Flink as an invalid option: "
                    f"{self._flink_detail(error)}"
                )
            for case, sql in parse_errors.items():
                error = self._dry_run_error(project, sql)
                if error is None:
                    broken.append(f"{case}: accepted")
                elif "SQL parse failed" not in error:
                    broken.append(
                        f"{case}: failed without a parse error: {self._flink_detail(error)}"
                    )
        assert not broken, (
            "Flink's lexer rules changed; update the lexer in "
            "dbt/adapters/confluent/statement_sql.py (an error that isn't Flink's may be "
            "transient; rerun to check):\n" + "\n".join(broken)
        )

    def test_identifiers_are_case_sensitive(self, project, rules_table):
        """Names are case-sensitive even unquoted, so the check must never fold their case:
        `type` resolves, `Type` doesn't."""
        with project.adapter.connection_named("identifier_case"):
            error = self._dry_run_error(project, f"SELECT type FROM {rules_table}")
            assert error is None, f"`SELECT type` was rejected: {self._flink_detail(error)}"
            error = self._dry_run_error(project, f"SELECT Type FROM {rules_table}")
        assert error is not None, (
            "`SELECT Type` was accepted: identifiers are no longer case-sensitive"
        )
        assert "not found in any table" in error, self._flink_detail(error)
