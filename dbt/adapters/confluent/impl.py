import logging
import re
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

import agate
from confluent_sql.exceptions import OperationalError, StatementNotFoundError
from dbt_common.contracts.constraints import ConstraintType, ModelLevelConstraint
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.base import BaseRelation, available
from dbt.adapters.base.impl import InformationSchema, _parse_callback_empty_table
from dbt.adapters.confluent import ConfluentColumn, ConfluentConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.relation import Policy
from dbt.adapters.events.types import AdapterEventWarning
from dbt.adapters.sql import SQLAdapter

from .naming import sanitize_statement_name
from .utils import fetch_from_cursor

logger = logging.getLogger(__name__)

# Every dbt-confluent-specific config key, keyed by which materializations
# consume it. This is a closed set we own completely -- validate_materialization_config
# only ever inspects keys listed here, so a user's own custom config (read by
# their own hooks/macros) is never touched or second-guessed, no matter which
# materialization or key name they pick. Keep in sync with what each
# materialization's Jinja actually reads (grep `config.get` under
# macros/materializations/models/).
_UNIVERSAL_CONFIG_KEYS = frozenset(
    {"statement_name", "compute_pool_id", "ignore_unsupported_config"}
)

MATERIALIZATION_CONFIG_KEYS: dict[str, frozenset[str]] = {
    "table": _UNIVERSAL_CONFIG_KEYS | {"distributed_by", "on_schema_drift"},
    "view": _UNIVERSAL_CONFIG_KEYS,
    "streaming_source": _UNIVERSAL_CONFIG_KEYS
    | {"connector", "with", "distributed_by", "on_schema_drift"},
    "streaming_table": _UNIVERSAL_CONFIG_KEYS
    | {"with", "distributed_by", "on_schema_drift", "statement_properties"},
}

_ALL_CONFIG_KEYS: frozenset[str] = frozenset().union(*MATERIALIZATION_CONFIG_KEYS.values())

# start_mode keyword → argument arity. Confluent documents eight forms: four
# bare keywords, plus *_TIMESTAMP with a required argument and *_NOW with an
# optional one (FROM_NOW doubles as bare "now" and parameterized "now minus
# interval").
_START_MODE_ARITY = {
    "FROM_BEGINNING": "none",
    "RESUME_OR_FROM_BEGINNING": "none",
    "FROM_NOW": "optional",
    "RESUME_OR_FROM_NOW": "optional",
    "FROM_TIMESTAMP": "required",
    "RESUME_OR_FROM_TIMESTAMP": "required",
}

_START_MODE_FORMS = (
    "FROM_BEGINNING, FROM_NOW, FROM_NOW(INTERVAL '<n>' <unit>), "
    "FROM_TIMESTAMP('<timestamp>'), RESUME_OR_FROM_BEGINNING, RESUME_OR_FROM_NOW, "
    "RESUME_OR_FROM_NOW(INTERVAL '<n>' <unit>), RESUME_OR_FROM_TIMESTAMP('<timestamp>')"
)

# A start_mode string: a keyword, optionally followed by a parenthesized
# argument. The argument is checked lexically, not structurally: any mix of
# well-formed single-quoted SQL literals (quotes escaped by doubling) and bare
# word/space characters, so an interval literal like INTERVAL '7' DAY passes
# through while a stray quote, paren, or operator — anything that could
# terminate the rendered DDL early — fails the match. The server is the
# authority on the argument's structure (see render_start_mode).
_START_MODE_RE = re.compile(
    r"\s*(?P<kw>[A-Za-z_]+)\s*"
    r"(?:\((?P<arg>(?:'(?:[^']|'')*'|[\w \t])*)\)\s*)?"
)


@dataclass(frozen=True, eq=False, repr=False)
class ConfluentRelation(BaseRelation):
    quote_character: str = "`"
    include_policy: Policy = field(
        default_factory=lambda: Policy(database=True, schema=True, identifier=True)
    )

    def quoted(self, identifier):
        # Flink SQL does not support backticks in identifiers, so raise an error instead
        # of trying to escape the identifier.
        if self.quote_character in identifier:
            # TODO: Is this the right error?
            raise CompilationError(
                f"Quote character '{self.quote_character}' can't be used in identifiers!",
                get_node_info(),
            )
        return f"{self.quote_character}{identifier}{self.quote_character}"

    def make_confluent_fqn(self):
        return ".".join([f"`{p}`" for p in [self.database, self.schema, self.identifier] if p])


class _CleanupRegistry(threading.local):
    """Thread-local registry of temp relations to drop in post_model_hook.

    Subclassing threading.local runs __init__ once per thread, so every worker
    thread dbt spins up starts with its own empty list. A bare threading.local()
    with `.relations` assigned on the main thread (where the adapter is
    constructed) would be invisible to dbt's worker threads, which is where the
    model hooks actually run.
    """

    def __init__(self) -> None:
        self.relations: list = []


class ConfluentAdapter(SQLAdapter):
    """
    Controls actual implementation of adapter, and ability to override certain methods.
    """

    ConnectionManager: type[ConfluentConnectionManager] = ConfluentConnectionManager
    connections: ConfluentConnectionManager
    Relation: type[ConfluentRelation] = ConfluentRelation
    Column: type[ConfluentColumn] = ConfluentColumn

    def __init__(self, config, mp_context) -> None:
        super().__init__(config, mp_context)
        # Deferred-cleanup registry, consumed by post_model_hook. Thread-local
        # because dbt runs each node — and both its model hooks — on its own
        # worker thread; _CleanupRegistry.__init__ gives each of those threads
        # its own empty list on first access.
        self._deferred_cleanups = _CleanupRegistry()

    @classmethod
    def quote(cls, identifier: str) -> str:
        """
        Quotes identifiers (table names, column names, schemas) with backticks.
        """
        return f"`{identifier}`"

    def check_schema_exists(self, database, schema) -> bool:
        schemas = self.list_schemas(self.quote(database))
        # Remove duplicates here since we can't use a DISTINCT on INFORMATION_SCHEMA
        return schema in schemas

    def create_schema(self, relation: BaseRelation) -> None:
        """
        Check if schema exists; if it does, do nothing (schemas are managed externally).
        If it doesn't exist, raise an error requiring pre-creation.
        """
        relation = relation.without_identifier()

        # Check if schema already exists
        if self.check_schema_exists(relation.database, relation.schema):
            # Schema exists, no need to create - this is expected
            return

        # Schema doesn't exist - raise error
        raise DbtDatabaseError(
            f"Schema '{relation.schema}' does not exist in Confluent Cloud. "
            f"Schemas (Kafka clusters) must be created in Confluent Cloud before use. "
            f"This adapter does not support schema creation."
        )

    def drop_schema(self, relation: BaseRelation) -> None:
        """
        Schemas cannot be dropped via dbt - they must be managed in Confluent Cloud.
        """
        raise DbtDatabaseError(
            f"Cannot drop schema '{relation.schema}'. "
            f"Schemas (Kafka clusters) must be managed in Confluent Cloud. "
            f"This adapter does not support schema deletion."
        )

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "CURRENT_TIMESTAMP"

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint) -> str | None:
        # Flink expects `PRIMARY KEY (cols) NOT ENFORCED`, but the base adapter
        # renders the expression before the column list (`PRIMARY KEY NOT ENFORCED (cols)`),
        # which Flink rejects with a parse error.
        if constraint.type == ConstraintType.primary_key:
            prefix = f"constraint {constraint.name} " if constraint.name else ""
            column_list = ", ".join(constraint.columns)
            expression = f" {constraint.expression}" if constraint.expression else ""
            return f"{prefix}primary key ({column_list}){expression}"
        return super().render_model_constraint(constraint)

    @available.parse(_parse_callback_empty_table)
    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: int | None = None,
        execution_mode: str | None = None,
        hidden: bool = False,
        statement_name: str | None = None,
        compute_pool_id: str | None = None,
        statement_properties: dict[str, str | int | bool] | None = None,
    ) -> tuple[AdapterResponse, "agate.Table"]:
        return self.connections.execute(
            sql=sql,
            auto_begin=auto_begin,
            fetch=fetch,
            limit=limit,
            execution_mode=execution_mode,
            hidden=hidden,
            statement_name=statement_name,
            compute_pool_id=compute_pool_id,
            statement_properties=statement_properties,
        )

    @available
    def drop_materialized_table(self, relation: BaseRelation) -> None:
        """DROP MATERIALIZED TABLE IF EXISTS, then wait for the catalog to agree.

        MTs need this dedicated drop — a regular DROP TABLE phantom-drops
        them (see drop_relation). IF EXISTS keeps a stale relation cache or
        an externally-dropped table from failing the run; the cache is
        evicted first, mirroring SQLAdapter.drop_relation.

        Unlike a regular DROP TABLE, the MT drop removes its catalog entry
        asynchronously. Every caller immediately recreates the name, and
        racing that teardown fails ("table already exists") or, worse — on
        the CREATE OR ALTER path — silently binds to the dying table. So poll
        until the entry is gone; on timeout raise a retriable DbtDatabaseError
        (`dbt retry` picks it up once the teardown finishes).
        """
        self.cache_dropped(relation)
        self.execute(
            f"DROP MATERIALIZED TABLE IF EXISTS {relation}", execution_mode="streaming_ddl"
        )
        self._wait_for_catalog_absence(relation)

    def _wait_for_catalog_absence(self, relation: BaseRelation, timeout: float = 120.0) -> None:
        """Poll INFORMATION_SCHEMA until `relation` no longer appears."""
        deadline = time.monotonic() + timeout
        backoff = 1.0
        while True:
            if self.get_relation_kind(relation) == "absent":
                return
            if time.monotonic() >= deadline:
                raise DbtDatabaseError(
                    f"Dropped materialized table {relation} is still listed in the "
                    f"catalog after {timeout:.0f}s; its asynchronous teardown is "
                    f"lagging. Retry with `dbt retry` once it completes."
                )
            time.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

    @available
    def get_relation_kind(self, relation: BaseRelation) -> str:
        """Classify what `relation` currently is in the live catalog.

        Returns 'materialized_table', 'regular' (table or view), or 'absent'
        (no catalog entry). Deliberately a live probe rather than a relation
        cache lookup: the cache can't tell an MT from a regular table (see
        drop_relation) and may be stale.
        """
        _, table = self.execute(
            "SELECT IS_MATERIALIZED FROM INFORMATION_SCHEMA.`TABLES` "
            f"WHERE TABLE_CATALOG_ID = '{relation.database}' "
            f"AND TABLE_SCHEMA = '{relation.schema}' "
            f"AND TABLE_NAME = '{relation.identifier}'",
            fetch=True,
        )
        if len(table.rows) == 0:
            return "absent"
        if any(str(row[0]).upper() == "YES" for row in table.rows):
            return "materialized_table"
        return "regular"

    def drop_relation(self, relation: BaseRelation) -> None:
        """Drop a relation, routing Flink materialized tables to the MT drop.

        A Flink materialized table reports TABLE_TYPE='BASE TABLE', so dbt's
        relation cache types it as a regular table and the drop macro renders
        DROP TABLE. The server does NOT reject that: it silently accepts DROP
        TABLE against an MT and phantom-drops it — the catalog entry
        disappears transiently, but the table's backing resources survive,
        same-name creates keep failing with "table already exists", and the
        MT later resurfaces in the catalog. Because the failed drop raises no
        error, no fallback can catch it: materialized tables must be detected
        *before* the drop — one IS_MATERIALIZED lookup, skipped for views,
        which can't be MTs — and routed to drop_materialized_table. This is
        what lets --full-refresh replace a materialized table after a model
        switches away from the materialized_table materialization.
        """
        # RelationType subclasses str, so this compares the enum's value; a
        # None type (unknown) conservatively still gets the pre-check.
        if relation.type != "view" and self.get_relation_kind(relation) == "materialized_table":
            self.drop_materialized_table(relation)
            return
        super().drop_relation(relation)

    @available
    def get_statement_name(
        self,
        model_name: str,
        project_name: str,
        suffix: str = "",
        statement_name_override: str | None = None,
    ) -> str:
        """Build a deterministic, sanitized Flink statement name.

        Called from Jinja macros via adapter.get_statement_name().
        Returns the final name ready for the Flink API.
        """
        if statement_name_override:
            name = f"{statement_name_override}{suffix}"
        else:
            prefix = self.config.credentials.statement_name_prefix
            name = f"{prefix}{project_name}-{model_name}{suffix}"
        return sanitize_statement_name(name)

    def _handle_pool_scoped_403(
        self,
        e: OperationalError,
        statement_name: str,
        *,
        action: str,
        expect_exists: bool = True,
    ) -> bool:
        """Decide how to treat an OperationalError from a statement API call.

        Compute-pool-scoped FlinkDeveloper roles return 403 — not 404 — when
        the target statement does not exist or lives on a different compute
        pool than the one in config (Confluent Cloud intentionally hides
        existence across pool boundaries). We can't disambiguate "missing"
        from "no permission" from the response, so we treat 403 as missing and
        let any genuine permission problem surface on a subsequent operation
        that runs on the same scope.

        Returns True if `e` was a handled 403 (the caller should treat the
        statement as missing); returns False if it's any other error (the
        caller must re-raise).

        action: noun describing the call ("deletion", "inspection"), used in
            the message. expect_exists: if True (default), the 403 is
            surprising — the caller had reason to believe the statement
            existed — so we emit a loud AdapterEventWarning. If False (e.g.
            orphan-cleanup paths), the 403 is expected and we log at debug.
        """
        if getattr(e, "http_status_code", None) != 403:
            return False
        if expect_exists:
            fire_event(
                AdapterEventWarning(
                    base_msg=(
                        f"Got 403 during {action} of Flink statement "
                        f"'{statement_name}'. Under compute-pool-scoped roles this "
                        f"is the expected response for a statement that is missing "
                        f"or lives on a different compute pool, so we are treating "
                        f"it as missing. If subsequent operations fail "
                        f"unexpectedly, verify that the API key can manage "
                        f"statements in this compute pool."
                    )
                )
            )
        else:
            logger.debug(
                "Got 403 on opportunistic %s of statement '%s' (no orphan present).",
                action,
                statement_name,
            )
        return True

    @available
    def statement_needs_restart(self, statement_name: str) -> bool:
        """Return True if the long-running statement should be re-submitted.

        True when the statement is missing (404, or 403 under pool-scoped
        roles — see _handle_pool_scoped_403) or in a terminal phase
        (COMPLETED, STOPPED, FAILED, DELETED). False for healthy phases,
        including in-flight transitions (PENDING, RUNNING, DEGRADED, STOPPING,
        DELETING) which we must not interrupt. No side effects.

        Used by `decide_action` (Jinja) to recover a streaming_table whose
        INSERT died without the table being dropped.
        """
        conn = self.connections.get_thread_connection()
        try:
            statement = conn.handle.get_statement(statement_name)
        except StatementNotFoundError:
            return True
        except OperationalError as e:
            if not self._handle_pool_scoped_403(e, statement_name, action="inspection"):
                raise
            return True
        return statement.phase.is_terminal

    @available
    def delete_statement(self, statement_name: str, expect_exists: bool = True) -> None:
        """Delete a Flink statement by name. No-op if it doesn't exist.

        See _handle_pool_scoped_403 for how the 403 that pool-scoped roles
        return for a missing statement is treated. expect_exists controls
        whether that 403 warns loudly (default) or logs quietly (e.g.
        orphan-cleanup paths where the statement is opportunistically deleted).

        Async deletion is not awaited here: the connection manager retries
        CREATE on 409 to handle the in-flight teardown race against the
        next statement that reuses this name.
        """
        conn = self.connections.get_thread_connection()
        handle = conn.handle
        try:
            handle.delete_statement(statement_name)
        except StatementNotFoundError:
            return  # Already gone (either deleted instantly or never existed)
        except OperationalError as e:
            if not self._handle_pool_scoped_403(
                e, statement_name, action="deletion", expect_exists=expect_exists
            ):
                raise

    def pre_model_hook(self, config: Mapping[str, Any]) -> None:
        """Reset this thread's deferred-cleanup registry.

        Worker threads are reused across nodes, so entries left behind by a
        node that died without reaching its post-hook (e.g. SIGKILL mid-run)
        must not leak into the next node's cleanup.

        Deliberately ignores `config`: dbt-core's test task passes the whole
        context dict here instead of the config, so the argument can't be
        relied on across node types.
        """
        self._deferred_cleanups.relations.clear()

    @available
    def defer_drop(self, relation) -> None:
        """Register a relation for post_model_hook to drop.

        dbt calls post_model_hook in a try/finally around the materialization,
        so registered relations are dropped even when the materialization
        raises — the closest thing to try/finally that Jinja macros can get.
        Register temp relations *before* creating them: the drop goes through
        `drop_relation` (IF EXISTS), so a failure before creation is a no-op.

        Statements are intentionally not registered for deletion here: the temp
        objects are created by bounded statements (drift check appends
        `WHERE FALSE`; unit-test fixtures are bounded INSERTs), which reach a
        terminal phase immediately and are reaped by the cursor.close() in
        ConfluentConnectionManager.execute(). DROP also succeeds without first
        stopping any dependent statement.
        """
        if relation not in self._deferred_cleanups.relations:
            self._deferred_cleanups.relations.append(relation)

    def post_model_hook(self, config: Mapping[str, Any], context: Any) -> None:
        """Drop the temp relations registered by this node.

        Uses `drop_relation` so cleanup is relation-type aware (table, view,
        materialized view — each `drop ... if exists`) and keeps dbt's relation
        cache consistent, rather than hardcoding DROP TABLE.

        dbt calls this in a try/finally around the materialization, so it also
        runs when the materialization failed. Cleanup failures are demoted to
        warnings: raising here would mask the materialization's own error, and
        a leaked temp object is reclaimed by the next run's preemptive drop.

        Like pre_model_hook, ignores its arguments and relies only on the
        thread-local registry.
        """
        relations = self._deferred_cleanups.relations
        for relation in relations:
            try:
                self.drop_relation(relation)
            except Exception as e:
                fire_event(
                    AdapterEventWarning(
                        base_msg=(
                            f"Failed to drop {relation} during post-model cleanup: {e}. "
                            f"It will be reclaimed by the next run."
                        )
                    )
                )
        self._deferred_cleanups.relations.clear()

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "STRING"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "FLOAT" if decimals else "INT"

    @classmethod
    def convert_integer_type(cls, agate_table, col_idx):
        return "INT"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx) -> str:
        return "TIMESTAMP"

    def rename_relation(self, from_relation, to_relation):
        """Custom rename_relation routine.

        `ALTER TABLE` is not supported, so we raise an exception if a user tries.
        `ALTER VIEW ... RENAME TO` should be supported, but the server gives an
        error if we try to use it. I confirmed that it's a bug, it should be supported,
        but it doesn't work. For now, fall back to creating a clone, and then dropping
        the original view.
        Link to jira issue: https://confluentinc.atlassian.net/browse/FSE-878
        """
        if not from_relation.is_view:
            raise DbtDatabaseError(
                f"Renaming is only supported in views, got {from_relation.type}"
            )

        self.cache_renamed(from_relation, to_relation)

        # Now, to manually duplicate a view, we first need to get its definition using a SHOW
        _, res = self.execute(f"SHOW CREATE VIEW {from_relation.identifier}", fetch=True)
        ddl = res[0].values()[0]

        # Fully quote the entire relation, regardless of include policies.
        old_fqn = from_relation.make_confluent_fqn()
        new_fqn = to_relation.make_confluent_fqn()

        # I don't like this, but it's a temporary workaround hopefully.
        # Use regexp to extract the definition of the view we want to clone.
        pattern = re.compile(
            rf"(CREATE\s+VIEW\s+){re.escape(old_fqn)}(?=(\s|\(|\\n|$))",
            re.IGNORECASE | re.MULTILINE,
        )

        # Create the cloned view
        new_ddl = pattern.sub(rf"\1{new_fqn}", ddl, count=1)
        self.execute(new_ddl)

        # Drop the original one
        self.execute(f"DROP VIEW {old_fqn}")

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: set[str],
        used_schemas: frozenset[tuple[str, str]],
    ) -> "agate.Table":
        """
        Override catalog generation to work around Confluent Cloud's INFORMATION_SCHEMA limitations.

        Confluent Cloud doesn't support JOINs on INFORMATION_SCHEMA, so we:
        1. Query TABLES and COLUMNS with a single query
        2. Split and then join them in Python
        3. Return an agate.Table with the standard catalog structure
        """
        # Reuse the same default kwargs, although `schemas` is not used in the macro.
        kwargs = {"information_schema": information_schema, "schemas": schemas}

        # This query return both tables and columns, all with the same row structure.
        # We distinguish between them by the presence (or lack) of "table_name"/"column_name"
        # This allows us to get the catalog with a single query, which, given the
        # overhead of each query, is a significant time saving move.
        catalog = self.execute_macro("get_catalog", kwargs=kwargs)

        # Sort by database.schema.name first, so we get all the rows (table and columns) for
        # any given table in the right order.
        # Then sort based on whether table_type is None.
        # This sorts the list so that we get the table definition first, then all the
        # columns for that table.
        # Finally, sort by column_index so we can build the catalog table by simply
        # iterating over this list in order.
        catalog.sort(
            key=lambda x: (
                x["table_database"],
                x["table_schema"],
                x["table_name"],
                x["table_type"] is None,
                x["column_index"],
            )
        )
        rows = []
        table_type = None
        for row in catalog:
            if row["table_type"] is not None:
                table_type = row["table_type"]
                continue
            row["table_type"] = table_type
            rows.append(row)

        # Create agate table
        table = agate.Table.from_object(rows)

        # Filter using the base adapter's method
        return self._catalog_filter_table(table, used_schemas)

    @available
    def get_tested_model_relation(self, tested_node_unique_id, database, schema):
        """Resolve the tested model's relation from its unique_id.

        Unit tests run in a separate manifest where graph.nodes is empty,
        so we can't look up nodes directly. Instead, we extract the model
        identifier from the unique_id (format: model.<package>.<name>)
        and find the relation in the adapter's cache.
        """
        # unique_id format:
        #   non-versioned: model.<package>.<name>
        #   versioned:     model.<package>.<name>.v<version>
        _, _, name, *v = tested_node_unique_id.split(".")
        version = f"_{v[0]}" if v and v[0].startswith("v") else ""
        identifier = f"{name}{version}"
        relation = self.get_relation(database, schema, identifier)
        if relation is None:
            raise DbtDatabaseError(
                "Could not find relation for tested model with unique_id "
                f"'{tested_node_unique_id}'. Looked for relation with identifier "
                f"'{identifier}' in database '{database}', schema '{schema}'"
            )
        return relation

    @available
    def parse_unit_test_ctes(self, extra_ctes, compiled_sql):
        """Parse the CTE information injected by dbt-core for unit tests.

        dbt-core compiles unit test fixtures as CTEs with the format:
            " __dbt__cte__<name> as (\n<fixture_sql>\n)"
        and prepends them to the compiled SQL as:
            "with <cte1>, <cte2> <main_sql>"

        This method extracts each CTE's name, fixture body, and original
        model identifier, and strips the CTE prefix from the compiled SQL
        to recover the main query.

        Returns a dict with:
            - ctes: list of {cte_name, body, original_identifier} dicts
            - main_sql: the compiled SQL with the CTE prefix removed
        """
        ctes = []
        for cte in extra_ctes:
            cte_sql = cte["sql"].strip()
            # Format is: __dbt__cte__<name> as (\n<body>\n)
            as_idx = cte_sql.index(" as (")
            cte_name = cte_sql[:as_idx].strip()
            body = cte_sql[as_idx + 5 : -1]  # skip " as (" and trailing ")"
            original_identifier = cte_name.replace("__dbt__cte__", "")
            ctes.append(
                {
                    "cte_name": cte_name,
                    "body": body,
                    "original_identifier": original_identifier,
                }
            )

        # Strip the CTE prefix to get the main query
        main_sql = compiled_sql
        if ctes:
            cte_sqls = [cte["sql"] for cte in extra_ctes]
            cte_prefix = "with" + ", ".join(cte_sqls) + " "
            main_sql = compiled_sql[len(cte_prefix) :]

        return {"ctes": ctes, "main_sql": main_sql}

    @available
    def generate_schema_check_temp_name(self, identifier: str) -> str:
        """Generate the temporary table name for a model's schema drift check.

        Deterministic per model, on purpose: the temp table is normally
        dropped by post_model_hook (which runs even when the materialization
        fails), but a hard-killed process or a cleanup drop that fails leaks
        it as a real Kafka-backed topic. With a stable name, the next drift
        check reclaims the leak via its DROP TABLE IF EXISTS before
        recreating. (Concurrent runs of the same project would collide on
        this name, but they already collide on the deterministic Flink
        statement names, so this adds no new hazard.)
        """
        return "__dbt_tmp_schema_check_" + identifier

    @available
    def escape_string_literal(self, value: object) -> str:
        """Escape a value for embedding in a Flink SQL string literal ('...').

        Flink SQL escapes a single quote inside a string literal by doubling
        it. Used when rendering user-supplied config (WITH option keys/values,
        `connector`) into DDL, where an unescaped quote would break the
        statement — or terminate the literal and inject arbitrary clauses.
        """
        return str(value).replace("'", "''")

    @available
    def all_confluent_config_keys(self) -> list[str]:
        """Every dbt-confluent-specific config key any materialization recognizes.

        The Jinja caller (`validate_materialization_config` in helpers.sql)
        probes exactly these keys via `config.get(...)` -- and no others --
        so a user's own custom config keys (read by their own hooks/macros)
        are never inspected or second-guessed by this adapter, regardless of
        which key name they happen to pick.
        """
        return sorted(_ALL_CONFIG_KEYS)

    @available
    def validate_materialization_config(
        self, materialization: str, observed_config: dict[str, Any]
    ) -> None:
        """Raise CompilationError for a dbt-confluent config key set on a
        materialization that doesn't consume it.

        observed_config: every key from `all_confluent_config_keys()` that
        this model actually set (`config.get(key) is not none`), gathered by
        the calling Jinja macro, mapped to its configured value.

        `ignore_unsupported_config` (a `config(...)` list of key name
        strings) lets a model opt specific keys out of this check -- e.g. if
        a key name coincidentally collides with the user's own unrelated
        custom config read by their own hooks/macros. Deliberately list-based
        rather than a blanket on/off switch, so opting out of one false
        positive can't also silently swallow a real future mistake on a
        different key in the same model.
        """
        allowed = MATERIALIZATION_CONFIG_KEYS.get(materialization)
        if allowed is None:
            return

        ignore_list = observed_config.get("ignore_unsupported_config") or []
        if not isinstance(ignore_list, list) or not all(isinstance(k, str) for k in ignore_list):
            raise CompilationError(
                "'ignore_unsupported_config' config must be a list of config key name strings."
            )
        ignored = set(ignore_list)

        unsupported = sorted(
            key for key in observed_config if key not in allowed and key not in ignored
        )
        if not unsupported:
            return

        violations = []
        for key in unsupported:
            supported_on = sorted(
                mat for mat, keys in MATERIALIZATION_CONFIG_KEYS.items() if key in keys
            )
            supported_str = (
                ", ".join(f"'{mat}'" for mat in supported_on)
                if supported_on
                else "no materialization"
            )
            violations.append(
                f"  - '{key}' is not supported for the '{materialization}' materialization "
                f"(supported on: {supported_str})."
            )
        raise CompilationError(
            "Unsupported config:\n"
            + "\n".join(violations)
            + "\nIf this is intentional (e.g. a coincidental name collision with your own "
            "custom config read by another macro), add the key to "
            "config(ignore_unsupported_config=[...])."
        )

    @available
    def validate_distributed_by_config(self, dist: object) -> None:
        """Raise CompilationError if the `distributed_by` config is malformed.

        Called once per materialization run so downstream consumers
        (`get_distributed_by_clause`, `check_for_schema_drift`) can read the
        config directly without re-validating.

        Allowed shape: a dict with a non-empty list/tuple of non-empty
        strings under `columns`, an optional positive int under `buckets`,
        and no other keys. `bool` is a subclass of `int` in Python, so it's
        excluded explicitly to keep `True`/`False` from slipping through.
        """
        if dist is None:
            return
        if not isinstance(dist, dict):
            raise CompilationError(
                "'distributed_by' config must be a mapping with a non-empty "
                "'columns' list of column names"
            )
        columns = dist.get("columns")
        if not columns or isinstance(columns, str) or not isinstance(columns, (list, tuple)):
            raise CompilationError(
                "'distributed_by' config must be a mapping with a non-empty "
                "'columns' list of column names"
            )
        for col in columns:
            if not isinstance(col, str) or not col:
                raise CompilationError(
                    "'distributed_by.columns' must contain only non-empty strings"
                )
            if "`" in col:
                raise CompilationError(
                    f"'distributed_by.columns' must not contain backtick characters; got: {col}"
                )
        buckets = dist.get("buckets")
        if buckets is not None:
            if isinstance(buckets, bool) or not isinstance(buckets, int) or buckets <= 0:
                raise CompilationError(
                    f"'distributed_by.buckets' must be a positive integer; got: {buckets}"
                )
        unknown = set(dist.keys()) - {"columns", "buckets"}
        if unknown:
            # Sort for stable messages; mention the first to mirror the
            # previous Jinja behavior of failing on the first unknown key.
            key = sorted(unknown)[0]
            raise CompilationError(
                f"'distributed_by' has unknown key '{key}'. Allowed keys: 'columns', 'buckets'"
            )

    @available
    def validate_materialized_table_config(self, model_config: Any) -> None:
        """Reject configs Confluent's MT dialect doesn't support.

        `freshness_interval`, `refresh_mode`, and `partition_by` exist in
        open-source Flink materialized tables but not in Confluent's dialect.
        Collects every offending key into one error. `model_config` is the
        Jinja config object (anything with `.get`); distributed_by and
        start_mode are validated by their own helpers.
        """
        unsupported = [
            key
            for key in ("freshness_interval", "refresh_mode", "partition_by")
            if model_config.get(key) is not None
        ]
        if unsupported:
            keys = "', '".join(unsupported)
            verb = "is" if len(unsupported) == 1 else "are"
            raise CompilationError(
                f"'{keys}' {verb} not supported by the 'materialized_table' "
                f"materialization for Confluent Flink.\n"
                f"Supported config options are: distributed_by, with, start_mode."
            )

    @available
    def render_start_mode(self, value: object) -> str:
        """Validate the `start_mode` config and render the START_MODE value.

        Accepts the eight documented Confluent forms as a plain string (what
        users paste from the docs): FROM_BEGINNING / RESUME_OR_FROM_BEGINNING
        (no argument), FROM_TIMESTAMP / RESUME_OR_FROM_TIMESTAMP (argument
        required), FROM_NOW / RESUME_OR_FROM_NOW (argument optional). The
        keyword is normalized to uppercase and its arity enforced; the
        argument is only checked lexically (see _START_MODE_RE) and passed
        through verbatim, because the server accepts more than a plain string
        literal there — FROM_NOW takes an interval literal such as
        INTERVAL '7' DAY — and is the authority on its structure. Validation
        is eager on purpose: a bad start_mode must fail before the
        full-refresh path drops the existing table. Returns '' when the
        config is unset; raises CompilationError on anything else.
        """
        if value is None:
            return ""
        match = _START_MODE_RE.fullmatch(str(value))
        keyword = match.group("kw").upper() if match else ""
        arity = _START_MODE_ARITY.get(keyword)
        if not match or arity is None:
            raise CompilationError(
                f"'{value}' is not a valid value for 'start_mode'.\n"
                f"Accepted forms are: {_START_MODE_FORMS}."
            )
        arg = (match.group("arg") or "").strip()
        if arg in ("", "''"):  # no parens, empty parens, or empty literal
            if arity == "required":
                raise CompilationError(
                    f"'start_mode' {keyword} requires an argument, e.g. {keyword}('...')."
                )
            return keyword
        if arity == "none":
            raise CompilationError(f"'start_mode' {keyword} does not take an argument.")
        return f"{keyword}({arg})"

    @available
    def check_schema_drift(
        self,
        existing_relation: ConfluentRelation,
        temp_relation: ConfluentRelation,
        drift_catalog: "agate.Table",
        expected_with: dict[str, str],
        expected_distribution: dict | None = None,
        enforce: Literal["all", "columns"] = "all",
        expected_connector: str | None = None,
    ) -> None:
        """Compare existing vs expected schema and raise CompilationError on drift.

        drift_catalog is the agate.Table returned by `get_drift_catalog`: a
        sparse UNION ALL with a `section` discriminator (COLUMNS, TABLES,
        TABLE_OPTIONS) and a `table_name` discriminator that distinguishes the
        existing relation from the temp relation in the COLUMNS section.
        Splitting it client-side trades one round-trip for a bit of Python.

        Each helper returns a list of one-line violation strings; we collect
        them all and raise a single error so the user sees every drift in one
        run rather than fixing them one at a time.

        `enforce` controls which concerns can produce violations:
            "all"     — columns + options + distribution (default).
            "columns" — only column drift raises. Used by the streaming
                        restart path under `on_schema_drift='ignore'`, where
                        options/distribution drift is fine but a column
                        mismatch would cause Flink to reject the INSERT.

        `expected_connector` is streaming_source's mandatory `connector`
        config. The materialization renders it into the DDL's WITH clause
        (where the existing table's INFORMATION_SCHEMA options report it),
        but it lives outside the `with` config — so it's merged into the
        expected options here to participate in options drift like any
        other option. None for materializations without a connector.
        """
        (
            existing_columns,
            expected_columns,
            existing_options,
            existing_distribution,
            existing_is_materialized,
        ) = self._partition_drift_catalog(
            drift_catalog, existing_relation.identifier, temp_relation.identifier
        )

        # A materialized table can't be managed by the drop-and-recreate
        # materializations at all — a skip would silently leave Flink
        # maintaining the old defining query, and a streaming restart would
        # submit an INSERT against it.
        if existing_is_materialized:
            raise CompilationError(
                f"{existing_relation} exists as a Flink materialized table, which "
                f"cannot be managed by this model's materialization. Either set "
                f"materialized='materialized_table' on the model, or run with "
                f"--full-refresh to drop it and recreate the relation (dropping a "
                f"materialized table permanently deletes the backing Kafka topic "
                f"and its data; its Schema Registry subjects are NOT deleted and "
                f"may block the recreate under the same name until removed — see "
                f"'Switching materializations' in MATERIALIZATIONS.md)."
            )

        # An empty expected_columns means the drift-check temp table came back
        # with zero columns from INFORMATION_SCHEMA. The temp table was just
        # created from the model's column definitions / select query, so it has
        # columns; an empty result almost always means Confluent Cloud's
        # INFORMATION_SCHEMA hasn't yet propagated the freshly-created table.
        # Surface this as a retriable database error rather than letting it
        # cascade into a false "drift detected" message.
        if not expected_columns:
            raise DbtDatabaseError(
                f"Drift check could not introspect the expected schema for "
                f"'{existing_relation}': the temp table created from the model "
                f"definition returned no columns from INFORMATION_SCHEMA. "
                f"This usually indicates a transient Confluent Cloud metadata "
                f"propagation lag. Retry with `dbt retry`; if it persists, "
                f"run with `--full-refresh` or file a bug."
            )

        # Same guard, existing side: the drift check only runs when dbt's
        # cache says the relation exists, so zero COLUMNS rows for it means
        # the same metadata propagation lag (or the table was dropped
        # externally mid-run). Without this guard the check would report
        # every model column as "column added" and steer the user toward a
        # needless --full-refresh.
        if not existing_columns:
            raise DbtDatabaseError(
                f"Drift check could not introspect the existing schema for "
                f"'{existing_relation}': the table returned no columns from "
                f"INFORMATION_SCHEMA. This usually indicates a transient "
                f"Confluent Cloud metadata propagation lag (or the table was "
                f"dropped outside dbt during the run). Retry with `dbt retry`; "
                f"if it persists, run with `--full-refresh` or file a bug."
            )

        violations: list[str] = []
        violations.extend(self._check_column_drift(existing_columns, expected_columns))
        if enforce == "all":
            expected_options = dict(expected_with)
            if expected_connector is not None:
                expected_options["connector"] = expected_connector
            violations.extend(self._check_options_drift(expected_options, existing_options))
            violations.extend(
                self._check_distribution_drift(expected_distribution, existing_distribution)
            )
        if violations:
            bullets = "\n".join(f"  - {v}" for v in violations)
            raise CompilationError(
                f"Schema drift detected for {existing_relation}:\n"
                f"{bullets}\n"
                f"Use --full-refresh to recreate the table."
            )

    @staticmethod
    def _partition_drift_catalog(
        drift_catalog,
        existing_identifier: str,
        temp_identifier: str,
    ) -> tuple[dict[str, str], dict[str, str], dict[str, str], dict | None, bool]:
        """Split the unified UNION ALL result into per-concern structures.

        Returns:
            existing_columns: {column_name: data_type} for the existing table
            expected_columns: {column_name: data_type} for the temp table
            existing_options: {option_key: option_value}
            existing_distribution: {buckets, columns} or None
            existing_is_materialized: True if the existing relation is a
                Flink materialized table (IS_MATERIALIZED='YES')
        """
        columns_by_table: dict[str, dict[str, str]] = {
            existing_identifier: {},
            temp_identifier: {},
        }
        existing_options: dict[str, str] = {}
        is_distributed = False
        is_materialized = False
        buckets: int | None = None
        positions: list[tuple[int, str]] = []

        for row in drift_catalog:
            section = row["section"]
            if section == "COLUMNS":
                # Defensive: get_drift_catalog filters TABLE_NAME to existing/temp,
                # so a None target shouldn't happen — but skip rather than crash if
                # INFORMATION_SCHEMA ever returns an unexpected row.
                target = columns_by_table.get(row["table_name"])
                if target is None:
                    fire_event(
                        AdapterEventWarning(
                            base_msg=(
                                f"Got empty table during drift check for columns in {row['table_name']}. "
                                "Columns drift detection skipped, this is probably a bug."
                            )
                        )
                    )
                    continue
                target[row["col_name"]] = row["data_type"]
                if row["table_name"] == existing_identifier and row["dist_position"] is not None:
                    positions.append((row["dist_position"], row["col_name"]))
            elif section == "TABLES":
                if str(row["is_materialized"]).upper() == "YES":
                    is_materialized = True
                if str(row["is_distributed"]).upper() == "YES":
                    is_distributed = True
                    buckets = row["dist_buckets"]
            elif section == "TABLE_OPTIONS":
                existing_options[row["option_key"]] = row["option_value"]

        existing_distribution: dict | None = None
        if is_distributed:
            existing_distribution = {
                "buckets": buckets,
                "columns": [col for _, col in sorted(positions)],
            }
        return (
            columns_by_table[existing_identifier],
            columns_by_table[temp_identifier],
            existing_options,
            existing_distribution,
            is_materialized,
        )

    @staticmethod
    def _check_column_drift(
        existing_map: dict[str, str],
        expected_map: dict[str, str],
    ) -> list[str]:
        """Return one violation string per added/removed/type-changed column.

        Both maps come from INFORMATION_SCHEMA.COLUMNS so types are already
        in Flink's canonical form — no normalization needed. Sorted output
        keeps error messages stable across runs.
        """
        violations: list[str] = []
        existing_names = set(existing_map)
        expected_names = set(expected_map)
        for added in sorted(expected_names - existing_names):
            violations.append(f"column added: '{added}'")
        for removed in sorted(existing_names - expected_names):
            violations.append(f"column removed: '{removed}'")
        for col in sorted(existing_names & expected_names):
            if existing_map[col] != expected_map[col]:
                violations.append(
                    f"column type: '{col}' "
                    f"existing='{existing_map[col]}', expected='{expected_map[col]}'"
                )
        return violations

    @staticmethod
    def _check_options_drift(
        expected_with: dict[str, str],
        existing_options: dict[str, str],
    ) -> list[str]:
        """Return one violation per WITH option whose value has drifted.

        Coerces the expected value to str before comparing — `INFORMATION_SCHEMA`
        always returns option values as strings, so an `int 1` in the model
        config matches the existing `'1'`.
        """
        violations: list[str] = []
        for key, value in expected_with.items():
            existing_value = existing_options.get(key)
            if existing_value != str(value):
                shown = existing_value if existing_value is not None else "<not set>"
                violations.append(f"option: '{key}' existing='{shown}', expected='{str(value)}'")
        return violations

    @staticmethod
    def _check_distribution_drift(
        expected: dict | None,
        existing: dict | None,
    ) -> list[str]:
        """Return violations for `distributed_by` mismatches.

        Confluent assigns a default distribution to every Kafka-backed table
        (typically derived from the primary key), and INFORMATION_SCHEMA does
        not distinguish user-specified from auto-assigned distribution. We
        therefore mirror the WITH-options check: only verify what the user
        explicitly requested via `config(distributed_by=...)`. Detects column
        and bucket-count mismatches when set; cannot detect removal because
        the auto-assigned default would falsely trigger every run.
        """
        if expected is None:
            return []
        if existing is None:
            return [f"distribution: existing=<none>, expected={expected}"]
        violations: list[str] = []
        expected_cols = list(expected["columns"])
        if existing["columns"] != expected_cols:
            violations.append(
                f"distribution columns: existing={existing['columns']}, expected={expected_cols}"
            )
        expected_buckets = expected.get("buckets")
        if expected_buckets is not None and existing["buckets"] != expected_buckets:
            violations.append(
                f"distribution buckets: existing={existing['buckets']}, expected={expected_buckets}"
            )
        return violations

    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor(mode=conn.credentials.execution_mode)
        try:
            cursor.execute(sql)
            if hasattr(conn.handle, "commit"):
                conn.handle.commit()
            if fetch == "one":
                return fetch_from_cursor(cursor, limit=1)
            elif fetch == "all":
                return fetch_from_cursor(cursor)
            else:
                return
        except BaseException as e:
            if conn.handle and not getattr(conn.handle, "closed", True):
                conn.handle.rollback()
            logger.exception(sql)
            raise e
        finally:
            conn.transaction_open = False
            cursor.close()
