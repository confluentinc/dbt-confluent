import logging
import re
import time
from dataclasses import dataclass, field

import agate
from confluent_sql.exceptions import StatementNotFoundError
from dbt_common.events.contextvars import get_node_info
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.base import BaseRelation, available
from dbt.adapters.base.impl import InformationSchema, _parse_callback_empty_table
from dbt.adapters.confluent import ConfluentColumn, ConfluentConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.relation import Policy
from dbt.adapters.sql import SQLAdapter

from .naming import sanitize_statement_name
from .utils import fetch_from_cursor

logger = logging.getLogger(__name__)


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


class ConfluentAdapter(SQLAdapter):
    """
    Controls actual implementation of adapter, and ability to override certain methods.
    """

    ConnectionManager: type[ConfluentConnectionManager] = ConfluentConnectionManager
    connections: ConfluentConnectionManager
    Relation: type[ConfluentRelation] = ConfluentRelation
    Column: type[ConfluentColumn] = ConfluentColumn

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
    ) -> tuple[AdapterResponse, "agate.Table"]:
        return self.connections.execute(
            sql=sql,
            auto_begin=auto_begin,
            fetch=fetch,
            limit=limit,
            execution_mode=execution_mode,
            hidden=hidden,
            statement_name=statement_name,
        )

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

    @available
    def delete_statement(self, statement_name: str) -> None:
        """Delete a Flink statement by name and wait for deletion to complete.

        Deletion of RUNNING statements is async (the job must stop first),
        so we poll until the statement is gone (404).
        No-op if the statement doesn't already exist.
        """
        conn = self.connections.get_thread_connection()
        handle = conn.handle

        # Send the delete request (no-op on 404).
        handle.delete_statement(statement_name)

        # Check if the statement is already gone after the delete request.
        try:
            handle.get_statement(statement_name)
        except StatementNotFoundError:
            return  # Already gone (either deleted instantly or never existed)

        # Statement still exists — it's being stopped. Poll until gone.
        logger.info("Waiting for statement '%s' to be deleted...", statement_name)
        max_wait = 60
        waited = 0
        attempt = 1
        while waited < max_wait:
            backoff = min(2**attempt, 15)
            time.sleep(backoff)
            waited += backoff
            attempt += 1
            try:
                handle.get_statement(statement_name)
            except StatementNotFoundError:
                return  # Successfully deleted

        raise DbtDatabaseError(
            f"Statement '{statement_name}' still exists after {waited}s. "
            f"Flink may still be stopping the job."
        )

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
    def generate_schema_check_temp_name(self, identifier: str, invocation_id: str) -> str:
        """Generate a unique temporary table name for schema drift checks."""
        return "__dbt_tmp_schema_check_" + identifier + "_" + invocation_id.replace("-", "")

    @available
    def check_schema_drift(
        self,
        relation_name: str,
        existing_columns,
        expected_columns,
        expected_with: dict[str, str],
        existing_options: dict[str, str],
    ) -> None:
        """Compare existing vs expected schema and raise CompilationError on drift.

        existing_columns: agate.Table from INFORMATION_SCHEMA query
        expected_columns: agate.Table from INFORMATION_SCHEMA query (via temp table)
        expected_with: config(with={...}) dict
        existing_options: dict from INFORMATION_SCHEMA.TABLE_OPTIONS
        """
        # No type normalization: both existing and expected columns come from
        # INFORMATION_SCHEMA.COLUMNS queries, so types are already in Flink's
        # canonical form.
        existing_map = {col["column_name"]: col["data_type"] for col in existing_columns}
        expected_map = {col["column_name"]: col["data_type"] for col in expected_columns}

        existing_names = sorted(existing_map)
        expected_names = sorted(expected_map)

        if existing_names != expected_names:
            raise CompilationError(
                f"Schema drift detected for '{relation_name}'.\n"
                f"Existing columns: {existing_names}\n"
                f"Expected columns: {expected_names}\n"
                f"Use --full-refresh to recreate the table."
            )

        for col_name in expected_map:
            if existing_map[col_name] != expected_map[col_name]:
                raise CompilationError(
                    f"Schema drift detected for '{relation_name}'.\n"
                    f"Column '{col_name}' type mismatch: "
                    f"existing='{existing_map[col_name]}', expected='{expected_map[col_name]}'.\n"
                    f"Use --full-refresh to recreate the table."
                )

        for key, value in expected_with.items():
            existing_value = existing_options.get(key)
            if existing_value != str(value):
                raise CompilationError(
                    f"Table options drift detected for '{relation_name}'.\n"
                    f"Option '{key}': "
                    f"existing='{existing_value or '<not set>'}', expected='{str(value)}'.\n"
                    f"Use --full-refresh to recreate the table."
                )

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
