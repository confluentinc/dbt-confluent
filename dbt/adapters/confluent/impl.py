import builtins
import re
from dataclasses import dataclass, field
from typing import Callable

import agate
from dbt_common.dataclass_schema import StrEnum
from dbt_common.events.contextvars import get_node_info
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.base import BaseRelation
from dbt.adapters.confluent import ConfluentConnectionManager
from dbt.adapters.contracts.relation import Policy, RelationType
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.utils import classproperty


class ConfluentRelationType(StrEnum):
    Table = "BASE TABLE"
    External = "EXTERNAL TABLE"
    SystemTable = "SYSTEM TABLE"
    View = "VIEW"
    CTE = "cte"
    MaterializedView = "materialized_view"
    Ephemeral = "ephemeral"


@dataclass(frozen=True, eq=False, repr=False)
class ConfluentRelation(BaseRelation):
    type: ConfluentRelationType | str | None = None
    quote_character: str = "`"
    include_policy: Policy = field(
        default_factory=lambda: Policy(database=False, schema=True, identifier=True)
    )

    def __post_init__(self):
        # TODO: This feels a bit forced, there may be a better way of handling
        # the different names of relation types, but it works for now.
        normalized_type = None

        if self.type is not None:
            if isinstance(self.type, ConfluentRelationType):
                normalized_type = self.type
            elif isinstance(self.type, str):
                # Convert to lowercase because the default relation types are lowercase.
                type_str = self.type.lower()

                if type_str == RelationType.Table:
                    normalized_type = ConfluentRelationType.Table
                elif type_str == RelationType.View:
                    normalized_type = ConfluentRelationType.View
                elif type_str == RelationType.External:
                    normalized_type = ConfluentRelationType.External
                # elif type_str == RelationType.CTE or type_str == RelationType.Ephemeral:
                #     # CTE and Ephemeral are not database objects, keep them as strings
                #     normalized_type = type_str
                else:
                    normalized_type = ConfluentRelationType(self.type)
        object.__setattr__(self, "type", normalized_type)

    @classproperty
    def Table(cls) -> str:
        return str(ConfluentRelationType.Table)

    @classproperty
    def View(cls) -> str:
        return str(ConfluentRelationType.View)

    @classproperty
    def External(cls) -> str:
        return str(ConfluentRelationType.External)

    @classproperty
    def get_relation_type(cls) -> builtins.type[ConfluentRelationType]:
        return ConfluentRelationType

    @property
    def is_table(self) -> bool:
        """
        Overridden property.
        Checks if the relation type is a Table.
        """
        return self.type == ConfluentRelationType.Table

    @property
    def is_view(self) -> bool:
        """
        Overridden property.
        Checks if the relation type is a View.
        """
        return self.type == ConfluentRelationType.View

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
        return ".".join(
            [f"`{p}`" for p in [self.database, self.schema, self.identifier] if p]
        )


class ConfluentAdapter(SQLAdapter):
    """
    Controls actual implementation of adapter, and ability to override certain methods.
    """

    ConnectionManager: type[ConfluentConnectionManager] = ConfluentConnectionManager
    Relation: type[ConfluentRelation] = ConfluentRelation

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
        if from_relation.type != ConfluentRelationType.View:
            raise DbtDatabaseError(
                f"Renaming is only supported in views, got {from_relation.type}"
            )

        self.cache_renamed(from_relation, to_relation)

        # Now, to manually duplicate a view, we first need to get its definition using a SHOW
        _, res = self.execute(
            f"SHOW CREATE VIEW {from_relation.identifier}", fetch=True
        )
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
