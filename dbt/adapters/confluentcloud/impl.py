import datetime
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Type

import agate
from dbt.adapters.base import BaseRelation
from dbt.adapters.contracts.relation import Policy, RelationType
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.utils import classproperty
from dbt_common.dataclass_schema import StrEnum
from dbt_common.events.contextvars import get_node_info
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.confluentcloud import ConfluentCloudConnectionManager


class ConfluentCloudRelationType(StrEnum):
    Table = "BASE TABLE"
    External = "EXTERNAL TABLE"
    SystemTable = "SYSTEM TABLE"
    View = "VIEW"


@dataclass(frozen=True, eq=False, repr=False)
class ConfluentCloudRelation(BaseRelation):
    type: ConfluentCloudRelationType | str | None = None
    quote_character: str = "`"
    include_policy: Policy = field(
        default_factory=lambda: Policy(database=False, schema=True, identifier=True)
    )

    def __post_init__(self):
        normalized_type = None

        if self.type is not None:
            if isinstance(self.type, ConfluentCloudRelationType):
                normalized_type = self.type
            elif isinstance(self.type, str):
                type_str = self.type.lower()

                if type_str == RelationType.Table:
                    normalized_type = ConfluentCloudRelationType.Table
                elif type_str == RelationType.View:
                    normalized_type = ConfluentCloudRelationType.View
                elif type_str == RelationType.External:
                    normalized_type = ConfluentCloudRelationType.External
                else:
                    normalized_type = ConfluentCloudRelationType(self.type)
        object.__setattr__(self, 'type', normalized_type)

    @classproperty
    def Table(cls) -> str:
        return str(ConfluentCloudRelationType.Table)

    @classproperty
    def View(cls) -> str:
        return str(ConfluentCloudRelationType.View)

    @classproperty
    def External(cls) -> str:
        return str(ConfluentCloudRelationType.External)

    @classproperty
    def get_relation_type(cls) -> Type[ConfluentCloudRelationType]:
        return ConfluentCloudRelationType

    @property
    def is_table(self) -> bool:
        """
        Overridden property.
        Checks if the relation type is a Table.
        """
        return self.type == ConfluentCloudRelationType.Table

    @property
    def is_view(self) -> bool:
        """
        Overridden property.
        Checks if the relation type is a View.
        """
        return self.type == ConfluentCloudRelationType.View

    def quoted(self, identifier):
        # TODO: Maybe we should escape it, but confluent sql docs are not explicit about
        # how to escape identifiers. Empirically, duplicating the quote character works:
        # `test``-identifier` is correctly interpreted as "test`-identifier", althouth using the
        # quote character itself in an identifier returns an error from the server.
        if self.quote_character in identifier:
            # TODO: Is this the right error?
            raise CompilationError(
                f"Quote character '{self.quote_character}' can't be used in identifiers!",
                get_node_info(),
            )
        return f"{self.quote_character}{identifier}{self.quote_character}"

    def make_confluent_fqn(self):
        return ".".join([f"`{p}`" for p in [self.database, self.schema, self.identifier] if p])


class ConfluentCloudAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager: Type[ConfluentCloudConnectionManager] = ConfluentCloudConnectionManager
    Relation: Type[ConfluentCloudRelation] = ConfluentCloudRelation

    def quote(self, identifier: str) -> str:
        """
        Quotes identifiers (table names, column names, schemas) with backticks.
        """
        return "`{}`".format(identifier)

    def get_relation(self, database: str, schema: str, identifier: str):
        res = super().get_relation(database, schema, identifier)
        return res

    def check_schema_exists(self, database: str, schema: str) -> bool:
        # breakpoint()
        results = self.execute_macro("list_schemas", kwargs={"database": database})
        exists = True if schema in [row[0] for row in results] else False
        return exists

    def list_relations_without_caching(self, schema):
        res = super().list_relations_without_caching(schema)
        # breakpoint()
        return res

    def list_schemas(self, database: str) -> list[str]:
        # breakpoint()
        res = super().list_schemas(database)
        # Remove duplicates here since we can't use a DISTINCT on INFORMATION_SCHEMA
        return list(set(res))

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

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
        """
        if from_relation.type != ConfluentCloudRelationType.View:
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

