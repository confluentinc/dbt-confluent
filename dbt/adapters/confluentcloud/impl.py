import re
from dataclasses import dataclass, field
from decimal import Decimal
import datetime

import agate
from dbt.adapters.base import BaseRelation
from dbt.adapters.contracts.relation import Policy, RelationType
from dbt.adapters.exceptions import RelationTypeNullError
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.utils import classproperty
from dbt.exceptions import CompilationError
from dbt_common.dataclass_schema import StrEnum
from dbt_common.events.contextvars import get_node_info
from typing import Type

from dbt.adapters.confluentcloud import ConfluentCloudConnectionManager


class ConfluentCloudRelationType(StrEnum):
    Table = "BASE TABLE"
    External = "EXTERNAL TABLE"
    SystemTable = "SYSTEM TABLE"
    View = "VIEW"
    # CTE = "cte"
    # DynamicTable = "dynamic_table"
    # Function = "function"



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
        # return f"{self.quote_character}{identifier}{self.quote_character}"
        return f"{identifier}"


class ConfluentCloudAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager: Type[ConfluentCloudConnectionManager] = ConfluentCloudConnectionManager
    Relation: Type[ConfluentCloudRelation] = ConfluentCloudRelation

    def quote(self, value) -> str:
        """
        Escapes a value for safe insertion into a Flink SQL query.
    
        - None -> NULL
        - str -> 'escaped string'
        - int/float -> 123
        - bool -> TRUE / FALSE
        - datetime -> TIMESTAMP '...'
        """
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes by doubling them
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        elif isinstance(value, (int, float, Decimal)):
            # Numbers are safe to render directly
            return f"{value}"
        elif isinstance(value, bool):
            # Flink SQL uses TRUE/FALSE keywords
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (datetime.datetime, datetime.date)):
            # Format as ISO string and use Flink SQL's TIMESTAMP literal
            return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='microseconds')}'"
        else:
            # Fallback for other types
            escaped_value = str(value).replace("'", "''")
            return f"'{escaped_value}'"

    def check_schema_exists(self, database: str, schema: str) -> bool:
        results = self.execute_macro("list_schemas", kwargs={"database": database})
        exists = True if schema in [row[0] for row in results] else False
        return exists

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
        # TODO CT-211
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))  # type: ignore[attr-defined]
        return "FLOAT" if decimals else "INT"

    @classmethod
    def convert_integer_type(cls, agate_table, col_idx):
        return "INT"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx) -> str:
        return "TIMESTAMP"

    def rename_relation(self, from_relation, to_relation):
        self.cache_renamed(from_relation, to_relation)

        _, res = self.execute(f"SHOW CREATE VIEW {from_relation.identifier}", fetch=True)
        ddl = res[0].values()[0]

        def make_confluent_fqn(rel):
            return ".".join(
                [f"`{p}`" for p in [rel.database, rel.schema, rel.identifier] if p]
            )

        old_fqn = make_confluent_fqn(from_relation)
        new_fqn = make_confluent_fqn(to_relation)

        pattern = re.compile(
            rf"(CREATE\s+VIEW\s+){re.escape(old_fqn)}(?=(\s|\(|\\n|$))", 
            re.IGNORECASE | re.MULTILINE
        )

        new_ddl = pattern.sub(rf"\1{new_fqn}", ddl, count=1)
        self.execute(new_ddl)
        self.execute(f"DROP VIEW {old_fqn}")

    def list_schemas(self, database: str) -> list[str]:
        res = super().list_schemas(database)
        # Remove duplicates here since we can't use a DISTINCT on INFORMATION_SCHEMA
        return list(set(res))

    # def list_relations_without_caching(
    #     self,
    #     schema_relation: ConfluentCloudRelation,
    # ) -> list[ConfluentCloudRelation]:
    #     kwargs = {"schema_relation": schema_relation}
    #     results = self.execute_macro("list_relations_without_caching", kwargs=kwargs)
    #     print("RESULTS:", results)

    #     relations = []
    #     quote_policy = {"database": True, "schema": True, "identifier": True}
    #     for database, name, schema, rel_type in results:
    #         print(rel_type)
    #         try:
    #             rel_type = self.Relation.get_relation_type(rel_type)
    #         except ValueError:
    #             rel_type = self.Relation.External
    #         relations.append(
    #             self.Relation.create(
    #                 database=database,
    #                 schema=schema,
    #                 identifier=name,
    #                 quote_policy=quote_policy,
    #                 type=rel_type,
    #             )
    #         )
    #     print("RELATIONS:", relations)
    #     return relations

