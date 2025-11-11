from dataclasses import dataclass, field

import agate
from dbt.adapters.base import BaseRelation
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.contracts.relation import Policy
from dbt_common.events.contextvars import get_node_info
import dbt

from dbt.adapters.confluentcloud import ConfluentCloudConnectionManager


@dataclass(frozen=True, eq=False, repr=False)
class ConfluentCloudRelation(BaseRelation):
    quote_character: str = "`"
    include_policy: Policy = field(
        default_factory=lambda: Policy(database=False, schema=True, identifier=True)
    )

    def quoted(self, identifier):
        # TODO: Maybe we should escape it, but confluent sql docs are not explicit about
        # how to escape identifiers. Empirically, duplicating the quote character works:
        # `test``-identifier` is correctly interpreted as "test`-identifier", althouth using the
        # quote character itself in an identifier returns an error from the server.
        if self.quote_character in identifier:
            # TODO: Is this the right error?
            raise dbt.exceptions.CompilationError(
                f"Quote character '{self.quote_character}' can't be used in identifiers!",
                get_node_info(),
            )
        # return f"{self.quote_character}{identifier}{self.quote_character}"
        return f"{identifier}"


class ConfluentCloudAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = ConfluentCloudConnectionManager
    Relation = ConfluentCloudRelation


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

    @classmethod
    def quote(cls, identifier: str):
        # escaped = identifier.replace("'", "''")
        if "`" in identifier:
            raise ValueError("Identifier contains '`', that's illegal")
        return f"`{identifier}`"

    def list_schemas(self, database: str) -> list[str]:
        res = super().list_schemas(database)
        # Remove duplicates here since we can't use a DISTINCT on INFORMATION_SCHEMA
        return list(set(res))

    # def list_schemas(self, database: str) -> List[str]:
    #     results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})

    #     return [row[0] for row in results]

    # def create_schema(self, relation: ConfluentCloudConnectionManager) -> None:
    #     breakpoint()
    #     # TODO: Should we create a schema, or just check that it exists?
    #     pass
    #     # Superclass implementation:
    #     # relation = relation.without_identifier()
    #     # fire_event(SchemaCreation(relation=_make_ref_key_dict(relation)))
    #     # kwargs = {
    #     #     "relation": relation,
    #     # }
    #     # self.execute_macro(CREATE_SCHEMA_MACRO_NAME, kwargs=kwargs)
    #     # self.commit_if_has_connection()
    #     # # we can't update the cache here, as if the schema already existed we
    #     # # don't want to (incorrectly) say that it's empty

    def list_relations(self, database, schema):
        return super().list_relations(database, schema)

    def list_relations_without_caching(
        self,
        schema_relation: ConfluentCloudRelation,
    ) -> list[BaseRelation]:
        kwargs = {"schema_relation": schema_relation}
        results = self.execute_macro("list_relations_without_caching", kwargs=kwargs)

        relations = []
        quote_policy = {"database": True, "schema": True, "identifier": True}
        for _database, name, _schema, _type in results:
            try:
                _type = self.Relation.get_relation_type(_type)
            except ValueError:
                _type = self.Relation.External
            relations.append(
                self.Relation.create(
                    database=_database,
                    schema=_schema,
                    identifier=name,
                    quote_policy=quote_policy,
                    type=_type,
                )
            )
        return relations
