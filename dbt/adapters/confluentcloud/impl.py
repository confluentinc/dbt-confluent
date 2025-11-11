from dbt.adapters.sql import SQLAdapter

from dbt.adapters.confluentcloud import ConfluentCloudConnectionManager
from dbt.adapters.base import BaseRelation

class ConfluentCloudAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = ConfluentCloudConnectionManager

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

    @classmethod
    def quote(cls, identifier):
        return f"'{identifier}'"

    def list_schemas(self, database: str) -> list[str]:
        return ["dbt_adapter"]

    def create_schema(self, relation: BaseRelation) -> None:
        # TODO: Should we create a schema, or just check that it exists?
        pass
        # Superclass implementation:
        # relation = relation.without_identifier()
        # fire_event(SchemaCreation(relation=_make_ref_key_dict(relation)))
        # kwargs = {
        #     "relation": relation,
        # }
        # self.execute_macro(CREATE_SCHEMA_MACRO_NAME, kwargs=kwargs)
        # self.commit_if_has_connection()
        # # we can't update the cache here, as if the schema already existed we
        # # don't want to (incorrectly) say that it's empty

    def list_relations_without_caching(
        self,
        schema_relation: BaseRelation,
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
        breakpoint()
        return relations
