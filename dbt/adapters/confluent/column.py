from dbt.adapters.base.column import Column


class ConfluentColumn(Column):
    """
    Custom Column class for Confluent adapter.

    Overrides the quoted property to use backticks instead of double quotes,
    as Confluent Cloud SQL requires backticks for identifiers.
    """

    @property
    def quoted(self) -> str:
        return f"`{self.column}`"
