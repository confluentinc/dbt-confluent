from typing import ClassVar

from dbt.adapters.base.column import Column


class ConfluentColumn(Column):
    """
    Custom Column class for Confluent adapter.

    Overrides the quoted property to use backticks instead of double quotes,
    as Confluent Cloud SQL requires backticks for identifiers.
    """

    # dbt-core's default TYPE_LABELS maps STRING -> TEXT, which contract
    # enforcement (ctx_model in dbt-core's providers.py) uses to translate a
    # model's declared `data_type: string` before both the schema-probe query
    # and the rendered column DDL. Flink SQL has no TEXT type (only
    # STRING/VARCHAR), so that translation broke every contract-enforced
    # model with a string column ("Unknown identifier 'TEXT'"). See #92.
    #
    # Deliberately empty rather than {"STRING": "STRING"}: translate_type's
    # fallback (`TYPE_LABELS.get(dtype.upper(), dtype)`) returns the original,
    # un-uppercased dtype on a miss, so an empty map makes STRING pass through
    # verbatim in whatever case the user wrote -- the same untouched
    # passthrough every other type (bigint, decimal(10,2), ...) already gets
    # here. A `{"STRING": "STRING"}` entry would instead silently normalize
    # STRING's case while leaving every other type's case alone, an asymmetry
    # nothing here requires.
    TYPE_LABELS: ClassVar[dict[str, str]] = {}

    @property
    def quoted(self) -> str:
        return f"`{self.column}`"
