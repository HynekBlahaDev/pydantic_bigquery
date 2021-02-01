from datetime import date, datetime, timezone
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from google.cloud.bigquery import SchemaField

from til_bigquery import BigQueryModel


class ExampleEnum(Enum):
    FOO = "FOO"
    BAR = "BAR"


class ExampleModel(BigQueryModel):
    __TABLE_NAME__: str = "example_model"
    __PARTITION_FIELD__: Optional[str] = "inserted_at"
    __CLUSTERING_FIELDS__: List[str] = ["my_string", "my_enum"]

    # We could move these properties to the parent scope => the same basic structure for all tables.
    # I don't have a strong opinion, this is just an example how common fields might be set.
    insert_id: str = str(uuid4())
    inserted_at: datetime = datetime.now(timezone.utc)

    my_string: str
    my_integer: int
    my_float: float
    my_bool: bool
    my_date: date
    my_datetime: datetime
    my_enum: ExampleEnum

    my_nullable_string: Optional[str]
    my_nullable_integer: Optional[int]
    my_nullable_float: Optional[float]
    my_nullable_bool: Optional[bool]
    my_nullable_date: Optional[date]
    my_nullable_datetime: Optional[datetime]

    my_repeatable_string: List[str]
    my_repeatable_integer: List[int]
    my_repeatable_float: List[float]
    my_repeatable_bool: List[bool]
    my_repeatable_date: List[date]
    my_repeatable_datetime: List[datetime]


def test_get_schema() -> None:
    result = ExampleModel.get_bigquery_schema()
    expected = [
        SchemaField("insert_id", "STRING", "REQUIRED", None, (), None),
        SchemaField("inserted_at", "TIMESTAMP", "REQUIRED", None, (), None),
        SchemaField("my_string", "STRING", "REQUIRED", None, (), None),
        SchemaField("my_integer", "INTEGER", "REQUIRED", None, (), None),
        SchemaField("my_float", "FLOAT", "REQUIRED", None, (), None),
        SchemaField("my_bool", "BOOLEAN", "REQUIRED", None, (), None),
        SchemaField("my_date", "DATE", "REQUIRED", None, (), None),
        SchemaField("my_datetime", "TIMESTAMP", "REQUIRED", None, (), None),
        SchemaField("my_enum", "STRING", "REQUIRED", None, (), None),
        SchemaField("my_nullable_string", "STRING", "NULLABLE", None, (), None),
        SchemaField("my_nullable_integer", "INTEGER", "NULLABLE", None, (), None),
        SchemaField("my_nullable_float", "FLOAT", "NULLABLE", None, (), None),
        SchemaField("my_nullable_bool", "BOOLEAN", "NULLABLE", None, (), None),
        SchemaField("my_nullable_date", "DATE", "NULLABLE", None, (), None),
        SchemaField("my_nullable_datetime", "TIMESTAMP", "NULLABLE", None, (), None),
        SchemaField("my_repeatable_string", "STRING", "REPEATED", None, (), None),
        SchemaField("my_repeatable_integer", "INTEGER", "REPEATED", None, (), None),
        SchemaField("my_repeatable_float", "FLOAT", "REPEATED", None, (), None),
        SchemaField("my_repeatable_bool", "BOOLEAN", "REPEATED", None, (), None),
        SchemaField("my_repeatable_date", "DATE", "REPEATED", None, (), None),
        SchemaField("my_repeatable_datetime", "TIMESTAMP", "REPEATED", None, (), None),
    ]

    assert result == expected
