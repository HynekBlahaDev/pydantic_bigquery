import json
from datetime import date, datetime
from enum import Enum
from typing import Any, List, Optional

import pydantic
from google.cloud import bigquery

from .constants import BigQueryMode


class BigQueryModel(pydantic.BaseModel):
    __TABLE_NAME__: str
    __PARTITION_FIELD__: Optional[str] = None
    __CLUSTERING_FIELDS__: List[str] = []

    @classmethod
    def get_bigquery_schema(cls) -> List[bigquery.SchemaField]:
        return [cls._get_schema_field(field) for field in cls.__fields__.values()]

    @classmethod
    def _get_schema_field(cls, field: pydantic.fields.ModelField) -> bigquery.SchemaField:
        schema_type = cls._get_schema_field_type(field)
        schema_mode = cls._get_schema_field_mode(field)

        return bigquery.SchemaField(
            name=field.name,
            field_type=str(schema_type.value),
            mode=str(schema_mode.value),
        )

    @staticmethod
    def _get_schema_field_type(
        field: pydantic.fields.ModelField,
    ) -> bigquery.SqlTypeNames:
        if field.type_ == int:
            return bigquery.SqlTypeNames.INTEGER
        if field.type_ == float:
            return bigquery.SqlTypeNames.FLOAT
        if field.type_ == str or issubclass(field.type_, Enum):
            return bigquery.SqlTypeNames.STRING
        if field.type_ == bool:
            return bigquery.SqlTypeNames.BOOLEAN
        if field.type_ == date:
            return bigquery.SqlTypeNames.DATE
        if field.type_ == datetime:
            return bigquery.SqlTypeNames.TIMESTAMP

        raise NotImplementedError(f"Unknown type: {field.type_}")

    @staticmethod
    def _get_schema_field_mode(field: pydantic.fields.ModelField) -> BigQueryMode:
        if field.shape == pydantic.fields.SHAPE_SINGLETON:
            if field.allow_none:
                return BigQueryMode.NULLABLE
            return BigQueryMode.REQUIRED

        if field.shape in (
            pydantic.fields.SHAPE_LIST,
            pydantic.fields.SHAPE_SET,
            pydantic.fields.SHAPE_TUPLE,
        ):
            if not field.allow_none:
                return BigQueryMode.REPEATED

        raise NotImplementedError(f"Unknown combination: shape={field.shape}, required={field.required}")

    def bq_dict(self) -> Any:
        # Conversion hack = Use pydantic encoders (datetime, enum -> str)
        return json.loads(self.json())
