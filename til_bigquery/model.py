import json
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, List, Optional
from uuid import UUID, uuid4

from google.cloud import bigquery
from pydantic import BaseModel, Extra, Field
from pydantic.fields import SHAPE_LIST, SHAPE_SET, SHAPE_SINGLETON, SHAPE_TUPLE, ModelField

from .constants import BigQueryMode


class BigQueryModelBase(BaseModel):
    __TABLE_NAME__: str
    __PARTITION_FIELD__: Optional[str] = None
    __CLUSTERING_FIELDS__: List[str] = []

    class Config:
        extra = Extra.forbid
        validate_all = True
        validate_assignment = True

    @classmethod
    def get_bigquery_schema(cls) -> List[bigquery.SchemaField]:
        return [cls._get_schema_field(field) for field in cls.__fields__.values()]

    @classmethod
    def _get_schema_field(cls, field: ModelField) -> bigquery.SchemaField:
        schema_type = cls._get_schema_field_type(field)
        schema_mode = cls._get_schema_field_mode(field)

        return bigquery.SchemaField(
            name=field.name,
            field_type=str(schema_type.value),
            mode=str(schema_mode.value),
        )

    @staticmethod
    def _get_schema_field_type(
        field: ModelField,
    ) -> bigquery.enums.SqlTypeNames:
        if field.type_ == int:
            return bigquery.enums.SqlTypeNames.INTEGER
        if field.type_ == float:
            return bigquery.enums.SqlTypeNames.FLOAT
        if field.type_ in (str, UUID) or issubclass(field.type_, Enum):
            return bigquery.enums.SqlTypeNames.STRING
        if field.type_ == bool:
            return bigquery.enums.SqlTypeNames.BOOLEAN
        if field.type_ == date:
            return bigquery.enums.SqlTypeNames.DATE
        if field.type_ == datetime:
            return bigquery.enums.SqlTypeNames.TIMESTAMP

        raise NotImplementedError(f"Unknown type: {field.type_}")

    @staticmethod
    def _get_schema_field_mode(field: ModelField) -> BigQueryMode:
        if field.shape == SHAPE_SINGLETON:
            if field.allow_none:
                return BigQueryMode.NULLABLE
            return BigQueryMode.REQUIRED

        if field.shape in (
            SHAPE_LIST,
            SHAPE_SET,
            SHAPE_TUPLE,
        ):
            if not field.allow_none:
                return BigQueryMode.REPEATED

        raise NotImplementedError(f"Unknown combination: shape={field.shape}, required={field.required}")

    def bq_dict(self) -> Any:
        # Conversion hack = Use pydantic encoders (datetime, enum -> str)
        return json.loads(self.json())


class BigQueryModel(BigQueryModelBase):
    insert_id: UUID = Field(default_factory=uuid4)
    inserted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class BigQueryModelLegacy(BigQueryModelBase):
    insert_id: UUID = Field(default_factory=uuid4)
    time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
