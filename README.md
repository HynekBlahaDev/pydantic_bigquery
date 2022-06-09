Pydantic-BigQuery
==============
### About
Pydantic-BigQuery integrates [pydantic models](https://pydantic-docs.helpmanual.io/) with [bigquery Client](https://googleapis.dev/python/bigquery/latest/index.html).

There are two main objects:<br />
**BigQueryModel** = base class for BigQuery table (structure is validated by pydantic).<br />
**BigQueryRepository** = base class for interaction with BigQuery engine (creating datasets, tables, saving, loading).

Model usage:
```python
from pydantic_bigquery import BigQueryModel
from typing import Optional, List
from enum import Enum
from datetime import date, datetime, timedelta, timezone

class ExampleEnum(str, Enum):
    FOO = "FOO"
    BAR = "BAR"

class ExampleModel(BigQueryModel):
    __TABLE_NAME__: str = "example_model"
    __PARTITION_FIELD__: Optional[str] = "inserted_at"
    __CLUSTERING_FIELDS__: List[str] = ["my_string", "my_enum"]

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

# Content is validated by Pydantic.BaseModel
model_instance = ExampleModel(
    my_string="hello",
    my_integer=1,
    my_float=1.23,
    my_bool=True,
    my_date=date.today(),
    my_datetime=datetime.now(timezone.utc),
    my_enum=ExampleEnum.FOO,
    my_repeatable_string=["hello", "world"],
    my_repeatable_integer=[1, 2],
    my_repeatable_float=[1.23, 4.56],
    my_repeatable_bool=[False, True],
    my_repeatable_date=[date.today(), date.today() + timedelta(days=1)],
    my_repeatable_datetime=[
        datetime.now(timezone.utc),
        datetime.now(timezone.utc) + timedelta(hours=12),
    ],
)
```

Repository usage:
```python
from pydantic_bigquery import BigQueryRepository

repository = BigQueryRepository("project_id", "dataset_id")
repository.create_dataset()
repository.create_table(ExampleModel)
repository.insert(model_instance)
```

Subclass **BigQueryRepository** to query table content and parse results back to ExampleModel. 🚀
