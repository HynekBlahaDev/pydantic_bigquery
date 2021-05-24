from datetime import date, datetime, timedelta, timezone
from typing import List, Optional
from uuid import UUID

import pytest
from faker import Faker
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from mock import create_autospec

from til_bigquery import BigQueryFetchError, BigQueryLocation, BigQueryModel, BigQueryModelBase, BigQueryRepository

from .test_model import ExampleEnum, ExampleModel

TEST_PROJECT_ID = "platform-local"
TEST_DATASET_ID = "test_package_til_bigquery"


class ExampleBigQueryRepository(BigQueryRepository):
    def get_example(self, insert_id: UUID) -> Optional[ExampleModel]:
        query = f"""
            SELECT *
            FROM `{self._project_id}.{self._dataset_id}.{ExampleModel.__TABLE_NAME__}`
            WHERE insert_id = @insert_id AND inserted_at < @inserted_at
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("insert_id", "STRING", str(insert_id)),
                bigquery.ScalarQueryParameter("inserted_at", "TIMESTAMP", datetime.now(timezone.utc)),
            ]
        )

        query_job: bigquery.QueryJob = self._client.query(query, job_config)
        if query_job.errors:
            raise BigQueryFetchError

        bq_row: bigquery.table.Row
        for bq_row in query_job.result(max_results=1):
            bq_dict = dict(bq_row.items())
            return ExampleModel(**bq_dict)

        return None

    def update_example(self, model: ExampleModel) -> Optional[ExampleModel]:
        query = f"""
            UPDATE `{self._project_id}.{self._dataset_id}.{model.__TABLE_NAME__}`
            SET my_string = @my_string
            WHERE insert_id = @insert_id AND inserted_at = @inserted_at
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("insert_id", "STRING", str(model.insert_id)),
                bigquery.ScalarQueryParameter("inserted_at", "TIMESTAMP", model.inserted_at),
                bigquery.ScalarQueryParameter("my_string", "STRING", model.my_string),
            ]
        )

        query_job: bigquery.QueryJob = self._client.query(query, job_config)
        if query_job.errors:
            raise BigQueryFetchError

        bq_row: bigquery.table.Row
        for bq_row in query_job.result(max_results=1):
            bq_dict = dict(bq_row.items())
            return ExampleModel(**bq_dict)

        return None


@pytest.fixture(scope="session", name="example_table_id")
def fixture_example_table_id() -> str:
    return "_".join(Faker().words(2))


# https://stackoverflow.com/a/57015304/14450231
@pytest.fixture(scope="session", name="bq_repository")
def fixture_bq_repository() -> BigQueryRepository:
    return BigQueryRepository(project_id=TEST_PROJECT_ID, dataset_id=TEST_DATASET_ID)


@pytest.fixture(scope="session", name="example_bq_repository")
def fixture_example_bq_repository() -> ExampleBigQueryRepository:
    return ExampleBigQueryRepository(project_id=TEST_PROJECT_ID, dataset_id=TEST_DATASET_ID)


@pytest.fixture(scope="session", name="example_model")
def fixture_example_model(example_table_id: str) -> ExampleModel:
    ExampleModel.__TABLE_NAME__ = example_table_id
    model = ExampleModel(
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
    return model


def test_create_dataset(bq_repository: BigQueryRepository) -> None:
    result = bq_repository.create_dataset(
        location=BigQueryLocation.US,
        description="TestDescription",
        labels=dict(name="value"),
        default_table_expiration_ms=3600000,  # minimum
    )

    assert result.location == BigQueryLocation.US
    assert result.description == "TestDescription"
    assert result.labels == dict(name="value")
    assert result.default_table_expiration_ms == 3600000


def test_get_dataset(bq_repository: BigQueryRepository) -> None:
    dataset = bq_repository.get_dataset()
    assert dataset


def test_get_dataset_not_found() -> None:
    bq_repository = BigQueryRepository(project_id=TEST_PROJECT_ID, dataset_id="this-dataset-doesnt-exist")
    dataset = bq_repository.get_dataset()
    assert dataset is None


def test_create_table(bq_repository: BigQueryRepository, example_table_id: str) -> None:
    ExampleModel.__TABLE_NAME__ = example_table_id
    result = bq_repository.create_table(
        ExampleModel,
        description="TestDescription2",
        labels=dict(name="value2"),
    )

    assert result.description == "TestDescription2"
    assert result.labels == dict(name="value2")


def test_get_table(bq_repository: BigQueryRepository, example_table_id: str) -> None:
    ExampleModel.__TABLE_NAME__ = example_table_id
    table = bq_repository.get_table(ExampleModel)
    assert table


def test_get_table_not_found(bq_repository: BigQueryRepository) -> None:
    mock = create_autospec(ExampleModel)
    mock.__TABLE_NAME__ = "this-table-doesnt-exist"
    table = bq_repository.get_table(mock)
    assert table is None


def test_insert_empty(bq_repository: BigQueryRepository) -> None:
    bq_repository.insert([])


def test_insert(bq_repository: BigQueryRepository, example_model: ExampleModel) -> None:
    bq_repository.insert(example_model)


def test_get(example_bq_repository: ExampleBigQueryRepository, example_model: ExampleModel) -> None:
    result = example_bq_repository.get_example(example_model.insert_id)
    assert result == example_model


def test_steaming_update_impossible(
    example_bq_repository: ExampleBigQueryRepository, example_model: ExampleModel
) -> None:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-manipulation-language#limitations
    #
    # Rows that were written to a table recently by using streaming (the tabledata.insertall method) cannot be modified
    # with UPDATE, DELETE, or MERGE statements. Recent writes are typically those that occur within the last 30 minutes.
    # Note that all other rows in the table remain modifiable by using UPDATE, DELETE, or MERGE statements.

    original = example_bq_repository.get_example(example_model.insert_id)
    assert original is not None
    assert original.my_string == "hello"

    original.my_string = "bonjour"
    with pytest.raises(BadRequest):
        example_bq_repository.update_example(original)


def test_insert_over_max_size(bq_repository: BigQueryRepository):
    class TinyBigQueryModel(BigQueryModel):
        __TABLE_NAME__: str = "tiny_model"

        integer: int

    bq_repository.create_table(TinyBigQueryModel)
    data: List[BigQueryModelBase] = [
        TinyBigQueryModel(integer=i) for i in range(bq_repository.MAX_INSERT_BATCH_SIZE + 1)
    ]
    bq_repository.insert(data)
