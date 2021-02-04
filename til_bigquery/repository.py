from typing import List, Optional, Type, Union

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from .constants import BigQueryLocation
from .exceptions import BigQueryInsertError
from .model import BigQueryModel


class BigQueryRepository:
    DEFAULT_TIMEOUT = 30

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        client: Optional[bigquery.Client] = None,
    ):
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._client = client or bigquery.Client()

    def create_dataset(self, location: BigQueryLocation = BigQueryLocation.EU, exists_ok: bool = True) -> None:
        dataset = bigquery.Dataset(f"{self._project_id}.{self._dataset_id}")
        dataset.location = location.value

        self._client.create_dataset(dataset, exists_ok=exists_ok, timeout=self.DEFAULT_TIMEOUT)

    def get_dataset(self) -> Optional[bigquery.Dataset]:
        dataset = bigquery.Dataset(f"{self._project_id}.{self._dataset_id}")

        try:
            return self._client.get_dataset(dataset, timeout=self.DEFAULT_TIMEOUT)
        except NotFound:
            return None

    def create_table(self, model: Type[BigQueryModel], exists_ok: bool = True) -> None:
        schema = model.get_bigquery_schema()
        table = bigquery.Table(
            f"{self._project_id}.{self._dataset_id}.{model.__TABLE_NAME__}",
            schema=schema,
        )

        if model.__PARTITION_FIELD__:
            table.time_partitioning = bigquery.TimePartitioning(field=model.__PARTITION_FIELD__)
            table.require_partition_filter = True
        if model.__CLUSTERING_FIELDS__:
            table.clustering_fields = model.__CLUSTERING_FIELDS__

        self._client.create_table(table, exists_ok=exists_ok, timeout=self.DEFAULT_TIMEOUT)

    def get_table(self, model: Type[BigQueryModel]) -> Optional[bigquery.Table]:
        table = bigquery.Table(
            f"{self._project_id}.{self._dataset_id}.{model.__TABLE_NAME__}",
        )

        try:
            return self._client.get_table(table)
        except NotFound:
            return None

    def insert(self, data: Union[BigQueryModel, List[BigQueryModel]]) -> None:
        if not data:
            raise BigQueryInsertError("Nothing to insert!")

        if not isinstance(data, list):
            data = [data]

        table_id = f"{self._project_id}.{self._dataset_id}.{data[0].__TABLE_NAME__}"
        rows_to_insert = [x.bq_dict() for x in data]

        errors = self._client.insert_rows_json(table_id, rows_to_insert, timeout=self.DEFAULT_TIMEOUT)
        if errors:
            raise BigQueryInsertError("Streaming insert error!")
