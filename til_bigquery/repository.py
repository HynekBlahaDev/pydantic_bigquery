from typing import Any, Dict, List, Optional, Type, Union

import structlog
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest, NotFound

from .constants import BigQueryLocation
from .exceptions import BigQueryInsertError
from .model import BigQueryModelBase

log = structlog.get_logger(__name__)


class BigQueryRepository:
    DEFAULT_TIMEOUT = 300
    MAX_INSERT_BATCH_SIZE = 10000

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        client: Optional[bigquery.Client] = None,
    ):
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._client = client or bigquery.Client(project_id)

    def create_dataset(
        self,
        location: BigQueryLocation = BigQueryLocation.EU,
        exists_ok: bool = True,
        description: Optional[str] = None,
        labels: Optional[Dict[str, Any]] = None,
        default_table_expiration_ms: Optional[int] = None,
    ) -> bigquery.Dataset:
        log.info(
            "repository.create_dataset.start",
            project_id=self._project_id,
            dataset_id=self._dataset_id,
            location=location,
            description=description,
            labels=labels,
            default_table_expiration_ms=default_table_expiration_ms,
        )

        dataset = bigquery.Dataset(f"{self._project_id}.{self._dataset_id}")
        dataset.location = location.value
        dataset.description = description
        dataset.labels = labels or dict()
        dataset.default_table_expiration_ms = default_table_expiration_ms

        return self._client.create_dataset(dataset, exists_ok=exists_ok, timeout=self.DEFAULT_TIMEOUT)

    def get_dataset(self) -> Optional[bigquery.Dataset]:
        log.info("repository.get_dataset.start", project_id=self._project_id, dataset_id=self._dataset_id)

        try:
            dataset = bigquery.Dataset(f"{self._project_id}.{self._dataset_id}")
            return self._client.get_dataset(dataset, timeout=self.DEFAULT_TIMEOUT)
        except NotFound:
            return None

    def create_table(
        self,
        model: Type[BigQueryModelBase],
        exists_ok: bool = True,
        description: Optional[str] = None,
        labels: Optional[Dict[str, Any]] = None,
    ) -> bigquery.Table:
        log.info(
            "repository.create_table.start",
            project_id=self._project_id,
            dataset_id=self._dataset_id,
            table_id=model.__TABLE_NAME__,
            description=description,
            labels=labels,
        )

        schema = model.get_bigquery_schema()
        table = bigquery.Table(
            f"{self._project_id}.{self._dataset_id}.{model.__TABLE_NAME__}",
            schema,
        )
        table.description = description
        table.labels = labels or dict()

        if model.__PARTITION_FIELD__:
            table.time_partitioning = bigquery.TimePartitioning(field=model.__PARTITION_FIELD__)
            table.require_partition_filter = True
        if model.__CLUSTERING_FIELDS__:
            table.clustering_fields = model.__CLUSTERING_FIELDS__

        return self._client.create_table(table, exists_ok=exists_ok, timeout=self.DEFAULT_TIMEOUT)

    def get_table(self, model: Type[BigQueryModelBase]) -> Optional[bigquery.Table]:
        log.info(
            "repository.get_table.start",
            project_id=self._project_id,
            dataset_id=self._dataset_id,
            table_id=model.__TABLE_NAME__,
        )

        try:
            table = bigquery.Table(f"{self._project_id}.{self._dataset_id}.{model.__TABLE_NAME__}")
            return self._client.get_table(table)
        except NotFound:
            return None

    def insert(self, data: Union[BigQueryModelBase, List[BigQueryModelBase]]) -> None:
        # Empty list
        if not data:
            return

        # Single item
        if not isinstance(data, list):
            data = [data]

        log.info(
            "repository.insert.start",
            project_id=self._project_id,
            dataset_id=self._dataset_id,
            table_id=data[0].__TABLE_NAME__,
            count=len(data),
        )

        # Create batches of maximum MAX_INSERT_BATCH_SIZE length
        data_batches = [
            data[i : i + self.MAX_INSERT_BATCH_SIZE] for i in range(0, len(data), self.MAX_INSERT_BATCH_SIZE)
        ]
        for data_batch in data_batches:
            rows_to_insert = [x.bq_dict() for x in data_batch]

            try:
                errors = self._client.insert_rows_json(
                    f"{self._project_id}.{self._dataset_id}.{data[0].__TABLE_NAME__}",
                    rows_to_insert,
                    timeout=self.DEFAULT_TIMEOUT,
                )
                if errors:
                    log.error("repository.insert.error", response=errors)
                    raise BigQueryInsertError("Streaming insert error!")
            except BadRequest as e:
                # This happens when payload is significantly over the limit and the server side of BQ trims it.
                # https://github.com/googleapis/google-cloud-go/issues/2855#issuecomment-702993221
                if (
                    "Your client has issued a malformed or illegal request." in e.response.text
                    or "Request payload size exceeds the limit: 10485760 bytes." in e.response.text
                ):
                    log.warning("repository.insert.too_large_body", response=e.response.text)

                    # Use bisect to reduce payload size
                    half_size = len(data_batch) // 2

                    # Recursive end condition
                    if half_size == 0:
                        log.exception("repository.insert.too_large_body_exception")
                        raise BigQueryInsertError("Row is too large") from e

                    # Recursive call
                    data_batch_1, data_batch_2 = data_batch[:half_size], data_batch[half_size:]
                    self.insert(data_batch_1)
                    self.insert(data_batch_2)
