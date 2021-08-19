class BigQueryInsertError(Exception):
    pass


class BigQueryBackendInsertError(BigQueryInsertError):
    pass


class BigQueryFetchError(Exception):
    pass
