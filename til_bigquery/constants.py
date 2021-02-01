from enum import Enum


class BigQueryLocation(Enum):
    EU = "EU"
    US = "US"


class BigQueryMode(Enum):
    NULLABLE = "NULLABLE"  # Optional
    REQUIRED = "REQUIRED"
    REPEATED = "REPEATED"  # List
