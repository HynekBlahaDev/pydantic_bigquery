from enum import Enum


class BigQueryLocation(str, Enum):
    EU = "EU"
    US = "US"


class BigQueryMode(str, Enum):
    NULLABLE = "NULLABLE"  # Optional
    REQUIRED = "REQUIRED"
    REPEATED = "REPEATED"  # List
