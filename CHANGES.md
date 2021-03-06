## 2021-12-22 - v0.3.2
 - Add validation check for project_id, dataset_id

## 2021-12-09 - v0.3.1
 - Unpin structlog dependency

## 2021-11-24 - v0.3.0
 - Model: Add support for nested fields (records)

## 2021-08-19 - v0.2.8
 - Insert: Retry on temporary backendError

## 2021-06-25 - v0.2.7
 - Insert: Log only the first error, full errors (large) break logs

## 2021-06-21 - v0.2.6
 - Insert: Retry with bisect when request body is too large (2)

## 2021-05-25 - v0.2.5
 - Insert: Retry with bisect when request body is too large

## 2021-05-24 - v0.2.4
 - Insert: Return when list is empty

## 2021-04-22 - v0.2.3
 - Add BigQueryModelLegacy: represents mbq models

## 2021-04-21 - v0.2.1
 - create_dataset(): Add optional description, labels, default_table_expiration_ms
 - create_table(): Add optional description, labels

## 2021-02-24 - v0.2.0
###### Update packages

## 2021-02-16 - v0.1.3
###### Batch inserts by 10k items (max insert limit)

## 2021-02-05 - v0.1.2
###### Requirements: Add structlog

## 2021-02-05 - v0.1.1
###### BigQueryModel: Add common fields

 - insert_id
 - inserted_at

## 2021-02-01 - v0.1.0
###### Beta version
