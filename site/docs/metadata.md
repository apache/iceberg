# Metadata Tables

This page describes the internal metadata tables maintained by Iceberg. Please refer to [definitions page](terms.md)
for more information on terms and definitions and the [specifications page](spec.md) for more information on Iceberg's
table specification. Complete metadata table schema can be found on the [Spark Queries page](spark-queries.md#metadata-table-schema). 

| Name                                              | Description |
| --------------------------------------------------| ------------|
| [`AllDataFilesTable`](#AllDataFilesTable)         | Contains rows representing all of the data files in the table. Each row will contain metadata as well as path information stored by the Iceberg. This differs from the `DataFilesTable` because it contains all files currently referenced by any existing Snapshot from this table rather than just the current one.
| [`AllEntriesTable`](#AllEntriesTable)             | Contains a table's manifest entries as rows, for both delete and data files. Please note that this table exposes internal details, like files that have been deleted. For a table of the live data files, please use `DataFilesTable`.
| [`AllManifestsTable`](#AllManifestsTable)         | Contains a table's valid manifest files as rows. A valid manifest file is referenced from any snapshot currently tracked by the table. This table may contain duplicate rows. 
| [`DataFilesTable`](#DataFilesTable)               | Contains a table's data files as rows.
| [`HistoryTable`](#HistoryTable)                   | Contains a table's history as rows. History is based on the table's snapshot log, which logs each update to the table's current snapshot.
| [`ManifestEntriesTable`](#ManifestEntriesTable)   | Contains a table's manifest entries as rows, for both delete and data files. Please note that this table exposes internal details, like files that have been deleted. For a table of the live data files, please use `DataFilesTable`.
| [`ManifestsTable`](#ManifestsTable)               | Contains a table's manifest files as rows.
| [`PartitionsTable`](#PartitionsTable)             | Contains a table's partitions as rows.
| [`SnapshotsTable`](#SnapshotsTable)               | Contains a table's known snapshots as rows. This does not include snapshots that have been expired using [`ExpireSnapshots`](https://iceberg.apache.org/javadoc/master/org/apache/iceberg/ExpireSnapshots.html).


## Table Schema

### <a id="AllDataFilesTable"></a> 1. `AllDataFilesTable`

| Column name           | Required  | Data type         | Description |
|-----------------------|-----------|-------------------|-------------|
| content               |           | int               | Contents of the file: 0=data, 1=position deletes, 2=equality deletes
| file_path             | ✔️        | string            | Location URI with FS scheme
| file_format           | ✔️        | string            | File format name: avro, orc, or parquet
| partition             | ✔️        | `struct<...>`     | Partition data tuple, schema based on the partition spec
| record_count          | ✔️        | long              | Number of records in the file
| file_size_in_bytes    | ✔️        | long              | Total file size in bytes
| column_sizes          | ️         | `map<int, long>`  | Map of column id to total size on disk
| value_counts          | ️         | `map<int, long>`  | Map of column id to total count, including null and NaN
| null_value_counts     | ️         | `map<int, long>`  | Map of column id to null value count
| nan_value_counts      |           | `map<int, long>`  | Map of column id to number of NaN values in the column
| lower_bounds          |           | `map<int, binary>`| Map of column id to lower bound
| upper_bounds          |           | `map<int, binary>`| Map of column id to upper bound
| key_metadata          |           | binary            | Encryption key metadata blob
| split_offsets         |           | `list<long>`      | Splittable offsets
| equality_ids          |           | `list<int>`       | Equality comparison field IDs
| sort_order_id         |           | int               | Sort order ID

### <a id="AllEntriesTable"></a> 2. `AllEntriesTable`

| Column name       | Required | Data type              | Description |
|-------------------|----------|------------------------|-------------|
| status            | ✔️       | int                    | Used to track additions and deletions: `0: EXISTING` `1: ADDED` `2: DELETED`
| snapshot_id       |          | long                   | Snapshot id where the file was added, or deleted if status is 2. Inherited when null.
| sequence_number   |          | long                   | Sequence number when the file was added. Inherited when null.
| data_file         | ✔️       | `data_file` `struct`   | File path, partition tuple, metrics, ...

### <a id="AllManifestsTable"></a> 3. `AllManifestsTable`

| Column name               | Required | Data type          | Description |
|---------------------------|----------|--------------------|-------------|
| path                      | ✔️       | string             | Location of the manifest file
| length                    | ✔️       | long               | Length of the manifest file
| partition_spec_id         |          | int                | ID of a partition spec used to write the manifest; must be listed in table metadata `partition-specs`
| added_snapshot_id         |          | long               | ID of the snapshot where the  manifest file was added
| added_data_files_count    |          | int                | Number of entries in the manifest that have status `ADDED` (1), when `null` this is assumed to be non-zero
| existing_data_files_count |          | int                | Number of entries in the manifest that have status `EXISTING` (0), when `null` this is assumed to be non-zero
| deleted_data_files_count  |          | int                | Number of entries in the manifest that have status `DELETED` (2), when `null` this is assumed to be non-zero
| partition_summaries       |          | `list<struct<...>>`| Partition summary information: contains null/nan, optional lower and upper bounds

### <a id="DataFilesTable"></a> 4. `DataFilesTable`

| Column name           | Required | Data type      | Description |
|-----------------------|-------|-------------------|-------------|
| content               |       | int               | Contents of the file: 0=data, 1=position deletes, 2=equality deletes
| file_path             | ✔️    | string            | Location URI with FS scheme
| file_format           | ✔️    | string            | File format name: avro, orc, or parquet
| partition             | ✔️    | `struct<...>`     | Partition data tuple, schema based on the partition spec
| record_count          | ✔️    | long              | Number of records in the file
| file_size_in_bytes    | ✔️    | long              | Total file size in bytes
| column_sizes          | ️     | `map<int, long>`  | Map of column id to total size on disk
| value_counts          | ️     | `map<int, long>`  | Map of column id to total count, including null and NaN
| null_value_counts     | ️     | `map<int, long>`  | Map of column id to null value count
| nan_value_counts      |       | `map<int, long>`  | Map of column id to number of NaN values in the column
| lower_bounds          |       | `map<int, binary>`| Map of column id to lower bound
| upper_bounds          |       | `map<int, binary>`| Map of column id to upper bound
| key_metadata          |       | binary            | Encryption key metadata blob
| split_offsets         |       | `list<long>`      | Splittable offsets
| equality_ids          |       | `list<int>`       | Equality comparison field IDs
| sort_order_id         |       | int               | Sort order ID

### <a id="HistoryTable"></a> 5. `HistoryTable`

| Column name           | Required  | Data type | Description |
|-----------------------|-----------|-----------|-------------|
| made_current_at       | ✔️        | timstampz | Timestamp (with timezone) when this snapshot was promoted to current, i.e. when the first writer to this snapshot committed.
| snapshot_id           | ✔️        | long      | A unique ID
| parent_id             |           | long      | ID of parent snapshot
| is_current_ancestor   | ✔️        | boolean   | True if if this snapshot is ancestor of current; false otherwise

### <a id="ManifestEntriesTable"></a> 6. `ManifestEntriesTable`

| Column name       | Required | Data type              | Description |
|-------------------|----------|------------------------|-------------|
| status            | ✔️       | int                    | Used to track additions and deletions: `0: EXISTING` `1: ADDED` `2: DELETED`
| snapshot_id       |          | long                   | Snapshot id where the file was added, or deleted if status is 2. Inherited when null.
| sequence_number   |          | long                   | Sequence number when the file was added. Inherited when null
| data_file         | ✔️       | `data_file` `struct`   | File path, partition tuple, metrics, ...

### <a id="ManifestsTable"></a> 7. `ManifestsTable`

| Column name               | Required | Data type          | Description |
|---------------------------|----------|--------------------|-------------|
| path                      | ✔️       | string             | Location of the manifest file
| length                    | ✔️       | long               | Length of the manifest file
| partition_spec_id         | ✔️       | int                | ID of a partition spec used to write the manifest; must be listed in table metadata `partition-specs`
| added_snapshot_id         | ✔️       | long               | ID of the snapshot where the  manifest file was added
| added_data_files_count    | ✔️       | int                | Number of entries in the manifest that have status `ADDED` (1), when `null` this is assumed to be non-zero
| existing_data_files_count | ✔️       | int                | Number of entries in the manifest that have status `EXISTING` (0), when `null` this is assumed to be non-zero
| deleted_data_files_count  | ✔️       | int                | Number of entries in the manifest that have status `DELETED` (2), when `null` this is assumed to be non-zero
| partition_summaries       | ✔️       | `list<struct<...>>`| Partition summary information: contains null/nan, optional lower and upper bounds

### <a id="PartitionsTable"></a> 8. `PartitionsTable`

| Column name   | Required | Data type      | Description |
|---------------|----------|----------------|-------------|
| partition     | ✔️       | `struct<...>`  | The table partition spec determined by partition type
| record_count  | ✔️       | long           | Aggregated number of records in this partition
| file_count    | ✔️       | int            | Total number of data files in this partition

### <a id="SnapshotsTable"></a> 9. `SnapshotsTable`

| Column name   | Required  | Data type                     | Description |
|---------------------------|-------------------------------|-------------|
| committed_at  | ✔️        | timestampz                    | Commit timestamp with timezone
| snapshot_id   | ✔️        | long                          | A unique ID
| parent_id     |           | long                          | The snapshot ID of the snapshot's parent. Omitted for any snapshot with no parent
| operation     |           | string                        | Used by some operations, like snapshot expiration, to skip processing certain snapshots. Possible `operation` values are: `append`, `replace`, `overwrite`, `delete`
| manifest_list |           | string                        | The location of a manifest list for this snapshot that tracks manifest files with additional meadata
| summary       |           | `map<string, string>`         | A string map that summarizes the snapshot changes |
