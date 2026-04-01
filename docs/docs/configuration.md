---
title: "Configuration"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Configuration

## Table properties

Iceberg tables support table properties to configure table behavior, like the default split size for readers.

### Read properties

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| read.split.target-size            | 134217728 (128 MB) | Target size when combining data input splits           |
| read.split.metadata-target-size   | 33554432 (32 MB)   | Target size when combining metadata input splits       |
| read.split.planning-lookback      | 10                 | Number of bins to consider when combining input splits |
| read.split.open-file-cost         | 4194304 (4 MB)     | The estimated cost to open a file, used as a minimum weight when combining splits. |
| read.parquet.vectorization.enabled| true               | Controls whether Parquet vectorized reads are used     |
| read.parquet.vectorization.batch-size| 5000            | The batch size for parquet vectorized reads            |
| read.orc.vectorization.enabled    | false              | Controls whether orc vectorized reads are used         |
| read.orc.vectorization.batch-size | 5000               | The batch size for orc vectorized reads                |

### Write properties

| Property                                            | Default                     | Description                                                                                                                                                                                                                                        |
|-----------------------------------------------------|-----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| write.format.default                                | parquet                     | Default file format for the table; parquet, avro, or orc                                                                                                                                                                                           |
| write.delete.format.default                         | data file format            | Default delete file format for the table; parquet, avro, or orc                                                                                                                                                                                    |
| write.parquet.row-group-size-bytes                  | 134217728 (128 MB)          | Parquet row group size                                                                                                                                                                                                                             |
| write.parquet.page-size-bytes                       | 1048576 (1 MB)              | Parquet page size                                                                                                                                                                                                                                  |
| write.parquet.page-version                          | v1                          | Parquet data page version: v1 (DataPage V1) or v2 (DataPage V2)                                                                                                                                                                                   |
| write.parquet.page-row-limit                        | 20000                       | Parquet page row limit                                                                                                                                                                                                                             |
| write.parquet.dict-size-bytes                       | 2097152 (2 MB)              | Parquet dictionary page size                                                                                                                                                                                                                       |
| write.parquet.compression-codec                     | zstd                        | Parquet compression codec: zstd, brotli, lz4, gzip, snappy, uncompressed                                                                                                                                                                           |
| write.parquet.compression-level                     | null                        | Parquet compression level                                                                                                                                                                                                                          |
| write.parquet.bloom-filter-enabled.column.col1      | (not set)                   | Hint to parquet to write a bloom filter for the column: 'col1'                                                                                                                                                                                     |
| write.parquet.bloom-filter-max-bytes                | 1048576 (1 MB)              | The maximum number of bytes for a bloom filter bitset                                                                                                                                                                                              |
| write.parquet.bloom-filter-fpp.column.col1          | 0.01                        | The false positive probability for a bloom filter applied to 'col1' (must > 0.0 and < 1.0)                                                                                                                                                         |
| write.parquet.bloom-filter-ndv.column.col1          | (not set)                   | The expected number of distinct values for a bloom filter applied to 'col1' (must > 0)                                                                                                                                                          |
| write.parquet.stats-enabled.column.col1             | (not set)                   | Controls whether to collect parquet column statistics for column 'col1'                                                                                                                                                                            |
| write.avro.compression-codec                        | gzip                        | Avro compression codec: gzip(deflate with 9 level), zstd, snappy, uncompressed                                                                                                                                                                     |
| write.avro.compression-level                        | null                        | Avro compression level                                                                                                                                                                                                                             |
| write.orc.stripe-size-bytes                         | 67108864 (64 MB)            | Define the default ORC stripe size, in bytes                                                                                                                                                                                                       |
| write.orc.block-size-bytes                          | 268435456 (256 MB)          | Define the default file system block size for ORC files                                                                                                                                                                                            |
| write.orc.compression-codec                         | zlib                        | ORC compression codec: zstd, lz4, lzo, zlib, snappy, none                                                                                                                                                                                          |
| write.orc.compression-strategy                      | speed                       | ORC compression strategy: speed, compression                                                                                                                                                                                                       |
| write.orc.bloom.filter.columns                      | (not set)                   | Comma separated list of column names for which a Bloom filter must be created                                                                                                                                                                      |
| write.orc.bloom.filter.fpp                          | 0.05                        | False positive probability for Bloom filter (must > 0.0 and < 1.0)                                                                                                                                                                                 |
| write.location-provider.impl                        | null                        | Optional custom implementation for LocationProvider                                                                                                                                                                                                |
| write.metadata.compression-codec                    | none                        | Metadata compression codec; none or gzip                                                                                                                                                                                                           |
| write.metadata.metrics.max-inferred-column-defaults | 100                         | Defines the maximum number of columns for which metrics are collected. Columns are included with a pre-order traversal of the schema: top level fields first; then all elements of the first nested struct; then the next nested struct and so on. |
| write.metadata.metrics.default                      | truncate(16)                | Default metrics mode for all columns in the table; none, counts, truncate(length), or full                                                                                                                                                         |
| write.metadata.metrics.column.col1                  | (not set)                   | Metrics mode for column 'col1' to allow per-column tuning; none, counts, truncate(length), or full                                                                                                                                                 |
| write.target-file-size-bytes                        | 536870912 (512 MB)          | Controls the size of files generated to target about this many bytes                                                                                                                                                                               |
| write.delete.target-file-size-bytes                 | 67108864 (64 MB)            | Controls the size of delete files generated to target about this many bytes                                                                                                                                                                        |
| write.distribution-mode                             |  not set, see engines for specific defaults, for example [Spark Writes](spark-writes.md#writing-distribution-modes) | Defines distribution of write data: __none__: don't shuffle rows; __hash__: hash distribute by partition key ; __range__: range distribute by partition key or sort key if table has an SortOrder                                                  |
| write.delete.distribution-mode                      | (not set)                   | Defines distribution of write delete data                                                                                                                                                                                                          |
| write.update.distribution-mode                      | (not set)                   | Defines distribution of write update data                                                                                                                                                                                                          |
| write.merge.distribution-mode                       | (not set)                   | Defines distribution of write merge data                                                                                                                                                                                                           |
| write.wap.enabled                                   | false                       | Enables write-audit-publish writes                                                                                                                                                                                                                 |
| write.summary.partition-limit                       | 0                           | Includes partition-level summary stats in snapshot summaries if the changed partition count is less than this limit                                                                                                                                |
| write.metadata.delete-after-commit.enabled          | false                       | Controls whether to delete the oldest **tracked** version metadata files after each table commit. See the [Remove old metadata files](maintenance.md#remove-old-metadata-files) section for additional details                                     |
| write.metadata.previous-versions-max                | 100                         | The max number of previous version metadata files to track                                                                                                                                                                                         |
| write.spark.fanout.enabled                          | false                       | Enables the fanout writer in Spark that does not require data to be clustered; uses more memory                                                                                                                                                    |
| write.object-storage.enabled                        | false                       | Enables the object storage location provider that adds a hash component to file paths                                                                                                                                                              |
| write.object-storage.partitioned-paths              | true                        | Includes the partition values in the file path                                                                                                                                                                                                     |
| write.data.path                                     | table location + /data      | Base location for data files                                                                                                                                                                                                                       |
| write.metadata.path                                 | table location + /metadata  | Base location for metadata files                                                                                                                                                                                                                   |
| write.delete.mode                                   | copy-on-write               | Mode used for delete commands: copy-on-write or merge-on-read (v2 and above)                                                                                                                                                                       |
| write.delete.isolation-level                        | serializable                | Isolation level for delete commands: serializable or snapshot                                                                                                                                                                                      |
| write.update.mode                                   | copy-on-write               | Mode used for update commands: copy-on-write or merge-on-read (v2 and above)                                                                                                                                                                       |
| write.update.isolation-level                        | serializable                | Isolation level for update commands: serializable or snapshot                                                                                                                                                                                      |
| write.merge.mode                                    | copy-on-write               | Mode used for merge commands: copy-on-write or merge-on-read (v2 and above)                                                                                                                                                                        |
| write.merge.isolation-level                         | serializable                | Isolation level for merge commands: serializable or snapshot                                                                                                                                                                                       |
| write.delete.granularity                            | partition                   | Controls the granularity of generated delete files: partition or file                                                                                                                                                                              |

### Encryption properties

| Property                          | Default            | Description                                                                           |
| --------------------------------- | ------------------ | ------------------------------------------------------------------------------------- |
| encryption.key-id                 | (not set)          | ID of the master key of the table                                                     |
| encryption.data-key-length        | 16 (bytes)         | Length of keys used for encryption of table files. Valid values are 16, 24, 32 bytes  |

See the [Encryption](encryption.md) document for additional details.

### Table behavior properties

| Property                           | Default          | Description                                                   |
| ---------------------------------- | ---------------- | ------------------------------------------------------------- |
| commit.retry.num-retries           | 4                | Number of times to retry a commit before failing              |
| commit.retry.min-wait-ms           | 100              | Minimum time in milliseconds to wait before retrying a commit |
| commit.retry.max-wait-ms           | 60000 (1 min)    | Maximum time in milliseconds to wait before retrying a commit |
| commit.retry.total-timeout-ms      | 1800000 (30 min) | Total retry timeout period in milliseconds for a commit |
| commit.status-check.num-retries    | 3                | Number of times to check whether a commit succeeded after a connection is lost before failing due to an unknown commit state |
| commit.status-check.min-wait-ms    | 1000 (1s)        | Minimum time in milliseconds to wait before retrying a status-check |
| commit.status-check.max-wait-ms    | 60000 (1 min)    | Maximum time in milliseconds to wait before retrying a status-check |
| commit.status-check.total-timeout-ms| 1800000 (30 min) | Total timeout period in which the commit status-check must succeed, in milliseconds |
| commit.manifest.target-size-bytes  | 8388608 (8 MB)   | Target size when merging manifest files                       |
| commit.manifest.min-count-to-merge | 100              | Minimum number of manifests to accumulate before merging      |
| commit.manifest-merge.enabled      | true             | Controls whether to automatically merge manifests on writes   |
| history.expire.max-snapshot-age-ms | 432000000 (5 days) | Default max age of snapshots to keep on the table and all of its branches while expiring snapshots |
| history.expire.min-snapshots-to-keep | 1                | Default min number of snapshots to keep on the table and all of its branches while expiring snapshots |
| history.expire.max-ref-age-ms      | `Long.MAX_VALUE` (forever) | For snapshot references except the `main` branch, default max age of snapshot references to keep while expiring snapshots. The `main` branch never expires. |
| gc.enabled                         | true             | Allows garbage collection operations such as expiring snapshots and removing orphan files |

### Reserved table properties
Reserved table properties are only used to control behaviors when creating or updating a table.
The value of these properties are not persisted as a part of the table metadata.

| Property       | Default  | Description                                                                                                                          |
| -------------- | -------- |--------------------------------------------------------------------------------------------------------------------------------------|
| format-version | 2        | Table's format version as defined in the [Spec](../../spec.md#format-versioning). Defaults to 2 since version 1.4.0. |

### Informational properties

Informational properties can be set to provide additional context about a table. They can be useful for documentation, discovery, and integration with external tools. They do not affect read/write behavior or query semantics.

| Property | Default    | Description                                                                                                         |
| -------- | ---------- | ------------------------------------------------------------------------------------------------------------------- |
| comment  | (not set)  | A table-level description that documents the business meaning and usage context. |

### Compatibility flags

| Property                                      | Default  | Description                                                   |
| --------------------------------------------- | -------- | ------------------------------------------------------------- |
| compatibility.snapshot-id-inheritance.enabled | false    | Enables committing snapshots without explicit snapshot IDs (always true if the format version is > 1) |
