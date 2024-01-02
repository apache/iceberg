---
title: "Configuration"
url: configuration
aliases:
    - "tables/configuration"
menu:
    main:
        parent: Tables
        identifier: tables_configuration
        weight: 0
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

| Property                                             | Default                     | Description                                                                                                                                                                                       |
|------------------------------------------------------|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| write.format.default                                 | parquet                     | Default file format for the table; parquet, avro, or orc                                                                                                                                          |
| write.delete.format.default                          | data file format            | Default delete file format for the table; parquet, avro, or orc                                                                                                                                   |
| write.parquet.row-group-size-bytes                   | 134217728 (128 MB)          | Parquet row group size                                                                                                                                                                            |
| write.parquet.page-size-bytes                        | 1048576 (1 MB)              | Parquet page size                                                                                                                                                                                 |
| write.parquet.page-row-limit                         | 20000                       | Parquet page row limit                                                                                                                                                                            |
| write.parquet.dict-size-bytes                        | 2097152 (2 MB)              | Parquet dictionary page size                                                                                                                                                                      |
| write.parquet.compression-codec                      | zstd                        | Parquet compression codec: zstd, brotli, lz4, gzip, snappy, uncompressed                                                                                                                          |
| write.parquet.compression-level                      | null                        | Parquet compression level                                                                                                                                                                         |
| write.parquet.bloom-filter-enabled.column.col1       | (not set)                   | Hint to parquet to write a bloom filter for the column: col1                                                                                                                                      |
| write.parquet.bloom-filter-max-bytes                 | 1048576 (1 MB)              | The maximum number of bytes for a bloom filter bitset                                                                                                                                             |
| write.avro.compression-codec                         | gzip                        | Avro compression codec: gzip(deflate with 9 level), zstd, snappy, uncompressed                                                                                                                    |
| write.avro.compression-level                         | null                        | Avro compression level                                                                                                                                                                            |
| write.orc.stripe-size-bytes                          | 67108864 (64 MB)            | Define the default ORC stripe size, in bytes                                                                                                                                                      |
| write.orc.block-size-bytes                           | 268435456 (256 MB)          | Define the default file system block size for ORC files                                                                                                                                           |
| write.orc.compression-codec                          | zlib                        | ORC compression codec: zstd, lz4, lzo, zlib, snappy, none                                                                                                                                         |
| write.orc.compression-strategy                       | speed                       | ORC compression strategy: speed, compression                                                                                                                                                      |
| write.orc.bloom.filter.columns                       | (not set)                   | Comma separated list of column names for which a Bloom filter must be created                                                                                                                     |
| write.orc.bloom.filter.fpp                           | 0.05                        | False positive probability for Bloom filter (must > 0.0 and < 1.0)                                                                                                                                |
| write.location-provider.impl                         | null                        | Optional custom implementation for LocationProvider                                                                                                                                               |
| write.metadata.compression-codec                     | none                        | Metadata compression codec; none or gzip                                                                                                                                                          |
| write.metadata.metrics.max-inferred-column-defaults  | 100                         | Defines the maximum number of columns for which metrics are collected                                                                                                                             |
| write.metadata.metrics.default                       | truncate(16)                | Default metrics mode for all columns in the table; none, counts, truncate(length), or full                                                                                                        |
| write.metadata.metrics.column.col1                   | (not set)                   | Metrics mode for column 'col1' to allow per-column tuning; none, counts, truncate(length), or full                                                                                                |
| write.target-file-size-bytes                         | 536870912 (512 MB)          | Controls the size of files generated to target about this many bytes                                                                                                                              |
| write.delete.target-file-size-bytes                  | 67108864 (64 MB)            | Controls the size of delete files generated to target about this many bytes                                                                                                                       |
| write.distribution-mode                              | none                        | Defines distribution of write data: __none__: don't shuffle rows; __hash__: hash distribute by partition key ; __range__: range distribute by partition key or sort key if table has an SortOrder |
| write.delete.distribution-mode                       | hash                        | Defines distribution of write delete data                                                                                                                                                         |
| write.update.distribution-mode                       | hash                        | Defines distribution of write update data                                                                                                                                                         |
| write.merge.distribution-mode                        | none                        | Defines distribution of write merge data                                                                                                                                                          |
| write.wap.enabled                                    | false                       | Enables write-audit-publish writes                                                                                                                                                                |
| write.summary.partition-limit                        | 0                           | Includes partition-level summary stats in snapshot summaries if the changed partition count is less than this limit                                                                               |
| write.metadata.delete-after-commit.enabled           | false                       | Controls whether to delete the oldest **tracked** version metadata files after commit                                                                                                             |
| write.metadata.previous-versions-max                 | 100                         | The max number of previous version metadata files to keep before deleting after commit                                                                                                            |
| write.spark.fanout.enabled                           | false                       | Enables the fanout writer in Spark that does not require data to be clustered; uses more memory                                                                                                   |
| write.object-storage.enabled                         | false                       | Enables the object storage location provider that adds a hash component to file paths                                                                                                             |
| write.data.path                                      | table location + /data      | Base location for data files                                                                                                                                                                      |
| write.metadata.path                                  | table location + /metadata  | Base location for metadata files                                                                                                                                                                  |
| write.delete.mode                                    | copy-on-write               | Mode used for delete commands: copy-on-write or merge-on-read (v2 only)                                                                                                                           |
| write.delete.isolation-level                         | serializable                | Isolation level for delete commands: serializable or snapshot                                                                                                                                     |
| write.update.mode                                    | copy-on-write               | Mode used for update commands: copy-on-write or merge-on-read (v2 only)                                                                                                                           |
| write.update.isolation-level                         | serializable                | Isolation level for update commands: serializable or snapshot                                                                                                                                     |
| write.merge.mode                                     | copy-on-write               | Mode used for merge commands: copy-on-write or merge-on-read (v2 only)                                                                                                                            |
| write.merge.isolation-level                          | serializable                | Isolation level for merge commands: serializable or snapshot                                                                                                                                      |

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

### Reserved table properties
Reserved table properties are only used to control behaviors when creating or updating a table.
The value of these properties are not persisted as a part of the table metadata.

| Property       | Default  | Description                                                   |
| -------------- | -------- | ------------------------------------------------------------- |
| format-version | 2        | Table's format version (can be 1 or 2) as defined in the [Spec](../../../spec/#format-versioning). Defaults to 2 since version 1.4.0. |

### Compatibility flags

| Property                                      | Default  | Description                                                   |
| --------------------------------------------- | -------- | ------------------------------------------------------------- |
| compatibility.snapshot-id-inheritance.enabled | false    | Enables committing snapshots without explicit snapshot IDs (always true if the format version is > 1) |

## Catalog properties

Iceberg catalogs support using catalog properties to configure catalog behaviors. Here is a list of commonly used catalog properties:

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| catalog-impl                      | null               | a custom `Catalog` implementation to use by an engine  |
| io-impl                           | null               | a custom `FileIO` implementation to use in a catalog   |
| warehouse                         | null               | the root path of the data warehouse                    |
| uri                               | null               | a URI string, such as Hive metastore URI               |
| clients                           | 2                  | client pool size                                       |
| cache-enabled                     | true               | Whether to cache catalog entries |
| cache.expiration-interval-ms      | 30000              | How long catalog entries are locally cached, in milliseconds; 0 disables caching, negative values disable expiration |
| metrics-reporter-impl | org.apache.iceberg.metrics.LoggingMetricsReporter | Custom `MetricsReporter` implementation to use in a catalog. See the [Metrics reporting](metrics-reporting) section for additional details |

`HadoopCatalog` and `HiveCatalog` can access the properties in their constructors.
Any other custom catalog can access the properties by implementing `Catalog.initialize(catalogName, catalogProperties)`.
The properties can be manually constructed or passed in from a compute engine like Spark or Flink.
Spark uses its session properties as catalog properties, see more details in the [Spark configuration](../spark-configuration#catalog-configuration) section.
Flink passes in catalog properties through `CREATE CATALOG` statement, see more details in the [Flink](../flink/#creating-catalogs-and-using-catalogs) section.

### Lock catalog properties

Here are the catalog properties related to locking. They are used by some catalog implementations to control the locking behavior during commits.

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| lock-impl                         | null               | a custom implementation of the lock manager, the actual interface depends on the catalog used  |
| lock.table                        | null               | an auxiliary table for locking, such as in [AWS DynamoDB lock manager](../aws/#dynamodb-for-commit-locking)  |
| lock.acquire-interval-ms          | 5000 (5 s)         | the interval to wait between each attempt to acquire a lock  |
| lock.acquire-timeout-ms           | 180000 (3 min)     | the maximum time to try acquiring a lock               |
| lock.heartbeat-interval-ms        | 3000 (3 s)         | the interval to wait between each heartbeat after acquiring a lock  |
| lock.heartbeat-timeout-ms         | 15000 (15 s)       | the maximum time without a heartbeat to consider a lock expired  |


## Hadoop configuration

The following properties from the Hadoop configuration are used by the Hive Metastore connector.
The HMS table locking is a 2-step process:
1. Lock Creation: Create lock in HMS and queue for acquisition
2. Lock Check: Check if lock successfully acquired

| Property                                  | Default         | Description                                                                  |
|-------------------------------------------|-----------------|------------------------------------------------------------------------------|
| iceberg.hive.client-pool-size             | 5               | The size of the Hive client pool when tracking tables in HMS                 |
| iceberg.hive.lock-creation-timeout-ms     | 180000 (3 min)  | Maximum time in milliseconds to create a lock in the HMS                     |
| iceberg.hive.lock-creation-min-wait-ms    | 50              | Minimum time in milliseconds between retries of creating the lock in the HMS |
| iceberg.hive.lock-creation-max-wait-ms    | 5000            | Maximum time in milliseconds between retries of creating the lock in the HMS |
| iceberg.hive.lock-timeout-ms              | 180000 (3 min)  | Maximum time in milliseconds to acquire a lock                               |
| iceberg.hive.lock-check-min-wait-ms       | 50              | Minimum time in milliseconds between checking the acquisition of the lock    |
| iceberg.hive.lock-check-max-wait-ms       | 5000            | Maximum time in milliseconds between checking the acquisition of the lock    |
| iceberg.hive.lock-heartbeat-interval-ms   | 240000 (4 min)  | The heartbeat interval for the HMS locks.                                    |
| iceberg.hive.metadata-refresh-max-retries | 2               | Maximum number of retries when the metadata file is missing                  |
| iceberg.hive.table-level-lock-evict-ms    | 600000 (10 min) | The timeout for the JVM table lock is                                        |
| iceberg.engine.hive.lock-enabled          | true            | Use HMS locks to ensure atomicity of commits                                 |

Note: `iceberg.hive.lock-check-max-wait-ms` and `iceberg.hive.lock-heartbeat-interval-ms` should be less than the [transaction timeout](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-hive.txn.timeout) 
of the Hive Metastore (`hive.txn.timeout` or `metastore.txn.timeout` in the newer versions). Otherwise, the heartbeats on the lock (which happens during the lock checks) would end up expiring in the 
Hive Metastore before the lock is retried from Iceberg.

Warn: Setting `iceberg.engine.hive.lock-enabled`=`false` will cause HiveCatalog to commit to tables without using Hive locks.
This should only be set to `false` if all following conditions are met:
 - [HIVE-26882](https://issues.apache.org/jira/browse/HIVE-26882)
is available on the Hive Metastore server
 - All other HiveCatalogs committing to tables that this HiveCatalog commits to are also on Iceberg 1.3 or later
 - All other HiveCatalogs committing to tables that this HiveCatalog commits to have also disabled Hive locks on commit.

**Failing to ensure these conditions risks corrupting the table.**

Even with `iceberg.engine.hive.lock-enabled` set to `false`, a HiveCatalog can still use locks for individual tables by setting the table property `engine.hive.lock-enabled`=`true`.
This is useful in the case where other HiveCatalogs cannot be upgraded and set to commit without using Hive locks.

