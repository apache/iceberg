---
title: "Configuration"
url: configuration
menu: main
weight: 1002
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

## Spark Catalog Properties

In Spark, catalog properties can be set either through the `--conf` argument when using the cli or in the `spark-defaults.conf` file.

{{% codetabs "SparkCatalogProperties" %}}
{{% addtab "Spark-Shell" "spark-init" "cli" %}}
{{% addtab "spark-defaults.conf" "spark-init" "spark-defaults" %}}
{{% tabcontent "cli" %}}
```sh
spark-submit --conf spark.sql.catalog.<catalogName>.<property>=<value>
```
{{% /tabcontent %}}
{{% tabcontent "spark-defaults" %}}
```sh
spark.sql.catalog.<catalogName>.<property>=<value>
```
{{% /tabcontent %}}
{{% /codetabs %}}

| Property                                                          | Values                                       | Description                                                                                                                                                                                                                              |
|-------------------------------------------------------------------|----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.sql.catalog.spark_catalog                                   | org.apache.iceberg.spark.SparkSessionCatalog | Adds support for Iceberg tables to Spark’s built-in catalog                                                                                                                                                                              |
| spark.sql.catalog._catalog-name_.**type**                         | `hive` or `hadoop`                           | The underlying Iceberg catalog implementation, `HiveCatalog`, `HadoopCatalog` or left unset if using a custom catalog.                                                                                                                   |
| spark.sql.catalog._catalog-name_.**catalog-impl**                 |                                              | The underlying Iceberg catalog implementation.                                                                                                                                                                                           |
| spark.sql.catalog._catalog-name_.**default-namespace**            | default                                      | The default current namespace for the catalog                                                                                                                                                                                            |
| spark.sql.catalog._catalog-name_.**uri**                          | thrift://host:port                           | Metastore connect URI; default from `hive-site.xml`                                                                                                                                                                                      |
| spark.sql.catalog._catalog-name_.**warehouse**                    | hdfs://nn:8020/warehouse/path                | Base path for the warehouse directory                                                                                                                                                                                                    |
| spark.sql.catalog._catalog-name_.**cache-enabled**                | `true` or `false`                            | Whether to enable catalog cache, default value is `true`                                                                                                                                                                                 |
| spark.sql.catalog._catalog-name_.**cache.expiration-interval-ms** | `30000` (30 seconds)                         | Duration after which cached catalog entries are expired; Only effective if `cache-enabled` is `true`. `-1` disables cache expiration and `0` disables caching entirely, irrespective of `cache-enabled`. Default is `30000` (30 seconds) |
| spark.sql.catalog._catalog-name_.**s3.delete-enabled**            | `true` or `false`                            | When set to `false`, the objects are not hard-deleted from S3                                                                                                                                                                            |
| spark.sql.catalog._catalog-name_.**s3.delete.tags**._tag_name_    | null                                         | Objects are tagged with the configured key-value pairs before deletion. Users can configure tag-based object lifecycle policy at bucket level to transition objects to different tiers.                                                  |
| spark.sql.catalog._catalog-name_.**s3.delete.num-threads**        | null                                         | Number of threads to be used for adding delete tags to the S3 objects                                                                                                                                                                    |

## Spark Table Properties

In Spark, table properties can be set through `ALTER TABLE` SQL commands.

{{% codetabs "SparkTableProperties" %}}
{{% addtab "Spark-SQL" "spark-queries" "spark-sql" %}}
{{% tabcontent "spark-sql" %}}
```sql
ALTER TABLE <catalog>.<database>.<table> SET TBLPROPERTIES (
    '<property>'='<value>'
)
```
{{% /tabcontent %}}
{{% /codetabs %}}

See the [Table Properties](#table-properties) section to see a list of all available table properties.

{{< hint info >}}To enable Iceberg's Spark SQL extensions, set `spark.sql.extensions` to `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`
in your Spark conf.{{< /hint >}}

## Spark DataFrame Read Options

Read options can be set at runtime when using the Spark DataFrame API.

{{% codetabs "SparkDataframeRead" %}}
{{% addtab "Scala" "spark-queries" "spark-sql" %}}
{{% addtab "PySpark" "spark-queries" "pyspark" %}}
{{% tabcontent "spark-sql" %}}
```scala
val df = spark.read
    .option("<property>", "<value>")
    .format("iceberg")
    .load("catalog.database.table")
```
{{% /tabcontent %}}
{{% tabcontent "pyspark" %}}
```python
df = spark.read.option("<property>", "<value>").format("iceberg").load("catalog.database.table")
```
{{% /tabcontent %}}
{{% /codetabs %}}

| Spark option    | Default               | Description                                                                               |
| --------------- | --------------------- | ----------------------------------------------------------------------------------------- |
| snapshot-id     | (latest)              | Snapshot ID of the table snapshot to read                                                 |
| as-of-timestamp | (latest)              | A timestamp in milliseconds; the snapshot used will be the snapshot current at this time. |
| split-size      | As per table property | Overrides this table's read.split.target-size and read.split.metadata-target-size         |
| lookback        | As per table property | Overrides this table's read.split.planning-lookback                                       |
| file-open-cost  | As per table property | Overrides this table's read.split.open-file-cost                                          |
| vectorization-enabled  | As per table property | Overrides this table's read.parquet.vectorization.enabled                                          |
| batch-size  | As per table property | Overrides this table's read.parquet.vectorization.batch-size                                          |
| stream-from-timestamp | (none) | A timestamp in milliseconds to stream from; if before the oldest known ancestor snapshot, the oldest will be used |

## Spark DataFrame Write Options

Write options can be set at runtime when using the Spark DataFrame API.

{{% codetabs "SparkDataframeWrite" %}}
{{% addtab "Scala" "spark-queries" "spark-sql" %}}
{{% addtab "PySpark" "spark-queries" "pyspark" %}}
{{% tabcontent "spark-sql" %}}
```scala
df.write
    .option("<property>", "<value>")
    .format("iceberg")
    .mode("append")
    .save("catalog.database.table")
```
{{% /tabcontent %}}
{{% tabcontent "pyspark" %}}
```python
df.write.option("<property>", "<value>").format("iceberg").mode("append").save("catalog.database.table")
```
{{% /tabcontent %}}
{{% /codetabs %}}

| Spark option           | Default                    | Description                                                  |
| ---------------------- | -------------------------- | ------------------------------------------------------------ |
| write-format           | Table write.format.default | File format to use for this write operation; parquet, avro, or orc |
| target-file-size-bytes | As per table property      | Overrides this table's write.target-file-size-bytes          |
| check-nullability      | true                       | Sets the nullable check on fields                            |
| snapshot-property._custom-key_    | null            | Adds an entry with custom-key and corresponding value in the snapshot summary  |
| fanout-enabled       | false        | Overrides this table's write.spark.fanout.enabled  |
| check-ordering       | true        | Checks if input schema and table schema are same  |
| isolation-level | null | Desired isolation level for Dataframe overwrite operations.  `null` => no checks (for idempotent writes), `serializable` => check for concurrent inserts or deletes in destination partitions, `snapshot` => checks for concurrent deletes in destination partitions. |
| validate-from-snapshot-id | null | If isolation level is set, id of base snapshot from which to check concurrent write conflicts into a table. Should be the snapshot before any reads from the table. Can be obtained via [Table API](../../api#table-metadata) or [Snapshots table](../spark-queries#snapshots). If null, the table's oldest known snapshot is used. |

## Flink Catalog Properties

In Flink, catalog properties can be set either in the `WITH` clause in Flink-SQL or by passing a property map to a catalog loader.

{{% codetabs "FlinkCatalogProperties" %}}
{{% addtab "SQL" "general-languages" "sql" %}}
{{% addtab "Java" "general-languages" "java" %}}
{{% tabcontent "sql" %}}
```sql
CREATE CATALOG <catalogName> WITH (
  'type'='iceberg',
  '<property>'='<value>'
);
```
{{% /tabcontent %}}
{{% tabcontent "java" %}}
```java
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;

Configuration conf = new Configuration();

Map<String, String> catalogProperties = new HashMap<>();
catalogProperties.put("<property>", "<value>");

CatalogLoader catalogLoader =
    CatalogLoader.custom(
        "<catalogName>",
        catalogProperties,
        conf,
        "<catalog-impl>");
Catalog catalog = catalogLoader.loadCatalog();
```
{{% /tabcontent %}}
{{% /codetabs %}}

| Property         | Default | Description                                                                                                                                                                                                                                               |
|------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type             | iceberg | This is required and must be set to `iceberg`                                                                                                                                                                                                             |
| catalog-type     | null    | This can be set to `hive` or `hadoop` and should be left unset for custom catalog implementations (use `catalog-impl` instead)                                                                                                                            |
| catalog-impl     | null    | The fully-qualified class name custom catalog implementation, must be set if catalog-type is unset.                                                                                                                                                       |
| property-version | null    | Version number to describe the property version. This property can be used for backwards compatibility in case the property format changes. The current property version is 1.                                                                            |
| cache-enabled    | true    | Whether to enable catalog cache.                                                                                                                                                                                                                          |
| uri              | null    | The Hive metastore's thrift URI. Required if using a hive catalog.                                                                                                                                                                                        |
| clients          | null    | The Hive metastore client pool size, default value is 2.                                                                                                                                                                                                  |
| warehouse        | true    | The Hive warehouse location. This should be specified if the `hive-conf-dir` is not set to a location containing a `hive-site.xml` or a `hive-site.xml` has not been added to the classpath.                                                              |
| hive-conf-dir    | null    | Path to a directory containing a `hive-site.xml` configuration file which will be used to provide custom Hive configuration values. This value takes precedence over the value of `hive.metastore.warehouse.dir` in a `hive-site.xml` configuration file. |


## Flink Table Properties

In Flink, table properties can be set through `ALTER TABLE` SQL commands.

{{% codetabs "FlinkTableProperties" %}}
{{% addtab "SQL" "general-languages" "sql" %}}
{{% tabcontent "sql" %}}
```sql
ALTER TABLE <catalog>.<database>.<table> SET (
    '<property>'='<value>'
)
```
{{% /tabcontent %}}
{{% /codetabs %}}

See the [Table Properties](#table-properties) section to see a list of all available table properties.

## Hive Catalog Properties

In Hive, catalog properties can be set using the `SET` keyword.

{{% codetabs "HiveCatalogProperties" %}}
{{% addtab "SQL" "general-languages" "sql" %}}
{{% tabcontent "sql" %}}
```sql
SET iceberg.catalog.<catalogName>.<property>=<value>;
```
{{% /tabcontent %}}
{{% /codetabs %}}

## Hive Table Properties

In Hive, table properties can be set through `ALTER TABLE` SQL commands.

{{% codetabs "HiveTableProperties" %}}
{{% addtab "SQL" "general-languages" "sql" %}}
{{% tabcontent "sql" %}}
```sql
ALTER TABLE tbl SET TBLPROPERTIES('<property>'='<value>');
```
{{% /tabcontent %}}
{{% /codetabs %}}

| Property                              | Default          | Description                                                                        |
| ------------------------------------- | ---------------- | ---------------------------------------------------------------------------------- |
| external.table.purge        | true                       | If all data and metadata should be purged in a table by default when the table is dropped |
| gc.enabled                  | true                       | if all data and metadata should be purged in the table by default |

{{< hint info >}}
Changing `gc.enabled` on the Iceberg table via UpdateProperties, updates
`external.table.purge` on the HMS table accordingly. Setting `external.table.purge` as a table
prop during Hive `CREATE TABLE` pushes `gc.enabled` down to the Iceberg table properties.
This ensures these properties are always consistent at table level between Hive and
Iceberg.
{{< /hint >}}

{{< hint info >}}
HMS table properties and Iceberg table properties are kept in sync for HiveCatalog tables in Hive 4.0.0+
{{< /hint >}}

See the [Table Properties](#table-properties) section to see a list of all available table properties.

## Table Properties

### Read Properties

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| read.split.target-size            | 134217728 (128 MB) | Target size when combining data input splits           |
| read.split.metadata-target-size   | 33554432 (32 MB)   | Target size when combining metadata input splits       |
| read.split.planning-lookback      | 10                 | Number of bins to consider when combining input splits |
| read.split.open-file-cost         | 4194304 (4 MB)     | The estimated cost to open a file, used as a minimum weight when combining splits. |
| read.parquet.vectorization.enabled| false              | Enables parquet vectorized reads                       |
| read.parquet.vectorization.batch-size| 5000            | The batch size for parquet vectorized reads            |
| read.orc.vectorization.enabled    | false              | Enables orc vectorized reads                           |
| read.orc.vectorization.batch-size | 5000               | The batch size for orc vectorized reads                |

### Write Properties

| Property                           | Default            | Description                                        |
| ---------------------------------- | ------------------ | -------------------------------------------------- |
| write.format.default               | parquet            | Default file format for the table; parquet, avro, or orc |
| write.delete.format.default        | data file format   | Default delete file format for the table; parquet, avro, or orc |
| write.parquet.row-group-size-bytes | 134217728 (128 MB) | Parquet row group size                             |
| write.parquet.page-size-bytes      | 1048576 (1 MB)     | Parquet page size                                  |
| write.parquet.dict-size-bytes      | 2097152 (2 MB)     | Parquet dictionary page size                       |
| write.parquet.compression-codec    | gzip               | Parquet compression codec: zstd, brotli, lz4, gzip, snappy, uncompressed |
| write.parquet.compression-level    | null               | Parquet compression level                          |
| write.avro.compression-codec       | gzip               | Avro compression codec: gzip(deflate with 9 level), zstd, snappy, uncompressed |
| write.avro.compression-level       | null               | Avro compression level                             |
| write.orc.stripe-size-bytes        | 67108864 (64 MB)   | Define the default ORC stripe size, in bytes       |
| write.orc.block-size-bytes         | 268435456 (256 MB) | Define the default file system block size for ORC files |
| write.orc.compression-codec        | zlib               | ORC compression codec: zstd, lz4, lzo, zlib, snappy, none |
| write.orc.compression-strategy     | speed              | ORC compression strategy: speed, compression |
| write.location-provider.impl       | null               | Optional custom implementation for LocationProvider  |
| write.metadata.compression-codec   | none               | Metadata compression codec; none or gzip           |
| write.metadata.metrics.default     | truncate(16)       | Default metrics mode for all columns in the table; none, counts, truncate(length), or full |
| write.metadata.metrics.column.col1 | (not set)          | Metrics mode for column 'col1' to allow per-column tuning; none, counts, truncate(length), or full |
| write.target-file-size-bytes       | 536870912 (512 MB) | Controls the size of files generated to target about this many bytes |
| write.delete.target-file-size-bytes| 67108864 (64 MB)   | Controls the size of delete files generated to target about this many bytes |
| write.distribution-mode            | none               | Defines distribution of write data: __none__: don't shuffle rows; __hash__: hash distribute by partition key ; __range__: range distribute by partition key or sort key if table has an SortOrder |
| write.delete.distribution-mode     | hash               | Defines distribution of write delete data          |
| write.wap.enabled                  | false              | Enables write-audit-publish writes |
| write.summary.partition-limit      | 0                  | Includes partition-level summary stats in snapshot summaries if the changed partition count is less than this limit |
| write.metadata.delete-after-commit.enabled | false      | Controls whether to delete the oldest version metadata files after commit |
| write.metadata.previous-versions-max       | 100        | The max number of previous version metadata files to keep before deleting after commit |
| write.spark.fanout.enabled         | false              | Enables the fanout writer in Spark that does not require data to be clustered; uses more memory |
| write.object-storage.enabled       | false              | Enables the object storage location provider that adds a hash component to file paths |
| write.data.path                    | table location + /data | Base location for data files |
| write.metadata.path                | table location + /metadata | Base location for metadata files |
| write.delete.mode                  | copy-on-write      | Mode used for delete commands: copy-on-write or merge-on-read (v2 only) |
| write.delete.isolation-level       | serializable       | Isolation level for delete commands: serializable or snapshot |
| write.update.mode                  | copy-on-write      | Mode used for update commands: copy-on-write or merge-on-read (v2 only) |
| write.update.isolation-level       | serializable       | Isolation level for update commands: serializable or snapshot |
| write.merge.mode                   | copy-on-write      | Mode used for merge commands: copy-on-write or merge-on-read (v2 only) |
| write.merge.isolation-level        | serializable       | Isolation level for merge commands: serializable or snapshot |

### Behavior Properties

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
| history.expire.max-snapshot-age-ms | 432000000 (5 days) | Default max age of snapshots to keep while expiring snapshots    |
| history.expire.min-snapshots-to-keep | 1                | Default min number of snapshots to keep while expiring snapshots |
| history.expire.max-ref-age-ms      | `Long.MAX_VALUE` (forever) | For snapshot references except the `main` branch, default max age of snapshot references to keep while expiring snapshots. The `main` branch never expires. |

### Reserved Table Properties

| Property       | Default  | Description                                                   |
| -------------- | -------- | ------------------------------------------------------------- |
| format-version | 1        | Table's format version (can be 1 or 2) as defined in the [Spec](../../../spec/#format-versioning). |

{{< hint info >}}
Reserved table properties are only used to control behaviors when creating or updating a table.
The value of these properties are not persisted as a part of the table metadata.
{{< /hint >}}

### Table Compatibility Flags

| Property                                      | Default  | Description                                                                               |
| --------------------------------------------- | -------- | ----------------------------------------------------------------------------------------- |
| compatibility.snapshot-id-inheritance.enabled | false    | Enables committing snapshots without explicit snapshot IDs; Not required for v2 tables    |

## Hadoop Configurations

The following properties from the Hadoop configuration are used by the Hive Metastore connector.

| Property                              | Default          | Description                                                                        |
| ------------------------------------- | ---------------- | ---------------------------------------------------------------------------------- |
| iceberg.hive.client-pool-size         | 5                | The size of the Hive client pool when tracking tables in HMS                       |
| iceberg.hive.lock-timeout-ms          | 360000 (6 min)   | Maximum time in milliseconds to acquire a lock                                     |
| iceberg.hive.lock-check-min-wait-ms   | 50               | Minimum time in milliseconds to check back on the status of lock acquisition       |
| iceberg.hive.lock-check-max-wait-ms   | 5000             | Maximum time in milliseconds to check back on the status of lock acquisition       |
| iceberg.mr.reuse.containers  | false                   | if Avro reader should reuse containers                 |
| iceberg.mr.case.sensitive    | true                    | if the query is case-sensitive                         |
| iceberg.mr.commit.table.thread.pool.size          | 10                                       | the number of threads of a shared thread pool to execute parallel commits for output tables |
| iceberg.mr.commit.file.thread.pool.size           | 10                                       | the number of threads of a shared thread pool to execute parallel commits for files in each output table |
| iceberg.mr.schema.auto.conversion        | false                       | if Hive should perform type auto-conversion         |

{{< hint info >}}
`iceberg.hive.lock-check-max-wait-ms` should be less than the [transaction timeout](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-hive.txn.timeout) 
of the Hive Metastore (`hive.txn.timeout` or `metastore.txn.timeout` in the newer versions). Otherwise, the heartbeats on the lock (which happens during the lock checks) would end up expiring in the 
Hive Metastore before the lock is retried from Iceberg.
{{< /hint >}}

