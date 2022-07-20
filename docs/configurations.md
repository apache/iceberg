---
title: "Configuration"
url: configuration
menu: main
weight: 1201
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

## Spark

### Spark Catalog Configurations

Catalog properties can be set in spark using either `--conf` when launching the shell, or in the `spark-defaults.conf` file.

{{% codetabs "SparkCatalogProperties" %}}
{{% addtab "CLI" "spark-init" "cli" %}}
{{% addtab "spark-defaults.conf" "spark-init" "spark-defaults" %}}
{{% tabcontent "cli"  %}}
```sh
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:{{% icebergVersion %}}\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.demo.type=hadoop \
    --conf spark.sql.catalog.demo.warehouse=$PWD/warehouse \
    --conf spark.sql.defaultCatalog=demo
```
{{% /tabcontent %}}
{{% tabcontent "spark-defaults" %}}
```sh
spark.jars.packages                                  org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:{{% icebergVersion %}}
spark.sql.extensions                                 org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog                      org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type                 hive
spark.sql.catalog.demo                               org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.demo.type                          hadoop
spark.sql.catalog.demo.warehouse                     $PWD/warehouse
spark.sql.defaultCatalog                             demo
```
{{% /tabcontent %}}
{{% /codetabs %}}

| Property                                           | Values                        | Description                                                          |
| -------------------------------------------------- | ----------------------------- | -------------------------------------------------------------------- |
| spark.sql.catalog.spark_catalog                    | org.apache.iceberg.spark.SparkSessionCatalog | Adds support for Iceberg tables to Spark’s built-in catalog |
| spark.sql.catalog._catalog-name_.type              | `hive` or `hadoop`            | The underlying Iceberg catalog implementation, `HiveCatalog`, `HadoopCatalog` or left unset if using a custom catalog. |
| spark.sql.catalog._catalog-name_.catalog-impl      |                               | The underlying Iceberg catalog implementation.|
| spark.sql.catalog._catalog-name_.default-namespace | default                       | The default current namespace for the catalog |
| spark.sql.catalog._catalog-name_.uri               | thrift://host:port            | Metastore connect URI; default from `hive-site.xml` |
| spark.sql.catalog._catalog-name_.warehouse         | hdfs://nn:8020/warehouse/path | Base path for the warehouse directory |
| spark.sql.catalog._catalog-name_.cache-enabled     | `true` or `false`             | Whether to enable catalog cache, default value is `true` |
| spark.sql.catalog._catalog-name_.cache.expiration-interval-ms | `30000` (30 seconds) | Duration after which cached catalog entries are expired; Only effective if `cache-enabled` is `true`. `-1` disables cache expiration and `0` disables caching entirely, irrespective of `cache-enabled`. Default is `30000` (30 seconds) |                                                   |
|  spark.sql.catalog._catalog-name_.s3.delete-enabled     | `true` or `false` | When set to `false`, the objects are not hard-deleted from S3 |
|  spark.sql.catalog._catalog-name_.s3.delete.tags._tag_name_    | null | Objects are tagged with the configured key-value pairs before deletion.
Users can configure tag-based object lifecycle policy at bucket level to transition objects to different tiers. |
|  spark.sql.catalog._catalog-name_.s3.delete.num-threads    | null | Number of threads to be used for adding delete tags to the S3 objects |

{{< hint info >}}To enable Iceberg's Spark SQL extensions, set `spark.sql.extensions` to `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`
in your Spark conf.{{< /hint >}}

### Spark Runtime Read Options

Read options can be set in spark when using the DataFrame API.

{{% codetabs "SparkReadOptions" %}}
{{% addtab "Spark-Shell" "spark-queries" "spark-shell" %}}
{{% addtab "PySpark" "spark-queries" "pyspark" %}}
{{% tabcontent "spark-shell" %}}
```scala
val df = spark.read.option("snapshot-id", 5636037920220996438L).table("nyc.taxis_sample")
```
{{% /tabcontent %}}
{{% tabcontent "pyspark" %}}
```py
df = spark.read.option("snapshot-id", 5636037920220996438).table("nyc.taxis_sample")
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

### Spark Runtime Write Options

Write options can be set in spark when using the DataFrame API.

{{% codetabs "SparkWriteOptions" %}}
{{% addtab "Spark-Shell" "spark-queries" "spark-shell" %}}
{{% addtab "PySpark" "spark-queries" "pyspark" %}}
{{% tabcontent "spark-shell" %}}
```scala
df.write.option("target-file-size-bytes", 268435456).saveAsTable("nyc.taxis_sample")
```
{{% /tabcontent %}}
{{% tabcontent "pyspark" %}}
```py
df.write.option("target-file-size-bytes", 268435456).saveAsTable("nyc.taxis_sample")
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

### Spark Table Properties

Table properties can be set when creating a table using SparkSQL.

```sql
CREATE TABLE demo.nyc.taxis (
  `vendor_id` BIGINT,
  `tpep_pickup_datetime` TIMESTAMP,
  `tpep_dropoff_datetime` TIMESTAMP,
  `passenger_count` DOUBLE,
  `trip_distance` DOUBLE
)
TBLPROPERTIES(
  'read.split.target-size'='268435456',
  'write.target-file-size-bytes'='268435456',
  'history.expire.max-snapshot-age-ms'='604800000'
)
```

#### Spark Table Read Properties

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

#### Spark Table Write Properties

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

#### Spark Table Behavior Properties

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

#### Reserved Table Properties

| Property       | Default  | Description                                                   |
| -------------- | -------- | ------------------------------------------------------------- |
| format-version | 1        | Table's format version (can be 1 or 2) as defined in the [Spec](../../../spec/#format-versioning). |

{{< hint info >}}
Reserved table properties are only used to control behaviors when creating or updating a table.
The value of these properties are not persisted as a part of the table metadata.
{{< /hint >}}

#### Compatibility Flags

| Property                                      | Default  | Description                                                                               |
| --------------------------------------------- | -------- | ----------------------------------------------------------------------------------------- |
| compatibility.snapshot-id-inheritance.enabled | false    | Enables committing snapshots without explicit snapshot IDs; Not required for v2 tables    |

## Java Client

When using the Java client, catalog properties can be set during catalog initialization.

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;

Map<String, String> properties = new HashMap<>();
properties.put("catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog");
properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
properties.put("warehouse", "s3://...");
properties.put("uri", "jdbc:postgresql://postgres:5432/demo_catalog");
properties.put("jdbc.user", "admin");
properties.put("jdbc.password", "password");

JdbcCatalog catalog = new JdbcCatalog();
Configuration conf = new Configuration();
catalog.setConf(conf);
catalog.initialize("demo", properties);
```

### Java Client Catalog Properties

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| catalog-impl                      | null               | a custom `Catalog` implementation to use by an engine  |
| io-impl                           | null               | a custom `FileIO` implementation to use in a catalog   |
| warehouse                         | null               | the root path of the data warehouse                    |
| uri                               | null               | a URI string, such as Hive metastore URI               |
| clients                           | 2                  | client pool size                                       |
| cache-enabled                     | true               | Whether to cache catalog entries |
| cache.expiration-interval-ms      | 30000              | How long catalog entries are locally cached, in milliseconds; 0 disables caching, negative values disable expiration |
| lock-impl                         | null               | a custom implementation of the lock manager, the actual interface depends on the catalog used  |
| lock.table                        | null               | an auxiliary table for locking, such as in [AWS DynamoDB lock manager](../aws/#dynamodb-for-commit-locking)  |
| lock.acquire-interval-ms          | 5 seconds          | the interval to wait between each attempt to acquire a lock  |
| lock.acquire-timeout-ms           | 3 minutes          | the maximum time to try acquiring a lock               |
| lock.heartbeat-interval-ms        | 3 seconds          | the interval to wait between each heartbeat after acquiring a lock  |
| lock.heartbeat-timeout-ms         | 15 seconds         | the maximum time without a heartbeat to consider a lock expired  |

{{< hint info >}}
`HadoopCatalog` and `HiveCatalog` can access the properties in their constructors.
Any other custom catalog can access the properties by implementing `Catalog.initialize(catalogName, catalogProperties)`.
The properties can be manually constructed or passed in from a compute engine like Spark or Flink.
Spark uses its session properties as catalog properties, see more details in the [Spark configuration](../spark-configuration#catalog-configuration) section.
Flink passes in catalog properties through `CREATE CATALOG` statement, see more details in the [Flink](../flink/#creating-catalogs-and-using-catalogs) section.
{{< /hint >}}

### JDBC Catalog Properties

| Property          | Default            | Description                              |
| ----------------- | ------------------ | ---------------------------------------- |
| uri               | null               | A JDBC URI                               |
| jdbc.user         | null               | The username for the JDBC connection     |
| jdbc.password     | null               | The password for the JDBC connection     |

### DynamoDB Catalog Properties

| Property                   | Default            | Description                                         |
| -------------------------- | ------------------ | --------------------------------------------------- |
| dynamodb.table-name        | iceberg            | name of the DynamoDB table used by DynamoDbCatalog  |

## Hive

Hive configuration can be set in the `hive-site.xml` file or inherited from the hadoop configuration defined in the `core-default.xml` or `core-site.xml` files.

```xml
  <property>
    <name>iceberg.hive.lock-timeout-ms</name>
    <value>600000</value>
  </property>
```

### Hadoop Configurations

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

### Hive Table Level Configurations

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

## FileIO

### S3FileIO

| Property                          | Default                                            | Description                                            |
| --------------------------------- | -------------------------------------------------- | ------------------------------------------------------ |
| s3.multipart.num-threads          | the available number of processors in the system   | number of threads to use for uploading parts to S3 (shared across all output streams)  |
| s3.multipart.part-size-bytes      | 32MB                                               | the size of a single part for multipart upload requests  |
| s3.multipart.threshold            | 1.5                                                | the threshold expressed as a factor times the multipart size at which to switch from uploading using a single put object request to uploading using multipart upload  |
| s3.staging-dir                    | `java.io.tmpdir` property value                    | the directory to hold temporary files  |
| s3.sse.type                       | `none`                                             | `none`, `s3`, `kms` or `custom`                        |
| s3.sse.key                        | `aws/s3` for `kms` type, null otherwise            | A KMS Key ID or ARN for `kms` type, or a custom base-64 AES256 symmetric key for `custom` type.  |
| s3.sse.md5                        | null                                               | If SSE type is `custom`, this value must be set as the base-64 MD5 digest of the symmetric key to ensure integrity. |
| s3.write.tags.<key>               | null                                               | Custom tags to add to S3 objects while writing and deleting. |
| s3.delete-enabled                 | true                                               | Whether to hard-delete files from S3. Commonly used in conjunction with `s3.delete.tags.<key>` |
| s3.delete.tags.<key>              | null                                               | Key-value pair to tag S3 objects for deletion |
| s3.delete.num-threads             | null                                               | The number of threads to use for adding delete tags to the S3 objects. |
| s3.use-arn-region-enabled         | true                                               | Enable S3FileIO to make cross-region calls; Not required for same-region or multi-region access points |
| s3.access-points.<bucket>         | null                                               | Access point to use for all S3 operations against <bucket>   |
| client.assume-role.arn            | null, requires user input                          | ARN of the role to assume, e.g. arn:aws:iam::123456789:role/myRoleToAssume  |
| client.assume-role.region         | null, requires user input                          | All AWS clients except the STS client will use the given region instead of the default region chain  |
| client.assume-role.external-id    | null                                               | An optional [external ID](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)  |
| client.assume-role.timeout-sec    | 1 hour                                             | Timeout of each assume role session. At the end of the timeout, a new set of role session credentials will be fetched through a STS client.  |
