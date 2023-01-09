---
title: "Configuration"
url: spark-configuration
aliases:
    - "spark/spark-configuration"
menu:
    main:
        parent: Spark
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

# Spark Configuration

## Catalogs

Spark adds an API to plug in table catalogs that are used to load, create, and manage Iceberg tables. Spark catalogs are configured by setting Spark properties under `spark.sql.catalog`.

This creates an Iceberg catalog named `hive_prod` that loads tables from a Hive metastore:

```plain
spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hive_prod.type = hive
spark.sql.catalog.hive_prod.uri = thrift://metastore-host:port
# omit uri to use the same URI as Spark: hive.metastore.uris in hive-site.xml
```

Iceberg also supports a directory-based catalog in HDFS that can be configured using `type=hadoop`:

```plain
spark.sql.catalog.hadoop_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hadoop_prod.type = hadoop
spark.sql.catalog.hadoop_prod.warehouse = hdfs://nn:8020/warehouse/path
```

{{< hint info >}}
The Hive-based catalog only loads Iceberg tables. To load non-Iceberg tables in the same Hive metastore, use a [session catalog](#replacing-the-session-catalog).
{{< /hint >}}

### Catalog configuration

A catalog is created and named by adding a property `spark.sql.catalog.(catalog-name)` with an implementation class for its value.

Iceberg supplies two implementations:

* `org.apache.iceberg.spark.SparkCatalog` supports a Hive Metastore or a Hadoop warehouse as a catalog
* `org.apache.iceberg.spark.SparkSessionCatalog` adds support for Iceberg tables to Spark's built-in catalog, and delegates to the built-in catalog for non-Iceberg tables

Both catalogs are configured using properties nested under the catalog name. Common configuration properties for Hive and Hadoop are:

| Property                                           | Values                        | Description                                                          |
| -------------------------------------------------- | ----------------------------- | -------------------------------------------------------------------- |
| spark.sql.catalog._catalog-name_.type              | `hive` or `hadoop`            | The underlying Iceberg catalog implementation, `HiveCatalog`, `HadoopCatalog` or left unset if using a custom catalog |
| spark.sql.catalog._catalog-name_.catalog-impl      |                               | The underlying Iceberg catalog implementation.|
| spark.sql.catalog._catalog-name_.default-namespace | default                       | The default current namespace for the catalog |
| spark.sql.catalog._catalog-name_.uri               | thrift://host:port            | Metastore connect URI; default from `hive-site.xml` |
| spark.sql.catalog._catalog-name_.warehouse         | hdfs://nn:8020/warehouse/path | Base path for the warehouse directory |
| spark.sql.catalog._catalog-name_.cache-enabled     | `true` or `false`             | Whether to enable catalog cache, default value is `true` |
| spark.sql.catalog._catalog-name_.cache.expiration-interval-ms | `30000` (30 seconds) | Duration after which cached catalog entries are expired; Only effective if `cache-enabled` is `true`. `-1` disables cache expiration and `0` disables caching entirely, irrespective of `cache-enabled`. Default is `30000` (30 seconds) |                                                   |

Additional properties can be found in common [catalog configuration](../configuration#catalog-properties).


### Using catalogs

Catalog names are used in SQL queries to identify a table. In the examples above, `hive_prod` and `hadoop_prod` can be used to prefix database and table names that will be loaded from those catalogs.

```sql
SELECT * FROM hive_prod.db.table -- load db.table from catalog hive_prod
```

Spark 3 keeps track of the current catalog and namespace, which can be omitted from table names.

```sql
USE hive_prod.db;
SELECT * FROM table -- load db.table from catalog hive_prod
```

To see the current catalog and namespace, run `SHOW CURRENT NAMESPACE`.

### Replacing the session catalog

To add Iceberg table support to Spark's built-in catalog, configure `spark_catalog` to use Iceberg's `SparkSessionCatalog`.

```plain
spark.sql.catalog.spark_catalog = org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type = hive
```

Spark's built-in catalog supports existing v1 and v2 tables tracked in a Hive Metastore. This configures Spark to use Iceberg's `SparkSessionCatalog` as a wrapper around that session catalog. When a table is not an Iceberg table, the built-in catalog will be used to load it instead.

This configuration can use same Hive Metastore for both Iceberg and non-Iceberg tables.

### Using catalog specific Hadoop configuration values

Similar to configuring Hadoop properties by using `spark.hadoop.*`, it's possible to set per-catalog Hadoop configuration values when using Spark by adding the property for the catalog with the prefix `spark.sql.catalog.(catalog-name).hadoop.*`. These properties will take precedence over values configured globally using `spark.hadoop.*` and will only affect Iceberg tables.

```plain
spark.sql.catalog.hadoop_prod.hadoop.fs.s3a.endpoint = http://aws-local:9000
```

### Loading a custom catalog

Spark supports loading a custom Iceberg `Catalog` implementation by specifying the `catalog-impl` property. Here is an example:

```plain
spark.sql.catalog.custom_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.custom_prod.catalog-impl = com.my.custom.CatalogImpl
spark.sql.catalog.custom_prod.my-additional-catalog-config = my-value
```

### Catalogs in Spark 2.4

When using Iceberg 0.11.0 and later, Spark 2.4 can load tables from multiple Iceberg catalogs or from table locations.

Catalogs in 2.4 are configured just like catalogs in 3.x, but only Iceberg catalogs are supported.


## SQL Extensions

Iceberg 0.11.0 and later add an extension module to Spark to add new SQL commands, like `CALL` for stored procedures or `ALTER TABLE ... WRITE ORDERED BY`.

Using those SQL commands requires adding Iceberg extensions to your Spark environment using the following Spark property:


| Spark extensions property | Iceberg extensions implementation                                   |
|---------------------------|---------------------------------------------------------------------|
| `spark.sql.extensions`    | `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` |

SQL extensions are not available for Spark 2.4.


## Runtime configuration

### Read options

Spark read options are passed when configuring the DataFrameReader, like this:

```scala
// time travel
spark.read
    .option("snapshot-id", 10963874102873L)
    .table("catalog.db.table")
```

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

### Write options

Spark write options are passed when configuring the DataFrameWriter, like this:

```scala
// write with Avro instead of Parquet
df.write
    .option("write-format", "avro")
    .option("snapshot-property.key", "value")
    .insertInto("catalog.db.table")
```

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
