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

# Spark

Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations. Spark DSv2 is an evolving API with different levels of support in Spark versions.

| Feature support                                  | Spark 3.0| Spark 2.4  | Notes                                          |
|--------------------------------------------------|----------|------------|------------------------------------------------|
| [SQL create table](#create-table)                | ✔️        |            |                                                |
| [SQL create table as](#create-table-as-select)   | ✔️        |            |                                                |
| [SQL replace table as](#replace-table-as-select) | ✔️        |            |                                                |
| [SQL alter table](#alter-table)                  | ✔️        |            |                                                |
| [SQL drop table](#drop-table)                    | ✔️        |            |                                                |
| [SQL select](#querying-with-sql)                 | ✔️        |            |                                                |
| [SQL insert into](#insert-into)                  | ✔️        |            |                                                |
| [SQL insert overwrite](#insert-overwrite)        | ✔️        |            |                                                |
| [DataFrame reads](#querying-with-dataframes)     | ✔️        | ✔️          |                                                |
| [DataFrame append](#appending-data)              | ✔️        | ✔️          |                                                |
| [DataFrame overwrite](#overwriting-data)         | ✔️        | ✔️          | ⚠ Behavior changed in Spark 3.0                |
| [DataFrame CTAS and RTAS](#creating-tables)      | ✔️        |            |                                                |
| [Metadata tables](#inspecting-tables)            | ✔️        | ✔️          |                                                |

## Configuring catalogs

Spark 3.0 adds an API to plug in table catalogs that are used to load, create, and manage Iceberg tables. Spark catalogs are configured by setting [Spark properties](../configuration#catalogs) under `spark.sql.catalog`.

This creates an Iceberg catalog named `hive_prod` that loads tables from a Hive metastore:

```plain
spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hive_prod.type = hive
spark.sql.catalog.hive_prod.uri = thrift://metastore-host:port
```

Iceberg also supports a directory-based catalog in HDFS that can be configured using `type=hadoop`:

```plain
spark.sql.catalog.hadoop_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hadoop_prod.type = hadoop
spark.sql.catalog.hadoop_prod.warehouse = hdfs://nn:8020/warehouse/path
```

!!! Note
    The Hive-based catalog only loads Iceberg tables. To load non-Iceberg tables in the same Hive metastore, use a [session catalog](#replacing-the-session-catalog).

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
# omit uri to use the same URI as Spark: hive.metastore.uris in hive-site.xml
```

Spark's built-in catalog supports existing v1 and v2 tables tracked in a Hive Metastore. This configures Spark to use Iceberg's `SparkSessionCatalog` as a wrapper around that session catalog. When a table is not an Iceberg table, the built-in catalog will be used to load it instead.

This configuration can use same Hive Metastore for both Iceberg and non-Iceberg tables.


## DDL commands

!!! Note
    Spark 2.4 can't create Iceberg tables with DDL, instead use the [Iceberg API](../java-api-quickstart).

### `CREATE TABLE`

Spark 3.0 can create tables in any Iceberg catalog with the clause `USING iceberg`:

```sql
CREATE TABLE prod.db.sample (
    id bigint COMMENT 'unique id',
    data string)
USING iceberg
```

Table create commands, including CTAS and RTAS, support the full range of Spark create clauses, including:

* `PARTITION BY (partition-expressions)` to configure partitioning
* `LOCATION '(fully-qualified-uri)'` to set the table location
* `COMMENT 'table documentation'` to set a table description
* `TBLPROPERTIES ('key'='value', ...)` to set [table configuration](../configuration)

Create commands may also set the default format with the `USING` clause. This is only supported for `SparkCatalog` because Spark handles the `USING` clause differently for the built-in catalog.

#### `PARTITIONED BY`

To create a partitioned table, use `PARTITIONED BY`:

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string)
USING iceberg
PARTITIONED BY (category)
```

The `PARTITIONED BY` clause supports transform expressions to create [hidden partitions](../partitioning).

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (bucket(16, id), days(ts), category)
```

Supported partition transforms are:

* `years` for yearly partitions
* `months` for monthly partitions
* `days` for daily partitions
* `hours` for hourly partitions
* `bucket` for bucketing (with width)
* `truncate` to truncate integers or strings (with length)

### `CREATE TABLE ... AS SELECT`

Iceberg supports CTAS as an atomic operation when using a [`SparkCatalog`](#configuring-catalogs). CTAS is supported, but is not atomic when using [`SparkSessionCatalog`](#replacing-the-session-catalog).

```sql
CREATE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```

### `REPLACE TABLE ... AS SELECT`

Iceberg supports RTAS as an atomic operation when using a [`SparkCatalog`](#configuring-catalogs). RTAS is supported, but is not atomic when using [`SparkSessionCatalog`](#replacing-the-session-catalog).

Atomic table replacement creates a new snapshot with the results of the `SELECT` query, but keeps table history.

```sql
REPLACE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```
```sql
CREATE OR REPLACE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```

The schema and partition spec will be replaced if changed. To avoid modifying the table's schema and partitioning, use `INSERT OVERWRITE` instead of `REPLACE TABLE`.

### `ALTER TABLE`

Iceberg has full `ALTER TABLE` support in Spark 3, including:

* Renaming a table
* Setting or removing table properties
* Adding, deleting, and renaming columns
* Adding, deleting, and renaming nested fields
* Reordering top-level columns and nested struct fields
* Widening the type of `int`, `float`, and `decimal` fields
* Making required columns optional

### `ALTER TABLE ... RENAME TO`

```sql
ALTER TABLE prod.db.sample RENAME TO prod.db.new_name
```

### `ALTER TABLE ... SET TBLPROPERTIES`

```sql
ALTER TABLE prod.db.sample SET TBLPROPERTIES (
    'read.split.target-size'='268435456'
)
```

Iceberg uses table properties to control table behavior. For a list of available properties, see [Table configuration](../configuration).

`UNSET` is used to remove properties:

```sql
ALTER TABLE prod.db.sample UNSET TBLPROPERTIES ('read.split.target-size')
```

### `ALTER TABLE ... ADD COLUMN`

```sql
ALTER TABLE prod.db.sample ADD COLUMN point struct<x: double NOT NULL, y: double NOT NULL> AFTER data
ALTER TABLE prod.db.sample ADD COLUMN point.z double FIRST
```

### `ALTER TABLE ... RENAME COLUMN`

```sql
ALTER TABLE prod.db.sample RENAME COLUMN data TO payload
ALTER TABLE prod.db.sample RENAME COLUMN location.lat TO latitude
```

Note that nested rename commands only rename the leaf field. The above command renames `location.lat` to `location.latitude`

### `ALTER TABLE ... ALTER COLUMN`

Alter column is used to widen types, make a field optional, set comments, and reorder fields.

```sql
ALTER TABLE prod.db.sample ALTER COLUMN id DROP NOT NULL
ALTER TABLE prod.db.sample ALTER COLUMN location.lat TYPE double
ALTER TABLE prod.db.sample ALTER COLUMN point.z AFTER y
ALTER TABLE prod.db.sample ALTER COLUMN id COMMENT 'unique id'
```

### `ALTER TABLE ... DROP COLUMN`

```sql
ALTER TABLE prod.db.sample DROP COLUMN id
ALTER TABLE prod.db.sample DROP COLUMN point.z
```

### `DROP TABLE`

To delete a table, run:

```sql
DROP TABLE prod.db.sample
```

## Querying with SQL

In Spark 3, tables use identifiers that include a [catalog name](#using-catalogs).

```sql
SELECT * FROM prod.db.table -- catalog: prod, namespace: db, table: table
```

Metadata tables, like `history` and `snapshots`, can use the Iceberg table name as a namespace.

For example, to read from the `files` metadata table for `prod.db.table`, run:

```
SELECT * FROM prod.db.table.files
```

## Querying with DataFrames

To load a table as a DataFrame, use `table`:

```scala
val df = spark.table("prod.db.table")
```

To configure the `DataFrameReader`, use a reader directly:

```sql
val df = spark.read
    .option("split-size", "268435456")
    .table("prod.db.table")
```

!!! Warning
    When reading with DataFrames in Spark 3, use `table` to load a table by name from a catalog.
    Using `format("iceberg")` loads an isolated table reference that is not refreshed when other queries update the table.


### Time travel

To select a specific table snapshot or the snapshot at some time, Iceberg supports two Spark read options:

* `snapshot-id` selects a specific table snapshot
* `as-of-timestamp` selects the current snapshot at a timestamp, in milliseconds

```scala
// time travel to October 26, 1986 at 01:21:00
spark.read
    .option("as-of-timestamp", "499162860000")
    .table("prod.db.table")
```

```scala
// time travel to snapshot with ID 10963874102873L
spark.read
    .option("snapshot-id", 10963874102873L)
    .table("prod.db.table")
```

Time travel is not yet supported by Spark's SQL syntax.

### Spark 2.4

Spark 2.4 requires using the DataFrame reader with `iceberg` as a format, becuase 2.4 does not support catalogs:

```scala
// named metastore table
spark.read.format("iceberg").load("db.table")
// Hadoop path table
spark.read.format("iceberg").load("hdfs://nn:8020/path/to/table")
```

#### Spark 2.4 with SQL

To run SQL `SELECT` statements on Iceberg tables in 2.4, register the DataFrame as a temporary table:

```scala
val df = spark.read.format("iceberg").load("db.table")
df.createOrReplaceTempView("table")

spark.sql("""select count(1) from table""").show()
```


## Writing with SQL

Spark 3 supports SQL `INSERT INTO` and `INSERT OVERWRITE`, as well as the new `DataFrameWriterV2` API.

### `INSERT INTO`

To append new data to a table, use `INSERT INTO`.

```sql
INSERT INTO prod.db.table VALUES (1, 'a'), (2, 'b')
```
```sql
INSERT INTO prod.db.table SELECT ...
```

### `INSERT OVERWRITE`

To replace data in the table with the result of a query, use `INSERT OVERWRITE`. Overwrites are atomic operations for Iceberg tables.

The partitions that will be replaced by `INSERT OVERWRITE` depends on Spark's partition overwrite mode and the partitioning of a table.

!!! Warning
    Spark 3.0.0 has a correctness bug that affects dynamic `INSERT OVERWRITE` with hidden partitioning, [SPARK-32168][spark-32168].
    For tables with [hidden partitions](../partitioning), wait for Spark 3.0.1.

[spark-32168]: https://issues.apache.org/jira/browse/SPARK-32168


#### Overwrite behavior

Spark's default overwrite mode is **static**, but **dynamic overwrite mode is recommended when writing to Iceberg tables.** Static overwrite mode determines which partitions to overwrite in a table by converting the `PARTITION` clause to a filter, but the `PARTITION` clause can only reference table columns.

Dynamic overwrite mode is configured by setting `spark.sql.sources.partitionOverwriteMode=dynamic`.

To demonstrate the behavior of dynamic and static overwrites, consider a `logs` table defined by the following DDL:

```sql
CREATE TABLE prod.my_app.logs (
    uuid string NOT NULL,
    level string NOT NULL,
    ts timestamp NOT NULL,
    message string)
USING iceberg
PARTITIONED BY (level, hours(ts))
```

#### Dynamic overwrite

When Spark's overwrite mode is dynamic, partitions that have rows produced by the `SELECT` query will be replaced.

For example, this query removes duplicate log events from the example `logs` table.

```sql
INSERT OVERWRITE prod.my_app.logs
SELECT uuid, first(level), first(ts), first(message)
FROM prod.my_app.logs
WHERE cast(ts as date) = '2020-07-01'
GROUP BY uuid
```

In dynamic mode, this will replace any partition with rows in the `SELECT` result. Because the date of all rows is restricted to 1 July, only hours of that day will be replaced.

#### Static overwrite

When Spark's overwrite mode is static, the `PARTITION` clause is converted to a filter that is used to delete from the table. If the `PARTITION` clause is omitted, all partitions will be replaced.

Because there is no `PARTITION` clause in the query above, it will drop all existing rows in the table when run in static mode, but will only write the logs from 1 July.

To overwrite just the partitions that were loaded, add a `PARTITION` clause that aligns with the `SELECT` query filter:

```sql
INSERT OVERWRITE prod.my_app.logs
PARTITION (level = 'INFO')
SELECT uuid, first(level), first(ts), first(message)
FROM prod.my_app.logs
WHERE level = 'INFO'
GROUP BY uuid
```

Note that this mode cannot replace hourly partitions like the dynamic example query because the `PARTITION` clause can only reference table columns, not hidden partitions.

### `DELETE FROM`

Spark 3 added support for `DELETE FROM` queries to remove data from tables.

Delete queries accept a filter to match rows to delete. Iceberg can delete data as long as the filter matches entire partitions of the table, or it can determine that all rows of a file match. If a file contains some rows that should be deleted and some that should not, Iceberg will throw an exception.

```sql
DELETE FROM prod.db.table
WHERE ts >= '2020-05-01 00:00:00' and ts < '2020-06-01 00:00:00'
```


## Writing with DataFrames

Spark 3 introduced the new `DataFrameWriterV2` API for writing to tables using data frames. The v2 API is recommended for several reasons:

* CTAS, RTAS, and overwrite by filter are supported
* All operations consistently write columns to a table by name
* Hidden partition expressions are supported in `partitionedBy`
* Overwrite behavior is explicit, either dynamic or by a user-supplied filter
* The behavior of each operation corresponds to SQL statements
    - `df.writeTo(t).create()` is equivalent to `CREATE TABLE AS SELECT`
    - `df.writeTo(t).replace()` is equivalent to `REPLACE TABLE AS SELECT`
    - `df.writeTo(t).append()` is equivalent to `INSERT INTO`
    - `df.writeTo(t).overwritePartitions()` is equivalent to dynamic `INSERT OVERWRITE`

The v1 DataFrame `write` API is still supported, but is not recommended.

!!! Warning
    When writing with the v1 DataFrame API in Spark 3, use `saveAsTable` or `insertInto` to load tables with a catalog.
    Using `format("iceberg")` loads an isolated table reference that will not automatically refresh tables used by queries.


### Appending data

To append a dataframe to an Iceberg table, use `append`:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").append()
```

#### Spark 2.4

In Spark 2.4, use the v1 API with `append` mode and `iceberg` format:

```scala
data.write
    .format("iceberg")
    .mode("append")
    .save("db.table")
```

### Overwriting data

To overwrite partitions dynamically, use `overwritePartitions()`:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").overwritePartitions()
```

To explicitly overwrite partitions, use `overwrite` to supply a filter:

```scala
data.writeTo("prod.db.table").overwrite($"level" === "INFO")
```

#### Spark 2.4

In Spark 2.4, overwrite values in an Iceberg table with `overwrite` mode and `iceberg` format:

```scala
data.write
    .format("iceberg")
    .mode("overwrite")
    .save("db.table")
```

!!! Warning
    **The behavior of overwrite mode changed between Spark 2.4 and Spark 3**.

The behavior of DataFrameWriter overwrite mode was undefined in Spark 2.4, but is required to overwrite the entire table in Spark 3. Because of this new requirement, the Iceberg source's behavior changed in Spark 3. In Spark 2.4, the behavior was to dynamically overwrite partitions. To use the Spark 2.4 behavior, add option `overwrite-mode=dynamic`.

### Creating tables

To run a CTAS or RTAS, use `create`, `replace`, or `createOrReplace` operations:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").create()
```

Create and replace operations support table configuration methods, like `partitionedBy` and `tableProperty`:

```scala
data.writeTo("prod.db.table")
    .tableProperty("write.format.default", "orc")
    .partitionBy($"level", days($"ts"))
    .createOrReplace()
```


## Inspecting tables

To inspect a table's history, snapshots, and other metadata, Iceberg supports metadata tables.

Metadata tables are identified by adding the metadata table name after the original table name. For example, history for `db.table` is read using `db.table.history`.

### History

To show table history, run:

```sql
SELECT * FROM prod.db.table.history
```
```text
+-------------------------+---------------------+---------------------+---------------------+
| made_current_at         | snapshot_id         | parent_id           | is_current_ancestor |
+-------------------------+---------------------+---------------------+---------------------+
| 2019-02-08 03:29:51.215 | 5781947118336215154 | NULL                | true                |
| 2019-02-08 03:47:55.948 | 5179299526185056830 | 5781947118336215154 | true                |
| 2019-02-09 16:24:30.13  | 296410040247533544  | 5179299526185056830 | false               |
| 2019-02-09 16:32:47.336 | 2999875608062437330 | 5179299526185056830 | true                |
| 2019-02-09 19:42:03.919 | 8924558786060583479 | 2999875608062437330 | true                |
| 2019-02-09 19:49:16.343 | 6536733823181975045 | 8924558786060583479 | true                |
+-------------------------+---------------------+---------------------+---------------------+
```

!!! Note
    **This shows a commit that was rolled back.** The example has two snapshots with the same parent, and one is *not* an ancestor of the current table state.

### Snapshots

To show the valid snapshots for a table, run:

```sql
SELECT * FROM prod.db.table.snapshots
```
```text
+-------------------------+----------------+-----------+-----------+----------------------------------------------------+-------------------------------------------------------+
| committed_at            | snapshot_id    | parent_id | operation | manifest_list                                      | summary                                               |
+-------------------------+----------------+-----------+-----------+----------------------------------------------------+-------------------------------------------------------+
| 2019-02-08 03:29:51.215 | 57897183625154 | null      | append    | s3://.../table/metadata/snap-57897183625154-1.avro | { added-records -> 2478404, total-records -> 2478404, |
|                         |                |           |           |                                                    |   added-data-files -> 438, total-data-files -> 438,   |
|                         |                |           |           |                                                    |   spark.app.id -> application_1520379288616_155055 }  |
| ...                     | ...            | ...       | ...       | ...                                                | ...                                                   |
+-------------------------+----------------+-----------+-----------+----------------------------------------------------+-------------------------------------------------------+
```

You can also join snapshots to table history. For example, this query will show table history, with the application ID that wrote each snapshot:

```sql
select
    h.made_current_at,
    s.operation,
    h.snapshot_id,
    h.is_current_ancestor,
    s.summary['spark.app.id']
from prod.db.table.history h
join prod.db.table.snapshots s
  on h.snapshot_id = s.snapshot_id
order by made_current_at
```
```text
+-------------------------+-----------+----------------+---------------------+----------------------------------+
| made_current_at         | operation | snapshot_id    | is_current_ancestor | summary[spark.app.id]            |
+-------------------------+-----------+----------------+---------------------+----------------------------------+
| 2019-02-08 03:29:51.215 | append    | 57897183625154 | true                | application_1520379288616_155055 |
| 2019-02-09 16:24:30.13  | delete    | 29641004024753 | false               | application_1520379288616_151109 |
| 2019-02-09 16:32:47.336 | append    | 57897183625154 | true                | application_1520379288616_155055 |
| 2019-02-08 03:47:55.948 | overwrite | 51792995261850 | true                | application_1520379288616_152431 |
+-------------------------+-----------+----------------+---------------------+----------------------------------+
```

### Manifests

To show a table's file manifests and each file's metadata, run:

```sql
SELECT * FROM prod.db.table.manifests
```
```text
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+---------------------------------+
| path                                                                 | length | partition_spec_id | added_snapshot_id   | added_data_files_count | existing_data_files_count | deleted_data_files_count | partitions                      |
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+---------------------------------+
| s3://.../table/metadata/45b5290b-ee61-4788-b324-b1e2735c0e10-m0.avro | 4479   | 0                 | 6668963634911763636 | 8                      | 0                         | 0                        | [[false,2019-05-13,2019-05-15]] |
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+---------------------------------+
```

### Files

To show a table's data files and each file's metadata, run:

```sql
SELECT * FROM prod.db.table.files
```
```text
+-------------------------------------------------------------------------+-------------+--------------+--------------------+--------------------+------------------+-------------------+-----------------+-----------------+--------------+---------------+
| file_path                                                               | file_format | record_count | file_size_in_bytes | column_sizes       | value_counts     | null_value_counts | lower_bounds    | upper_bounds    | key_metadata | split_offsets |
+-------------------------------------------------------------------------+-------------+--------------+--------------------+--------------------+------------------+-------------------+-----------------+-----------------+--------------+---------------+
| s3:/.../table/data/00000-3-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | [1 -> , 2 -> c] | [1 -> , 2 -> c] | null         | [4]           |
| s3:/.../table/data/00001-4-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | [1 -> , 2 -> b] | [1 -> , 2 -> b] | null         | [4]           |
| s3:/.../table/data/00002-5-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | [1 -> , 2 -> a] | [1 -> , 2 -> a] | null         | [4]           |
+-------------------------------------------------------------------------+-------------+--------------+--------------------+--------------------+------------------+-------------------+-----------------+-----------------+--------------+---------------+
```

### Inspecting with DataFrames

Metadata tables can be loaded in Spark 2.4 or Spark 3 using the DataFrameReader API:

```scala
// named metastore table
spark.read.format("iceberg").load("db.table.files").show(truncate = false)
// Hadoop path table
spark.read.format("iceberg").load("hdfs://nn:8020/path/to/table#files").show(truncate = false)
```
