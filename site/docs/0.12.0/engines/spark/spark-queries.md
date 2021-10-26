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

# Spark Queries

To use Iceberg in Spark, first configure [Spark catalogs](./spark-configuration.md).

Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations. Spark DSv2 is an evolving API with different levels of support in Spark versions:

| Feature support                                  | Spark 3.0| Spark 2.4  | Notes                                          |
|--------------------------------------------------|----------|------------|------------------------------------------------|
| [`SELECT`](#querying-with-sql)                   | ✔️        |            |                                                |
| [DataFrame reads](#querying-with-dataframes)     | ✔️        | ✔️          |                                                |
| [Metadata table `SELECT`](#inspecting-tables)    | ✔️        |            |                                                |
| [History metadata table](#history)               | ✔️        | ✔️          |                                                |
| [Snapshots metadata table](#snapshots)           | ✔️        | ✔️          |                                                |
| [Files metadata table](#files)                   | ✔️        | ✔️          |                                                |
| [Manifests metadata table](#manifests)           | ✔️        | ✔️          |                                                |


## Querying with SQL

In Spark 3, tables use identifiers that include a [catalog name](./spark-configuration.md#using-catalogs).

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

### Catalogs with DataFrameReader

Iceberg 0.11.0 adds multi-catalog support to `DataFrameReader` in both Spark 3.x and 2.4.

Paths and table names can be loaded with Spark's `DataFrameReader` interface. How tables are loaded depends on how
the identifier is specified. When using `spark.read.format("iceberg").path(table)` or `spark.table(table)` the `table`
variable can take a number of forms as listed below:

*  `file:/path/to/table`: loads a HadoopTable at given path
*  `tablename`: loads `currentCatalog.currentNamespace.tablename`
*  `catalog.tablename`: loads `tablename` from the specified catalog.
*  `namespace.tablename`: loads `namespace.tablename` from current catalog
*  `catalog.namespace.tablename`: loads `namespace.tablename` from the specified catalog.
*  `namespace1.namespace2.tablename`: loads `namespace1.namespace2.tablename` from current catalog

The above list is in order of priority. For example: a matching catalog will take priority over any namespace resolution.


### Time travel

To select a specific table snapshot or the snapshot at some time, Iceberg supports two Spark read options:

* `snapshot-id` selects a specific table snapshot
* `as-of-timestamp` selects the current snapshot at a timestamp, in milliseconds

```scala
// time travel to October 26, 1986 at 01:21:00
spark.read
    .option("as-of-timestamp", "499162860000")
    .format("iceberg")
    .load("path/to/table")
```

```scala
// time travel to snapshot with ID 10963874102873L
spark.read
    .option("snapshot-id", 10963874102873L)
    .format("iceberg")
    .load("path/to/table")
```

!!! Note
    Spark does not currently support using `option` with `table` in DataFrameReader commands. All options will be silently 
    ignored. Do not use `table` when attempting to time-travel or use other options. Options will be supported with `table`
    in [Spark 3.1 - SPARK-32592](https://issues.apache.org/jira/browse/SPARK-32592).

Time travel is not yet supported by Spark's SQL syntax.

### Spark 2.4

Spark 2.4 requires using the DataFrame reader with `iceberg` as a format, because 2.4 does not support direct SQL queries:

```scala
// named metastore table
spark.read.format("iceberg").load("catalog.db.table")
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


## Inspecting tables

To inspect a table's history, snapshots, and other metadata, Iceberg supports metadata tables.

Metadata tables are identified by adding the metadata table name after the original table name. For example, history for `db.table` is read using `db.table.history`.

!!! Note
    As of Spark 3.0, the format of the table name for inspection (`catalog.database.table.metadata`) doesn't work with Spark's default catalog (`spark_catalog`). If you've replaced the default catalog, you may want to use `DataFrameReader` API to inspect the table. 

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

### Files

To show a table's data files and each file's metadata, run:

```sql
SELECT * FROM prod.db.table.files
```
```text
+-------------------------------------------------------------------------+-------------+--------------+--------------------+--------------------+------------------+-------------------+------------------+-----------------+-----------------+--------------+---------------+
| file_path                                                               | file_format | record_count | file_size_in_bytes | column_sizes       | value_counts     | null_value_counts | nan_value_counts | lower_bounds    | upper_bounds    | key_metadata | split_offsets |
+-------------------------------------------------------------------------+-------------+--------------+--------------------+--------------------+------------------+-------------------+------------------+-----------------+-----------------+--------------+---------------+
| s3:/.../table/data/00000-3-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> c] | [1 -> , 2 -> c] | null         | [4]           |
| s3:/.../table/data/00001-4-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> b] | [1 -> , 2 -> b] | null         | [4]           |
| s3:/.../table/data/00002-5-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> a] | [1 -> , 2 -> a] | null         | [4]           |
+-------------------------------------------------------------------------+-------------+--------------+--------------------+--------------------+------------------+-------------------+------------------+-----------------+-----------------+--------------+---------------+
```

### Manifests

To show a table's file manifests and each file's metadata, run:

```sql
SELECT * FROM prod.db.table.manifests
```
```text
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+--------------------------------------+
| path                                                                 | length | partition_spec_id | added_snapshot_id   | added_data_files_count | existing_data_files_count | deleted_data_files_count | partition_summaries                  |
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+--------------------------------------+
| s3://.../table/metadata/45b5290b-ee61-4788-b324-b1e2735c0e10-m0.avro | 4479   | 0                 | 6668963634911763636 | 8                      | 0                         | 0                        | [[false,null,2019-05-13,2019-05-15]] |
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+--------------------------------------+
```

Note: 
1. Fields within `partition_summaries` column of the manifests table correspond to `field_summary` structs within [manifest list](./spec.md#manifest-lists), with the following order: 
   - `contains_null`
   - `contains_nan`
   - `lower_bound`
   - `upper_bound`
2. `contains_nan` could return null, which indicates that this information is not available from files' metadata. 
   This usually occurs when reading from V1 table, where `contains_nan` is not populated. 

## Inspecting with DataFrames

Metadata tables can be loaded in Spark 2.4 or Spark 3 using the DataFrameReader API:

```scala
// named metastore table
spark.read.format("iceberg").load("db.table.files").show(truncate = false)
// Hadoop path table
spark.read.format("iceberg").load("hdfs://nn:8020/path/to/table#files").show(truncate = false)
```

