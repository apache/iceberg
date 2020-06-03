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

| Feature support                              | Spark 2.4 | Spark 3.0 (unreleased) | Notes                                          |
|----------------------------------------------|-----------|------------------------|------------------------------------------------|
| [DataFrame reads](#reading-an-iceberg-table) | ✔️        | ✔️                     |                                                |
| [DataFrame append](#appending-data)          | ✔️        | ✔️                     |                                                |
| [DataFrame overwrite](#overwriting-data)     | ✔️        | ✔️                     | Overwrite mode replaces partitions dynamically |
| [Metadata tables](#inspecting-tables)        | ✔️        | ✔️                     |                                                |
| SQL create table                             |           | ✔️                     |                                                |
| SQL alter table                              |           | ✔️                     |                                                |
| SQL drop table                               |           | ✔️                     |                                                |
| SQL select                                   |           | ✔️                     |                                                |
| SQL create table as                          |           | ✔️                     |                                                |
| SQL replace table as                         |           | ✔️                     |                                                |
| SQL insert into                              |           | ✔️                     |                                                |
| SQL insert overwrite                         |           | ✔️                     |                                                |

!!! Note
    Spark 2.4 can't create Iceberg tables with DDL, instead use the [Iceberg API](../api-quickstart).


## Spark 2.4

### Creating a table

Spark 2.4 is limited to reading and writing existing Iceberg tables. Use the [Iceberg API](../api) to create Iceberg tables.

### Reading an Iceberg table

To read an Iceberg table, use the `iceberg` format in `DataFrameReader`:

```scala
spark.read.format("iceberg").load("db.table")
```

Iceberg tables identified by HDFS path are also supported:

```scala
spark.read.format("iceberg").load("hdfs://nn:8020/path/to/table")
```


### Time travel

To select a specific table snapshot or the snapshot at some time, Iceberg supports two Spark read options:

* `snapshot-id` selects a specific table snapshot
* `as-of-timestamp` selects the current snapshot at a timestamp, in milliseconds

```scala
// time travel to October 26, 1986 at 01:21:00
spark.read
    .format("iceberg")
    .option("as-of-timestamp", "499162860000")
    .load("db.table")
```

```scala
// time travel to snapshot with ID 10963874102873L
spark.read
    .format("iceberg")
    .option("snapshot-id", 10963874102873L)
    .load("db.table")
```

### Querying with SQL

To run SQL `SELECT` statements on Iceberg tables in 2.4, register the DataFrame as a temporary table:

```scala
val df = spark.read.format("iceberg").load("db.table")
df.createOrReplaceTempView("table")

spark.sql("""select count(1) from table""").show()
```


### Appending data

To append a dataframe to an Iceberg table, use the `iceberg` format with `append` mode in the `DataFrameWriter`:

```scala
val data: DataFrame = ...
data.write
    .format("iceberg")
    .mode("append")
    .save("db.table")
```


### Overwriting data

To overwrite values in an Iceberg table, use `overwrite` mode in the `DataFrameWriter`:

```scala
val data: DataFrame = ...
data.write
    .format("iceberg")
    .mode("overwrite")
    .save("db.table")
```

!!! Warning
    **Spark does not define the behavior of DataFrame overwrite**. Like most sources, Iceberg will dynamically overwrite partitions when the dataframe contains rows in a partition. Unpartitioned tables are completely overwritten.


### Inspecting tables

To inspect a table's history, snapshots, and other metadata, Iceberg supports metadata tables.

Metadata tables are identified by adding the metadata table name after the original table name. For example, history for `db.table` is read using `db.table.history`.

#### History

To show table history, run:

```scala
spark.read.format("iceberg").load("db.table.history").show(truncate = false)
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

#### Snapshots

To show the valid snapshots for a table, run:

```scala
spark.read.format("iceberg").load("db.table.snapshots").show(truncate = false)
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

```scala
spark.read.format("iceberg").load("db.table.history").createOrReplaceTempView("history")
spark.read.format("iceberg").load("db.table.snapshots").createOrReplaceTempView("snapshots")
```
```sql
select
    h.made_current_at,
    s.operation,
    h.snapshot_id,
    h.is_current_ancestor,
    s.summary['spark.app.id']
from history h
join snapshots s
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

#### Manifests

To show a table's file manifests and each file's metadata, run:

```scala
spark.read.format("iceberg").load("db.table.manifests").show(truncate = false)
```
```text
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+---------------------------------+
| path                                                                 | length | partition_spec_id | added_snapshot_id   | added_data_files_count | existing_data_files_count | deleted_data_files_count | partitions                      |
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+---------------------------------+
| s3://.../table/metadata/45b5290b-ee61-4788-b324-b1e2735c0e10-m0.avro | 4479   | 0                 | 6668963634911763636 | 8                      | 0                         | 0                        | [[false,2019-05-13,2019-05-15]] |
+----------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+---------------------------------+
```

#### Files

To show a table's data files and each file's metadata, run:

```scala
spark.read.format("iceberg").load("db.table.files").show(truncate = false)
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
