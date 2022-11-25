---
title: "Queries"
url: spark-queries
aliases:
    - "spark/spark-queries"
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

# Spark Queries

To use Iceberg in Spark, first configure [Spark catalogs](../spark-configuration).

Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations. Spark DSv2 is an evolving API with different levels of support in Spark versions:

| Feature support                                  | Spark 3 | Spark 2.4  | Notes                                          |
|--------------------------------------------------|-----------|------------|------------------------------------------------|
| [`SELECT`](#querying-with-sql)                   | ✔️        |            |                                                |
| [DataFrame reads](#querying-with-dataframes)     | ✔️        | ✔️          |                                                |
| [Metadata table `SELECT`](#inspecting-tables)    | ✔️        |            |                                                |
| [History metadata table](#history)               | ✔️        | ✔️          |                                                |
| [Snapshots metadata table](#snapshots)           | ✔️        | ✔️          |                                                |
| [Files metadata table](#files)                   | ✔️        | ✔️          |                                                |
| [Manifests metadata table](#manifests)           | ✔️        | ✔️          |                                                |
| [Partitions metadata table](#partitions)         | ✔️        | ✔️          |                                                |
| [All metadata tables](#all-metadata-tables)      | ✔️        | ✔️          |                                                |


## Querying with SQL

In Spark 3, tables use identifiers that include a [catalog name](../spark-configuration#using-catalogs).

```sql
SELECT * FROM prod.db.table; -- catalog: prod, namespace: db, table: table
```

Metadata tables, like `history` and `snapshots`, can use the Iceberg table name as a namespace.

For example, to read from the `files` metadata table for `prod.db.table`:

```sql
SELECT * FROM prod.db.table.files;
```
|content|file_path                                                                                                                                   |file_format|spec_id|partition|record_count|file_size_in_bytes|column_sizes      |value_counts    |null_value_counts|nan_value_counts|lower_bounds           |upper_bounds           |key_metadata|split_offsets|equality_ids|sort_order_id|
| -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- |
| 0 | s3:/.../table/data/00000-3-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET   | 0  | {1999-01-01, 01} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> c] | [1 -> , 2 -> c] | null         | [4]           | null | null |
| 0 | s3:/.../table/data/00001-4-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET   | 0  | {1999-01-01, 02} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> b] | [1 -> , 2 -> b] | null         | [4]           | null | null |
| 0 | s3:/.../table/data/00002-5-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET   | 0  | {1999-01-01, 03} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> a] | [1 -> , 2 -> a] | null         | [4]           | null | null |

## Querying with DataFrames

To load a table as a DataFrame, use `table`:

```scala
val df = spark.table("prod.db.table")
```

### Catalogs with DataFrameReader

Iceberg 0.11.0 adds multi-catalog support to `DataFrameReader` in both Spark 3 and 2.4.

Paths and table names can be loaded with Spark's `DataFrameReader` interface. How tables are loaded depends on how
the identifier is specified. When using `spark.read.format("iceberg").load(table)` or `spark.table(table)` the `table`
variable can take a number of forms as listed below:

*  `file:///path/to/table`: loads a HadoopTable at given path
*  `tablename`: loads `currentCatalog.currentNamespace.tablename`
*  `catalog.tablename`: loads `tablename` from the specified catalog.
*  `namespace.tablename`: loads `namespace.tablename` from current catalog
*  `catalog.namespace.tablename`: loads `namespace.tablename` from the specified catalog.
*  `namespace1.namespace2.tablename`: loads `namespace1.namespace2.tablename` from current catalog

The above list is in order of priority. For example: a matching catalog will take priority over any namespace resolution.


### Time travel

#### SQL

Spark 3.3 and later supports time travel in SQL queries using `TIMESTAMP AS OF` or `VERSION AS OF` clauses

```sql 
-- time travel to October 26, 1986 at 01:21:00
SELECT * FROM prod.db.table TIMESTAMP AS OF '1986-10-26 01:21:00';

-- time travel to snapshot with id 10963874102873L
SELECT * FROM prod.db.table VERSION AS OF 10963874102873;
```

In addition, `FOR SYSTEM_TIME AS OF` and `FOR SYSTEM_VERSION AS OF` clauses are also supported:

```sql
SELECT * FROM prod.db.table FOR SYSTEM_TIME AS OF '1986-10-26 01:21:00';
SELECT * FROM prod.db.table FOR SYSTEM_VERSION AS OF 10963874102873;
```

Timestamps may also be supplied as a Unix timestamp, in seconds:

```sql
-- timestamp in seconds
SELECT * FROM prod.db.table TIMESTAMP AS OF 499162860;
SELECT * FROM prod.db.table FOR SYSTEM_TIME AS OF 499162860;
```

#### DataFrame

To select a specific table snapshot or the snapshot at some time in the DataFrame API, Iceberg supports two Spark read options:

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

{{< hint info >}}
Spark 3.0 and earlier versions do not support using `option` with `table` in DataFrameReader commands. All options will be silently 
ignored. Do not use `table` when attempting to time-travel or use other options. See [SPARK-32592](https://issues.apache.org/jira/browse/SPARK-32592).
{{< /hint >}}


### Incremental read

To read appended data incrementally, use:

* `start-snapshot-id` Start snapshot ID used in incremental scans (exclusive).
* `end-snapshot-id` End snapshot ID used in incremental scans (inclusive). This is optional. Omitting it will default to the current snapshot.

```scala
// get the data added after start-snapshot-id (10963874102873L) until end-snapshot-id (63874143573109L)
spark.read()
  .format("iceberg")
  .option("start-snapshot-id", "10963874102873")
  .option("end-snapshot-id", "63874143573109")
  .load("path/to/table")
```

{{< hint info >}}
Currently gets only the data from `append` operation. Cannot support `replace`, `overwrite`, `delete` operations.
Incremental read works with both V1 and V2 format-version.
Incremental read is not supported by Spark's SQL syntax.
{{< /hint >}}

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

{{< hint info >}}
For Spark 2.4, use the `DataFrameReader` API to [inspect tables](#inspecting-with-dataframes).

For Spark 3, prior to 3.2, the Spark [session catalog](../spark-configuration#replacing-the-session-catalog) does not support table names with multipart identifiers such as `catalog.database.table.metadata`. As a workaround, configure an `org.apache.iceberg.spark.SparkCatalog`, or use the Spark `DataFrameReader` API.
{{< /hint >}}

### History

To show table history:

```sql
SELECT * FROM prod.db.table.history;
```

| made_current_at | snapshot_id  | parent_id | is_current_ancestor |
| -- | -- | -- | -- |
| 2019-02-08 03:29:51.215 | 5781947118336215154 | NULL                | true                |
| 2019-02-08 03:47:55.948 | 5179299526185056830 | 5781947118336215154 | true                |
| 2019-02-09 16:24:30.13  | 296410040247533544  | 5179299526185056830 | false               |
| 2019-02-09 16:32:47.336 | 2999875608062437330 | 5179299526185056830 | true                |
| 2019-02-09 19:42:03.919 | 8924558786060583479 | 2999875608062437330 | true                |
| 2019-02-09 19:49:16.343 | 6536733823181975045 | 8924558786060583479 | true                |

{{< hint info >}}
**This shows a commit that was rolled back.** The example has two snapshots with the same parent, and one is *not* an ancestor of the current table state.
{{< /hint >}}

### Metadata Log Entries

To show table metadata log entries:

```sql
SELECT * from prod.db.table.metadata_log_entries;
```

| timestamp | file | latest_snapshot_id | latest_schema_id | latest_sequence_number |
| -- | -- | -- | -- | -- |
| 2022-07-28 10:43:52.93 | s3://.../table/metadata/00000-9441e604-b3c2-498a-a45a-6320e8ab9006.metadata.json | null | null | null |
| 2022-07-28 10:43:57.487 | s3://.../table/metadata/00001-f30823df-b745-4a0a-b293-7532e0c99986.metadata.json | 170260833677645300 | 0 | 1 |
| 2022-07-28 10:43:58.25 | s3://.../table/metadata/00002-2cc2837a-02dc-4687-acc1-b4d86ea486f4.metadata.json | 958906493976709774 | 0 | 2 |

### Snapshots

To show the valid snapshots for a table:

```sql
SELECT * FROM prod.db.table.snapshots;
```

| committed_at | snapshot_id | parent_id | operation | manifest_list | summary |
| -- | -- | -- | -- | -- | -- |
| 2019-02-08 03:29:51.215 | 57897183625154 | null      | append    | s3://.../table/metadata/snap-57897183625154-1.avro | { added-records -> 2478404, total-records -> 2478404, added-data-files -> 438, total-data-files -> 438, spark.app.id -> application_1520379288616_155055 } |

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

| made_current_at | operation | snapshot_id | is_current_ancestor | summary[spark.app.id] |
| -- | -- | -- | -- | -- |
| 2019-02-08 03:29:51.215 | append    | 57897183625154 | true                | application_1520379288616_155055 |
| 2019-02-09 16:24:30.13  | delete    | 29641004024753 | false               | application_1520379288616_151109 |
| 2019-02-09 16:32:47.336 | append    | 57897183625154 | true                | application_1520379288616_155055 |
| 2019-02-08 03:47:55.948 | overwrite | 51792995261850 | true                | application_1520379288616_152431 |

### Files

To show a table's current data files:

```sql
SELECT * FROM prod.db.table.files;
```

|content|file_path                                                                                                                                   |file_format|spec_id|partition|record_count|file_size_in_bytes|column_sizes      |value_counts    |null_value_counts|nan_value_counts|lower_bounds           |upper_bounds           |key_metadata|split_offsets|equality_ids|sort_order_id|
| -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- |
| 0 | s3:/.../table/data/00000-3-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET   | 0  | {1999-01-01, 01} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> c] | [1 -> , 2 -> c] | null         | [4]           | null | null |
| 0 | s3:/.../table/data/00001-4-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET   | 0  | {1999-01-01, 02} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> b] | [1 -> , 2 -> b] | null         | [4]           | null | null |
| 0 | s3:/.../table/data/00002-5-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET   | 0  | {1999-01-01, 03} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> a] | [1 -> , 2 -> a] | null         | [4]           | null | null |

### Manifests

To show a table's current file manifests:

```sql
SELECT * FROM prod.db.table.manifests;
```

| path | length | partition_spec_id | added_snapshot_id | added_data_files_count | existing_data_files_count | deleted_data_files_count | partition_summaries |
| -- | -- | -- | -- | -- | -- | -- | -- |
| s3://.../table/metadata/45b5290b-ee61-4788-b324-b1e2735c0e10-m0.avro | 4479   | 0                 | 6668963634911763636 | 8                      | 0                         | 0                        | [[false,null,2019-05-13,2019-05-15]] |

Note:
1. Fields within `partition_summaries` column of the manifests table correspond to `field_summary` structs within [manifest list](../../../spec#manifest-lists), with the following order:
    - `contains_null`
    - `contains_nan`
    - `lower_bound`
    - `upper_bound`
2. `contains_nan` could return null, which indicates that this information is not available from the file's metadata.
    This usually occurs when reading from V1 table, where `contains_nan` is not populated.

### Partitions

To show a table's current partitions:

```sql
SELECT * FROM prod.db.table.partitions;
```

| partition | record_count | file_count | spec_id |
| -- | -- | -- | -- |
|  {20211001, 11}|           1|         1|         0|
|  {20211002, 11}|           1|         1|         0|
|  {20211001, 10}|           1|         1|         0|
|  {20211002, 10}|           1|         1|         0|

Note:
For unpartitioned tables, the partitions table will contain only the record_count and file_count columns.

### All Metadata Tables

These tables are unions of the metadata tables specific to the current snapshot, and return metadata across all snapshots.

{{< hint danger >}}
The "all" metadata tables may produce more than one row per data file or manifest file because metadata files may be part of more than one table snapshot.
{{< /hint >}}

#### All Data Files

To show all of the table's data files and each file's metadata:

```sql
SELECT * FROM prod.db.table.all_data_files;
```

| content | file_path | file_format | partition | record_count | file_size_in_bytes | column_sizes| value_counts | null_value_counts | nan_value_counts| lower_bounds| upper_bounds|key_metadata|split_offsets|equality_ids|sort_order_id|
| -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- |
|      0|s3://.../dt=20210102/00000-0-756e2512-49ae-45bb-aae3-c0ca475e7879-00001.parquet|    PARQUET|{20210102}|          14|              2444|{1 -> 94, 2 -> 17}|{1 -> 14, 2 -> 14}|  {1 -> 0, 2 -> 0}|              {}|{1 -> 1, 2 -> 20210102}|{1 -> 2, 2 -> 20210102}|        null|          [4]|        null|            0|
|      0|s3://.../dt=20210103/00000-0-26222098-032f-472b-8ea5-651a55b21210-00001.parquet|    PARQUET|{20210103}|          14|              2444|{1 -> 94, 2 -> 17}|{1 -> 14, 2 -> 14}|  {1 -> 0, 2 -> 0}|              {}|{1 -> 1, 2 -> 20210103}|{1 -> 3, 2 -> 20210103}|        null|          [4]|        null|            0|
|      0|s3://.../dt=20210104/00000-0-a3bb1927-88eb-4f1c-bc6e-19076b0d952e-00001.parquet|    PARQUET|{20210104}|          14|              2444|{1 -> 94, 2 -> 17}|{1 -> 14, 2 -> 14}|  {1 -> 0, 2 -> 0}|              {}|{1 -> 1, 2 -> 20210104}|{1 -> 3, 2 -> 20210104}|        null|          [4]|        null|            0|

#### All Manifests

To show all of the table's manifest files:

```sql
SELECT * FROM prod.db.table.all_manifests;
```

| path | length | partition_spec_id | added_snapshot_id | added_data_files_count | existing_data_files_count | deleted_data_files_count| partition_summaries|
| -- | -- | -- | -- | -- | -- | -- | -- |
| s3://.../metadata/a85f78c5-3222-4b37-b7e4-faf944425d48-m0.avro | 6376 | 0 | 6272782676904868561 | 2 | 0 | 0 |[{false, false, 20210101, 20210101}]|

Note:
1. Fields within `partition_summaries` column of the manifests table correspond to `field_summary` structs within [manifest list](../../../spec#manifest-lists), with the following order:
    - `contains_null`
    - `contains_nan`
    - `lower_bound`
    - `upper_bound`
2. `contains_nan` could return null, which indicates that this information is not available from the file's metadata.
    This usually occurs when reading from V1 table, where `contains_nan` is not populated.

### References

To show a table's known snapshot references:

```sql
SELECT * FROM prod.db.table.refs;
```

| name | type | snapshot_id | max_reference_age_in_ms | min_snapshots_to_keep | max_snapshot_age_in_ms | 
| -- | -- | -- | -- | -- | -- |
| main | BRANCH | 4686954189838128572 | 10 | 20 | 30 |
| testTag | TAG | 4686954189838128572 | 10 | null | null |

### Inspecting with DataFrames

Metadata tables can be loaded in Spark 2.4 or Spark 3 using the DataFrameReader API:

```scala
// named metastore table
spark.read.format("iceberg").load("db.table.files")
// Hadoop path table
spark.read.format("iceberg").load("hdfs://nn:8020/path/to/table#files")
```

### Time Travel with Metadata Tables

To inspect a tables's metadata with the time travel feature:

```sql
-- get the table's file manifests at timestamp Sep 20, 2021 08:00:00
SELECT * FROM prod.db.table.manifests TIMESTAMP AS OF '2021-09-20 08:00:00';

-- get the table's partitions with snapshot id 10963874102873L
SELECT * FROM prod.db.table.partitions VERSION AS OF 10963874102873;
```

Metadata tables can also be inspected with time travel using the DataFrameReader API:

```scala
// load the table's file metadata at snapshot-id 10963874102873 as DataFrame
spark.read.format("iceberg").option("snapshot-id", 10963874102873L).load("db.table.files")
```
