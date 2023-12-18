---
title: "Queries"
url: spark-queries
aliases:
    - "spark/spark-queries"
menu:
    main:
        parent: Spark
        identifier: spark_queries
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

To use Iceberg in Spark, first configure [Spark catalogs](../spark-configuration). Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations.

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

Spark 3.3 and later supports time travel in SQL queries using `TIMESTAMP AS OF` or `VERSION AS OF` clauses.
The `VERSION AS OF` clause can contain a long snapshot ID or a string branch or tag name.

{{< hint info >}}
Note: If the name of a branch or tag is the same as a snapshot ID, then the snapshot which is selected for time travel is the snapshot
with the given snapshot ID. For example, consider the case where there is a tag named '1' and it references snapshot with ID 2. 
If the version travel clause is `VERSION AS OF '1'`, time travel will be done to the snapshot with ID 1. 
If this is not desired, rename the tag or branch with a well-defined prefix such as 'snapshot-1'.
{{< /hint >}}

```sql 
-- time travel to October 26, 1986 at 01:21:00
SELECT * FROM prod.db.table TIMESTAMP AS OF '1986-10-26 01:21:00';

-- time travel to snapshot with id 10963874102873L
SELECT * FROM prod.db.table VERSION AS OF 10963874102873;

-- time travel to the head snapshot of audit-branch
SELECT * FROM prod.db.table VERSION AS OF 'audit-branch';

-- time travel to the snapshot referenced by the tag historical-snapshot
SELECT * FROM prod.db.table VERSION AS OF 'historical-snapshot';
```

In addition, `FOR SYSTEM_TIME AS OF` and `FOR SYSTEM_VERSION AS OF` clauses are also supported:

```sql
SELECT * FROM prod.db.table FOR SYSTEM_TIME AS OF '1986-10-26 01:21:00';
SELECT * FROM prod.db.table FOR SYSTEM_VERSION AS OF 10963874102873;
SELECT * FROM prod.db.table FOR SYSTEM_VERSION AS OF 'audit-branch';
SELECT * FROM prod.db.table FOR SYSTEM_VERSION AS OF 'historical-snapshot';
```

Timestamps may also be supplied as a Unix timestamp, in seconds:

```sql
-- timestamp in seconds
SELECT * FROM prod.db.table TIMESTAMP AS OF 499162860;
SELECT * FROM prod.db.table FOR SYSTEM_TIME AS OF 499162860;
```

The branch or tag may also be specified using a similar syntax to metadata tables, with `branch_<branchname>` or `tag_<tagname>`:

```sql
SELECT * FROM prod.db.table.`branch_audit-branch`;
SELECT * FROM prod.db.table.`tag_historical-snapshot`;
```

(Identifiers with "-" are not valid, and so must be escaped using back quotes.)

Note that the identifier with branch or tag may not be used in combination with `VERSION AS OF`.

#### DataFrame

To select a specific table snapshot or the snapshot at some time in the DataFrame API, Iceberg supports four Spark read options:

* `snapshot-id` selects a specific table snapshot
* `as-of-timestamp` selects the current snapshot at a timestamp, in milliseconds
* `branch` selects the head snapshot of the specified branch. Note that currently branch cannot be combined with as-of-timestamp.
* `tag` selects the snapshot associated with the specified tag. Tags cannot be combined with `as-of-timestamp`.

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

```scala
// time travel to tag historical-snapshot
spark.read
    .option(SparkReadOptions.TAG, "historical-snapshot")
    .format("iceberg")
    .load("path/to/table")
```

```scala
// time travel to the head snapshot of audit-branch
spark.read
    .option(SparkReadOptions.BRANCH, "audit-branch")
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
spark.read
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

## Inspecting tables

To inspect a table's history, snapshots, and other metadata, Iceberg supports metadata tables.

Metadata tables are identified by adding the metadata table name after the original table name. For example, history for `db.table` is read using `db.table.history`.

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
order by made_current_at;
```

| made_current_at | operation | snapshot_id | is_current_ancestor | summary[spark.app.id] |
| -- | -- | -- | -- | -- |
| 2019-02-08 03:29:51.215 | append    | 57897183625154 | true                | application_1520379288616_155055 |
| 2019-02-09 16:24:30.13  | delete    | 29641004024753 | false               | application_1520379288616_151109 |
| 2019-02-09 16:32:47.336 | append    | 57897183625154 | true                | application_1520379288616_155055 |
| 2019-02-08 03:47:55.948 | overwrite | 51792995261850 | true                | application_1520379288616_152431 |

### Entries

To show all the table's current manifest entries for both data and delete files.

```sql
SELECT * FROM prod.db.table.entries;
```

| status | snapshot_id | sequence_number | file_sequence_number | data_file | readable_metrics |
| -- | -- | -- | -- | -- | -- |
| 2 | 57897183625154 | 0 | 0 | {"content":0,"file_path":"s3:/.../table/data/00047-25-833044d0-127b-415c-b874-038a4f978c29-00612.parquet","file_format":"PARQUET","spec_id":0,"record_count":15,"file_size_in_bytes":473,"column_sizes":{1:103},"value_counts":{1:15},"null_value_counts":{1:0},"nan_value_counts":{},"lower_bounds":{1:},"upper_bounds":{1:},"key_metadata":null,"split_offsets":[4],"equality_ids":null,"sort_order_id":0} | {"c1":{"column_size":103,"value_count":15,"null_value_count":0,"nan_value_count":null,"lower_bound":1,"upper_bound":3}} |

### Files

To show a table's current files:

```sql
SELECT * FROM prod.db.table.files;
```

| content | file_path | file_format | spec_id | record_count | file_size_in_bytes | column_sizes | value_counts | null_value_counts | nan_value_counts | lower_bounds | upper_bounds | key_metadata | split_offsets | equality_ids | sort_order_id | readable_metrics |
| -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- |
| 0 | s3:/.../table/data/00042-3-a9aa8b24-20bc-4d56-93b0-6b7675782bb5-00001.parquet | PARQUET | 0 | 1 | 652 | {1:52,2:48} | {1:1,2:1} | {1:0,2:0} | {} | {1:,2:d} | {1:,2:d} | NULL | [4] | NULL | 0 | {"data":{"column_size":48,"value_count":1,"null_value_count":0,"nan_value_count":null,"lower_bound":"d","upper_bound":"d"},"id":{"column_size":52,"value_count":1,"null_value_count":0,"nan_value_count":null,"lower_bound":1,"upper_bound":1}} |
| 0 | s3:/.../table/data/00000-0-f9709213-22ca-4196-8733-5cb15d2afeb9-00001.parquet | PARQUET | 0 | 1 | 643 | {1:46,2:48} | {1:1,2:1} | {1:0,2:0} | {} | {1:,2:a} | {1:,2:a} | NULL | [4] | NULL | 0 | {"data":{"column_size":48,"value_count":1,"null_value_count":0,"nan_value_count":null,"lower_bound":"a","upper_bound":"a"},"id":{"column_size":46,"value_count":1,"null_value_count":0,"nan_value_count":null,"lower_bound":1,"upper_bound":1}} | 
| 0 | s3:/.../table/data/00001-1-f9709213-22ca-4196-8733-5cb15d2afeb9-00001.parquet | PARQUET | 0 | 2 | 644 | {1:49,2:51} | {1:2,2:2} | {1:0,2:0} | {} | {1:,2:b} | {1:,2:c} | NULL | [4] | NULL | 0 | {"data":{"column_size":51,"value_count":2,"null_value_count":0,"nan_value_count":null,"lower_bound":"b","upper_bound":"c"},"id":{"column_size":49,"value_count":2,"null_value_count":0,"nan_value_count":null,"lower_bound":2,"upper_bound":3}} |
| 1 | s3:/.../table/data/00081-4-a9aa8b24-20bc-4d56-93b0-6b7675782bb5-00001-deletes.parquet | PARQUET | 0 | 1 | 1560 | {2147483545:46,2147483546:152} | {2147483545:1,2147483546:1} | {2147483545:0,2147483546:0} | {} | {2147483545:,2147483546:s3:/.../table/data/00000-0-f9709213-22ca-4196-8733-5cb15d2afeb9-00001.parquet} | {2147483545:,2147483546:s3:/.../table/data/00000-0-f9709213-22ca-4196-8733-5cb15d2afeb9-00001.parquet} | NULL | [4] | NULL | NULL | {"data":{"column_size":null,"value_count":null,"null_value_count":null,"nan_value_count":null,"lower_bound":null,"upper_bound":null},"id":{"column_size":null,"value_count":null,"null_value_count":null,"nan_value_count":null,"lower_bound":null,"upper_bound":null}} |
| 2 | s3:/.../table/data/00047-25-833044d0-127b-415c-b874-038a4f978c29-00612.parquet | PARQUET | 0 | 126506 | 28613985 | {100:135377,101:11314} | {100:126506,101:126506} | {100:105434,101:11} | {} | {100:0,101:17} | {100:404455227527,101:23} | NULL | NULL | [1] | 0 | {"id":{"column_size":135377,"value_count":126506,"null_value_count":105434,"nan_value_count":null,"lower_bound":0,"upper_bound":404455227527},"data":{"column_size":11314,"value_count":126506,"null_value_count": 11,"nan_value_count":null,"lower_bound":17,"upper_bound":23}} |

{{< hint info >}}
Content refers to type of content stored by the data file:
  0  Data
  1  Position Deletes
  2  Equality Deletes
{{< /hint >}}

To show only data files or delete files, query `prod.db.table.data_files` and `prod.db.table.delete_files` respectively.
To show all files, data files and delete files across all tracked snapshots, query `prod.db.table.all_files`, `prod.db.table.all_data_files` and `prod.db.table.all_delete_files` respectively.

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

| partition      | spec_id | record_count  | file_count | total_data_file_size_in_bytes | position_delete_record_count | position_delete_file_count | equality_delete_record_count | equality_delete_file_count | last_updated_at(μs) | last_updated_snapshot_id |
| -------------- |---------|---------------|------------|--------------------------|------------------------------|----------------------------|------------------------------|----------------------------|---------------------|--------------------------|
| {20211001, 11} | 0       | 1             | 1          | 100                      | 2                            | 1                          | 0                            | 0                          | 1633086034192000    | 9205185327307503337      |
| {20211002, 11} | 0       | 4             | 3          | 500                      | 1                            | 1                          | 0                            | 0                          | 1633172537358000    | 867027598972211003       |
| {20211001, 10} | 0       | 7             | 4          | 700                      | 0                            | 0                          | 0                            | 0                          | 1633082598716000    | 3280122546965981531      |
| {20211002, 10} | 0       | 3             | 2          | 400                      | 0                            | 0                          | 1                            | 1                          | 1633169159489000    | 6941468797545315876      |

Note:
1. For unpartitioned tables, the partitions table will not contain the partition and spec_id fields.

2. The partitions metadata table shows partitions with data files or delete files in the current snapshot. However, delete files are not applied, and so in some cases partitions may be shown even though all their data rows are marked deleted by delete files.

### Positional Delete Files

To show all positional delete files from the current snapshot of table:

```sql
SELECT * from prod.db.table.position_deletes;
```

| file_path | pos | row | spec_id | delete_file_path |
| -- | -- | -- | -- | -- |
| s3:/.../table/data/00042-3-a9aa8b24-20bc-4d56-93b0-6b7675782bb5-00001.parquet | 1 | 0 | 0 | s3:/.../table/data/00191-1933-25e9f2f3-d863-4a69-a5e1-f9aeeebe60bb-00001-deletes.parquet |

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

#### All Delete Files

To show the table's delete files and each file's metadata from all the snapshots:

```sql
SELECT * FROM prod.db.table.all_delete_files;
```

| content | file_path | file_format | spec_id | record_count | file_size_in_bytes | column_sizes | value_counts | null_value_counts | nan_value_counts | lower_bounds | upper_bounds | key_metadata | split_offsets | equality_ids | sort_order_id | readable_metrics |
| -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- |
| 1 | s3:/.../table/data/00081-4-a9aa8b24-20bc-4d56-93b0-6b7675782bb5-00001-deletes.parquet | PARQUET | 0 | 1 | 1560 | {2147483545:46,2147483546:152} | {2147483545:1,2147483546:1} | {2147483545:0,2147483546:0} | {} | {2147483545:,2147483546:s3:/.../table/data/00000-0-f9709213-22ca-4196-8733-5cb15d2afeb9-00001.parquet} | {2147483545:,2147483546:s3:/.../table/data/00000-0-f9709213-22ca-4196-8733-5cb15d2afeb9-00001.parquet} | NULL | [4] | NULL | NULL | {"data":{"column_size":null,"value_count":null,"null_value_count":null,"nan_value_count":null,"lower_bound":null,"upper_bound":null},"id":{"column_size":null,"value_count":null,"null_value_count":null,"nan_value_count":null,"lower_bound":null,"upper_bound":null}} |
| 2 | s3:/.../table/data/00047-25-833044d0-127b-415c-b874-038a4f978c29-00612.parquet | PARQUET | 0 | 126506 | 28613985 | {100:135377,101:11314} | {100:126506,101:126506} | {100:105434,101:11} | {} | {100:0,101:17} | {100:404455227527,101:23} | NULL | NULL | [1] | 0 | {"id":{"column_size":135377,"value_count":126506,"null_value_count":105434,"nan_value_count":null,"lower_bound":0,"upper_bound":404455227527},"data":{"column_size":11314,"value_count":126506,"null_value_count": 11,"nan_value_count":null,"lower_bound":17,"upper_bound":23}} |

#### All Entries

To show the table's manifest entries from all the snapshots for both data and delete files:

```sql
SELECT * FROM prod.db.table.all_entries;
```

| status | snapshot_id | sequence_number | file_sequence_number | data_file | readable_metrics |
| -- | -- | -- | -- | -- | -- |
| 2 | 57897183625154 | 0 | 0 | {"content":0,"file_path":"s3:/.../table/data/00047-25-833044d0-127b-415c-b874-038a4f978c29-00612.parquet","file_format":"PARQUET","spec_id":0,"record_count":15,"file_size_in_bytes":473,"column_sizes":{1:103},"value_counts":{1:15},"null_value_counts":{1:0},"nan_value_counts":{},"lower_bounds":{1:},"upper_bounds":{1:},"key_metadata":null,"split_offsets":[4],"equality_ids":null,"sort_order_id":0} | {"c1":{"column_size":103,"value_count":15,"null_value_count":0,"nan_value_count":null,"lower_bound":1,"upper_bound":3}} |

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

Metadata tables can be loaded using the DataFrameReader API:

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
