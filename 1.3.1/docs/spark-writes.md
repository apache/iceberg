---
title: "Writes"
url: spark-writes
aliases:
    - "spark/spark-writes"
menu:
    main:
        parent: Spark
        identifier: spark_writes
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

# Spark Writes

To use Iceberg in Spark, first configure [Spark catalogs](../spark-configuration).

Some plans are only available when using [Iceberg SQL extensions](../spark-configuration#sql-extensions) in Spark 3.

Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations. Spark DSv2 is an evolving API with different levels of support in Spark versions:

| Feature support                                  | Spark 3 | Notes                                        |
|--------------------------------------------------|-----------|----------------------------------------------|
| [SQL insert into](#insert-into)                  | ✔️        |                                              |
| [SQL merge into](#merge-into)                    | ✔️        | ⚠ Requires Iceberg Spark extensions          |
| [SQL insert overwrite](#insert-overwrite)        | ✔️        |                                              |
| [SQL delete from](#delete-from)                  | ✔️        | ⚠ Row-level delete requires Spark extensions |
| [SQL update](#update)                            | ✔️        | ⚠ Requires Iceberg Spark extensions          |
| [DataFrame append](#appending-data)              | ✔️        |                                              |
| [DataFrame overwrite](#overwriting-data)         | ✔️        |                                              |
| [DataFrame CTAS and RTAS](#creating-tables)      | ✔️        |                                              |


## Writing with SQL

Spark 3 supports SQL `INSERT INTO`, `MERGE INTO`, and `INSERT OVERWRITE`, as well as the new `DataFrameWriterV2` API.

### `INSERT INTO`

To append new data to a table, use `INSERT INTO`.

```sql
INSERT INTO prod.db.table VALUES (1, 'a'), (2, 'b')
```
```sql
INSERT INTO prod.db.table SELECT ...
```

### `MERGE INTO`

Spark 3 added support for `MERGE INTO` queries that can express row-level updates.

Iceberg supports `MERGE INTO` by rewriting data files that contain rows that need to be updated in an `overwrite` commit.

**`MERGE INTO` is recommended instead of `INSERT OVERWRITE`** because Iceberg can replace only the affected data files, and because the data overwritten by a dynamic overwrite may change if the table's partitioning changes.


#### `MERGE INTO` syntax

`MERGE INTO` updates a table, called the _target_ table, using a set of updates from another query, called the _source_. The update for a row in the target table is found using the `ON` clause that is like a join condition.

```sql
MERGE INTO prod.db.target t   -- a target table
USING (SELECT ...) s          -- the source updates
ON t.id = s.id                -- condition to find updates for target rows
WHEN ...                      -- updates
```

Updates to rows in the target table are listed using `WHEN MATCHED ... THEN ...`. Multiple `MATCHED` clauses can be added with conditions that determine when each match should be applied. The first matching expression is used.

```sql
WHEN MATCHED AND s.op = 'delete' THEN DELETE
WHEN MATCHED AND t.count IS NULL AND s.op = 'increment' THEN UPDATE SET t.count = 0
WHEN MATCHED AND s.op = 'increment' THEN UPDATE SET t.count = t.count + 1
```

Source rows (updates) that do not match can be inserted:

```sql
WHEN NOT MATCHED THEN INSERT *
```

Inserts also support additional conditions:

```sql
WHEN NOT MATCHED AND s.event_time > still_valid_threshold THEN INSERT (id, count) VALUES (s.id, 1)
```

Only one record in the source data can update any given row of the target table, or else an error will be thrown.


### `INSERT OVERWRITE`

`INSERT OVERWRITE` can replace data in the table with the result of a query. Overwrites are atomic operations for Iceberg tables.

The partitions that will be replaced by `INSERT OVERWRITE` depends on Spark's partition overwrite mode and the partitioning of a table. `MERGE INTO` can rewrite only affected data files and has more easily understood behavior, so it is recommended instead of `INSERT OVERWRITE`.

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

Delete queries accept a filter to match rows to delete.

```sql
DELETE FROM prod.db.table
WHERE ts >= '2020-05-01 00:00:00' and ts < '2020-06-01 00:00:00'

DELETE FROM prod.db.all_events
WHERE session_time < (SELECT min(session_time) FROM prod.db.good_events)

DELETE FROM prod.db.orders AS t1
WHERE EXISTS (SELECT oid FROM prod.db.returned_orders WHERE t1.oid = oid)
```

If the delete filter matches entire partitions of the table, Iceberg will perform a metadata-only delete. If the filter matches individual rows of a table, then Iceberg will rewrite only the affected data files.

### `UPDATE`

Spark 3.1 added support for `UPDATE` queries that update matching rows in tables.

Update queries accept a filter to match rows to update.

```sql
UPDATE prod.db.table
SET c1 = 'update_c1', c2 = 'update_c2'
WHERE ts >= '2020-05-01 00:00:00' and ts < '2020-06-01 00:00:00'

UPDATE prod.db.all_events
SET session_time = 0, ignored = true
WHERE session_time < (SELECT min(session_time) FROM prod.db.good_events)

UPDATE prod.db.orders AS t1
SET order_status = 'returned'
WHERE EXISTS (SELECT oid FROM prod.db.returned_orders WHERE t1.oid = oid)
```

For more complex row-level updates based on incoming data, see the section on `MERGE INTO`.

## Writing to Branches
Branch writes can be performed via SQL by providing a branch identifier, `branch_yourBranch` in the operation.
Branch writes can also be performed as part of a write-audit-publish (WAP) workflow by specifying the `spark.wap.branch` config.
Note WAP branch and branch identifier cannot both be specified.
Also, the branch must exist before performing the write. 
The operation does **not** create the branch if it does not exist. 
For more information on branches please refer to [branches](../../tables/branching)
 
```sql
-- INSERT (1,' a') (2, 'b') into the audit branch.
INSERT INTO prod.db.table.branch_audit VALUES (1, 'a'), (2, 'b');

-- MERGE INTO audit branch
MERGE INTO prod.db.table.branch_audit t 
USING (SELECT ...) s        
ON t.id = s.id          
WHEN ...

-- UPDATE audit branch
UPDATE prod.db.table.branch_audit AS t1
SET val = 'c'

-- DELETE FROM audit branch
DELETE FROM prod.dbl.table.branch_audit WHERE id = 2;

-- WAP Branch write
SET spark.wap.branch = audit-branch
INSERT INTO prod.db.table VALUES (3, 'c');
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

{{< hint danger >}}
When writing with the v1 DataFrame API in Spark 3, use `saveAsTable` or `insertInto` to load tables with a catalog.
Using `format("iceberg")` loads an isolated table reference that will not automatically refresh tables used by queries.
{{< /hint >}}


### Appending data

To append a dataframe to an Iceberg table, use `append`:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").append()
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

### Creating tables

To run a CTAS or RTAS, use `create`, `replace`, or `createOrReplace` operations:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").create()
```

If you have replaced the default Spark catalog (`spark_catalog`) with Iceberg's `SparkSessionCatalog`, do:

```scala
val data: DataFrame = ...
data.writeTo("db.table").using("iceberg").create()
```

Create and replace operations support table configuration methods, like `partitionedBy` and `tableProperty`:

```scala
data.writeTo("prod.db.table")
    .tableProperty("write.format.default", "orc")
    .partitionedBy($"level", days($"ts"))
    .createOrReplace()
```

The Iceberg table location can also be specified by the `location` table property:

```scala
data.writeTo("prod.db.table")
    .tableProperty("location", "/path/to/location")
    .createOrReplace()
```

## Writing Distribution Modes

Iceberg's default Spark writers require that the data in each spark task is clustered by partition values. This 
distribution is required to minimize the number of file handles that are held open while writing. By default, starting
in Iceberg 1.2.0, Iceberg also requests that Spark pre-sort data to be written to fit this distribution. The
request to Spark is done through the table property `write.distribution-mode` with the value `hash`.

Let's go through writing the data against below sample table:

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (days(ts), category)
```

To write data to the sample table, data needs to be sorted by `days(ts), category` but this is taken care
of automatically by the default `hash` distribution. Previously this would have required manually sorting, but this 
is no longer the case.

```sql
INSERT INTO prod.db.sample
SELECT id, data, category, ts FROM another_table
```


There are 3 options for `write.distribution-mode`

* `none` - This is the previous default for Iceberg.  
This mode does not request any shuffles or sort to be performed automatically by Spark. Because no work is done 
automatically by Spark, the data must be *manually* sorted by partition value. The data must be sorted either within 
each spark task, or globally within the entire dataset. A global sort will minimize the number of output files.  
A sort can be avoided by using the Spark [write fanout](#write-properties) property but this will cause all 
file handles to remain open until each write task has completed.
* `hash` - This mode is the new default and requests that Spark uses a hash-based exchange to shuffle the incoming
write data before writing.  
Practically, this means that each row is hashed based on the row's partition value and then placed
in a corresponding Spark task based upon that value. Further division and coalescing of tasks may take place because of
[Spark's Adaptive Query planning](#controlling-file-sizes).
* `range` - This mode requests that Spark perform a range based exchanged to shuffle the data before writing.  
This is a two stage procedure which is more expensive than the `hash` mode. The first stage samples the data to 
be written based on the partition and sort columns. The second stage uses the range information to shuffle the input data into Spark 
tasks. Each task gets an exclusive range of the input data which clusters the data by partition and also globally sorts.  
While this is more expensive than the hash distribution, the global ordering can be beneficial for read performance if
sorted columns are used during queries. This mode is used by default if a table is created with a 
sort-order. Further division and coalescing of tasks may take place because of
[Spark's Adaptive Query planning](#controlling-file-sizes).


## Controlling File Sizes

When writing data to Iceberg with Spark, it's important to note that Spark cannot write a file larger than a Spark 
task and a file cannot span an Iceberg partition boundary. This means although Iceberg will always roll over a file 
when it grows to [`write.target-file-size-bytes`](../configuration/#write-properties), but unless the Spark task is 
large enough that will not happen. The size of the file created on disk will also be much smaller than the Spark task 
since the on disk data will be both compressed and in columnar format as opposed to Spark's uncompressed row 
representation. This means a 100 megabyte Spark task will create a file much smaller than 100 megabytes even if that
task is writing to a single Iceberg partition. If the task writes to multiple partitions, the files will be even
smaller than that.

To control what data ends up in each Spark task use a [`write distribution mode`](#writing-distribution-modes) 
or manually repartition the data. 

To adjust Spark's task size it is important to become familiar with Spark's various Adaptive Query Execution (AQE) 
parameters. When the `write.distribution-mode` is not `none`, AQE will control the coalescing and splitting of Spark
tasks during the exchange to try to create tasks of `spark.sql.adaptive.advisoryPartitionSizeInBytes` size. These 
settings will also affect any user performed re-partitions or sorts. 
It is important again to note that this is the in-memory Spark row size and not the on disk
columnar-compressed size, so a larger value than the target file size will need to be specified. The ratio of 
in-memory size to on disk size is data dependent. Future work in Spark should allow Iceberg to automatically adjust this
parameter at write time to match the `write.target-file-size-bytes`.

## Type compatibility

Spark and Iceberg support different set of types. Iceberg does the type conversion automatically, but not for all combinations,
so you may want to understand the type conversion in Iceberg in prior to design the types of columns in your tables.

### Spark type to Iceberg type

This type conversion table describes how Spark types are converted to the Iceberg types. The conversion applies on both creating Iceberg table and writing to Iceberg table via Spark.

| Spark           | Iceberg                 | Notes |
|-----------------|-------------------------|-------|
| boolean         | boolean                 |       |
| short           | integer                 |       |
| byte            | integer                 |       |
| integer         | integer                 |       |
| long            | long                    |       |
| float           | float                   |       |
| double          | double                  |       |
| date            | date                    |       |
| timestamp       | timestamp with timezone |       |
| char            | string                  |       |
| varchar         | string                  |       |
| string          | string                  |       |
| binary          | binary                  |       |
| decimal         | decimal                 |       |
| struct          | struct                  |       |
| array           | list                    |       |
| map             | map                     |       |

{{< hint info >}}
The table is based on representing conversion during creating table. In fact, broader supports are applied on write. Here're some points on write:

* Iceberg numeric types (`integer`, `long`, `float`, `double`, `decimal`) support promotion during writes. e.g. You can write Spark types `short`, `byte`, `integer`, `long` to Iceberg type `long`.
* You can write to Iceberg `fixed` type using Spark `binary` type. Note that assertion on the length will be performed.
{{< /hint >}}

### Iceberg type to Spark type

This type conversion table describes how Iceberg types are converted to the Spark types. The conversion applies on reading from Iceberg table via Spark.

| Iceberg                    | Spark                   | Note          |
|----------------------------|-------------------------|---------------|
| boolean                    | boolean                 |               |
| integer                    | integer                 |               |
| long                       | long                    |               |
| float                      | float                   |               |
| double                     | double                  |               |
| date                       | date                    |               |
| time                       |                         | Not supported |
| timestamp with timezone    | timestamp               |               |
| timestamp without timezone |                         | Not supported |
| string                     | string                  |               |
| uuid                       | string                  |               |
| fixed                      | binary                  |               |
| binary                     | binary                  |               |
| decimal                    | decimal                 |               |
| struct                     | struct                  |               |
| list                       | array                   |               |
| map                        | map                     |               |

