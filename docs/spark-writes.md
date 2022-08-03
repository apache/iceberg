---
title: "Writes"
url: spark-writes
aliases:
    - "spark/spark-writes"
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

# Spark Writes

To use Iceberg in Spark, first configure [Spark catalogs](../spark-configuration).

Some plans are only available when using [Iceberg SQL extensions](../spark-configuration#sql-extensions) in Spark 3.x.

Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations. Spark DSv2 is an evolving API with different levels of support in Spark versions:

| Feature support                                  | Spark 3.0| Spark 2.4  | Notes                                          |
|--------------------------------------------------|----------|------------|------------------------------------------------|
| [SQL insert into](#insert-into)                  | ✔️        |            |                                                |
| [SQL merge into](#merge-into)                    | ✔️        |            | ⚠ Requires Iceberg Spark extensions            |
| [SQL insert overwrite](#insert-overwrite)        | ✔️        |            |                                                |
| [SQL delete from](#delete-from)                  | ✔️        |            | ⚠ Row-level delete requires Spark extensions   |
| [SQL update](#update)                            | ✔️        |            | ⚠ Requires Iceberg Spark extensions            |
| [DataFrame append](#appending-data)              | ✔️        | ✔️          |                                                |
| [DataFrame overwrite](#overwriting-data)         | ✔️        | ✔️          | ⚠ Behavior changed in Spark 3.0                |
| [DataFrame CTAS and RTAS](#creating-tables)      | ✔️        |            |                                                |


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

{{< hint danger >}}
Spark 3.0.0 has a correctness bug that affects dynamic `INSERT OVERWRITE` with hidden partitioning, [SPARK-32168][spark-32168].
For tables with [hidden partitions](../partitioning/#icebergs-hidden-partitioning), make sure you use Spark 3.0.1.
{{< /hint >}}

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

{{< hint danger >}}
**The behavior of overwrite mode changed between Spark 2.4 and Spark 3**.
{{< /hint >}}

The behavior of DataFrameWriter overwrite mode was undefined in Spark 2.4, but is required to overwrite the entire table in Spark 3. Because of this new requirement, the Iceberg source's behavior changed in Spark 3. In Spark 2.4, the behavior was to dynamically overwrite partitions. To use the Spark 2.4 behavior, add option `overwrite-mode=dynamic`.

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

## Writing to partitioned tables

Iceberg requires the data to be sorted according to the partition spec per task (Spark partition) in prior to write
against partitioned table. This applies both Writing with SQL and Writing with DataFrames.

{{< hint info >}}
Explicit sort is necessary because Spark doesn't allow Iceberg to request a sort before writing as of Spark 3.0.
[SPARK-23889](https://issues.apache.org/jira/browse/SPARK-23889) is filed to enable Iceberg to require specific
distribution & sort order to Spark.
{{< /hint >}}

{{< hint info >}}
Both global sort (`orderBy`/`sort`) and local sort (`sortWithinPartitions`) work for the requirement.
{{< /hint >}}

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

To write data to the sample table, your data needs to be sorted by `days(ts), category`.

If you're inserting data with SQL statement, you can use `ORDER BY` to achieve it, like below:

```sql
INSERT INTO prod.db.sample
SELECT id, data, category, ts FROM another_table
ORDER BY ts, category
```

If you're inserting data with DataFrame, you can use either `orderBy`/`sort` to trigger global sort, or `sortWithinPartitions`
to trigger local sort. Local sort for example:

```scala
data.sortWithinPartitions("ts", "category")
    .writeTo("prod.db.sample")
    .append()
```

You can simply add the original column to the sort condition for the most partition transformations, except `bucket`.

For `bucket` partition transformation, you need to register the Iceberg transform function in Spark to specify it during sort.

Let's go through another sample table having bucket partition:

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (bucket(16, id))
```

You need to register the function to deal with bucket, like below:

```scala
import org.apache.iceberg.spark.IcebergSpark
import org.apache.spark.sql.types.DataTypes

IcebergSpark.registerBucketUDF(spark, "iceberg_bucket16", DataTypes.LongType, 16)
```

{{< hint info >}}
Explicit registration of the function is necessary because Spark doesn't allow Iceberg to provide functions.
[SPARK-27658](https://issues.apache.org/jira/browse/SPARK-27658) is filed to enable Iceberg to provide functions
which can be used in query.
{{< /hint >}}

Here we just registered the bucket function as `iceberg_bucket16`, which can be used in sort clause.

If you're inserting data with SQL statement, you can use the function like below:

```sql
INSERT INTO prod.db.sample
SELECT id, data, category, ts FROM another_table
ORDER BY iceberg_bucket16(id)
```

If you're inserting data with DataFrame, you can use the function like below:

```scala
data.sortWithinPartitions(expr("iceberg_bucket16(id)"))
    .writeTo("prod.db.sample")
    .append()
```


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

