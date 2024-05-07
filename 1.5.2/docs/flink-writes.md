---
title: "Flink Writes"
search:
  exclude: true
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
# Flink Writes

Iceberg support batch and streaming writes With [Apache Flink](https://flink.apache.org/)'s DataStream API and Table API.

## Writing with SQL

Iceberg support both `INSERT INTO` and `INSERT OVERWRITE`.

### `INSERT INTO`

To append new data to a table with a Flink streaming job, use `INSERT INTO`:

```sql
INSERT INTO `hive_catalog`.`default`.`sample` VALUES (1, 'a');
INSERT INTO `hive_catalog`.`default`.`sample` SELECT id, data from other_kafka_table;
```

### `INSERT OVERWRITE`

To replace data in the table with the result of a query, use `INSERT OVERWRITE` in batch job (flink streaming job does not support `INSERT OVERWRITE`). Overwrites are atomic operations for Iceberg tables.

Partitions that have rows produced by the SELECT query will be replaced, for example:

```sql
INSERT OVERWRITE sample VALUES (1, 'a');
```

Iceberg also support overwriting given partitions by the `select` values:

```sql
INSERT OVERWRITE `hive_catalog`.`default`.`sample` PARTITION(data='a') SELECT 6;
```

For a partitioned iceberg table, when all the partition columns are set a value in `PARTITION` clause, it is inserting into a static partition, otherwise if partial partition columns (prefix part of all partition columns) are set a value in `PARTITION` clause, it is writing the query result into a dynamic partition.
For an unpartitioned iceberg table, its data will be completely overwritten by `INSERT OVERWRITE`.

### `UPSERT`

Iceberg supports `UPSERT` based on the primary key when writing data into v2 table format. There are two ways to enable upsert.

1. Enable the `UPSERT` mode as table-level property `write.upsert.enabled`. Here is an example SQL statement to set the table property when creating a table. It would be applied for all write paths to this table (batch or streaming) unless overwritten by write options as described later.

    ```sql
    CREATE TABLE `hive_catalog`.`default`.`sample` (
        `id` INT COMMENT 'unique id',
        `data` STRING NOT NULL,
        PRIMARY KEY(`id`) NOT ENFORCED
    ) with ('format-version'='2', 'write.upsert.enabled'='true');
    ```

2. Enabling `UPSERT` mode using `upsert-enabled` in the [write options](#write-options) provides more flexibility than a table level config. Note that you still need to use v2 table format and specify the [primary key](../flink-ddl.md/#primary-key) or [identifier fields](../../spec.md#identifier-field-ids) when creating the table.

    ```sql
    INSERT INTO tableName /*+ OPTIONS('upsert-enabled'='true') */
    ...
    ```

!!! info
    OVERWRITE and UPSERT can't be set together. In UPSERT mode, if the table is partitioned, the partition fields should be included in equality fields.




## Writing with DataStream

Iceberg support writing to iceberg table from different DataStream input.


### Appending data

Flink supports writing `DataStream<RowData>` and `DataStream<Row>` to the sink iceberg table natively.

```java
StreamExecutionEnvironment env = ...;

DataStream<RowData> input = ... ;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path", hadoopConf);

FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .append();

env.execute("Test Iceberg DataStream");
```

The iceberg API also allows users to write generic `DataStream<T>` to iceberg table, more example could be found in this [unit test](https://github.com/apache/iceberg/blob/main/flink/v1.16/flink/src/test/java/org/apache/iceberg/flink/sink/TestFlinkIcebergSink.java).

### Overwrite data

Set the `overwrite` flag in FlinkSink builder to overwrite the data in existing iceberg tables:

```java
StreamExecutionEnvironment env = ...;

DataStream<RowData> input = ... ;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path", hadoopConf);

FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .overwrite(true)
    .append();

env.execute("Test Iceberg DataStream");
```

### Upsert data

Set the `upsert` flag in FlinkSink builder to upsert the data in existing iceberg table. The table must use v2 table format and have a primary key.

```java
StreamExecutionEnvironment env = ...;

DataStream<RowData> input = ... ;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path", hadoopConf);

FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .upsert(true)
    .append();

env.execute("Test Iceberg DataStream");
```

!!! info
    OVERWRITE and UPSERT can't be set together. In UPSERT mode, if the table is partitioned, the partition fields should be included in equality fields.


### Write with Avro GenericRecord

Flink Iceberg sink provides `AvroGenericRecordToRowDataMapper` that converts
Avro `GenericRecord` to Flink `RowData`. You can use the mapper to write
Avro GenericRecord DataStream to Iceberg.

Please make sure `flink-avro` jar is included in the classpath.
Also `iceberg-flink-runtime` shaded bundle jar can't be used
because the runtime jar shades the avro package.
Please use non-shaded `iceberg-flink` jar instead.

```java
DataStream<org.apache.avro.generic.GenericRecord> dataStream = ...;

Schema icebergSchema = table.schema();


// The Avro schema converted from Iceberg schema can't be used
// due to precision difference between how Iceberg schema (micro)
// and Flink AvroToRowDataConverters (milli) deal with time type.
// Instead, use the Avro schema defined directly.
// See AvroGenericRecordToRowDataMapper Javadoc for more details.
org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(icebergSchema, table.name());

GenericRecordAvroTypeInfo avroTypeInfo = new GenericRecordAvroTypeInfo(avroSchema);
RowType rowType = FlinkSchemaUtil.convert(icebergSchema);

FlinkSink.builderFor(
    dataStream,
    AvroGenericRecordToRowDataMapper.forAvroSchema(avroSchema),
    FlinkCompatibilityUtil.toTypeInfo(rowType))
  .table(table)
  .tableLoader(tableLoader)
  .append();
```

### Branch Writes
Writing to branches in Iceberg tables is also supported via the `toBranch` API in `FlinkSink`
For more information on branches please refer to [branches](../branching.md).
```java
FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .toBranch("audit-branch")
    .append();
```

### Metrics

The following Flink metrics are provided by the Flink Iceberg sink.

Parallel writer metrics are added under the sub group of `IcebergStreamWriter`.
They should have the following key-value tags.

* table: full table name (like iceberg.my_db.my_table)
* subtask_index: writer subtask index starting from 0

 Metric name                | Metric type | Description                                                                                         |
| ------------------------- |------------|-----------------------------------------------------------------------------------------------------|
| lastFlushDurationMs       | Gauge      | The duration (in milli) that writer subtasks take to flush and upload the files during checkpoint.  |
| flushedDataFiles          | Counter    | Number of data files flushed and uploaded.                                                          |
| flushedDeleteFiles        | Counter    | Number of delete files flushed and uploaded.                                                        |
| flushedReferencedDataFiles| Counter    | Number of data files referenced by the flushed delete files.                                        |
| dataFilesSizeHistogram    | Histogram  | Histogram distribution of data file sizes (in bytes).                                               |
| deleteFilesSizeHistogram  | Histogram  | Histogram distribution of delete file sizes (in bytes).                                             |

Committer metrics are added under the sub group of `IcebergFilesCommitter`.
They should have the following key-value tags.

* table: full table name (like iceberg.my_db.my_table)

 Metric name                      | Metric type | Description                                                                |
|---------------------------------|--------|----------------------------------------------------------------------------|
| lastCheckpointDurationMs        | Gauge  | The duration (in milli) that the committer operator checkpoints its state. |
| lastCommitDurationMs            | Gauge  | The duration (in milli) that the Iceberg table commit takes.               |
| committedDataFilesCount         | Counter | Number of data files committed.                                            |
| committedDataFilesRecordCount   | Counter | Number of records contained in the committed data files.                   |
| committedDataFilesByteCount     | Counter | Number of bytes contained in the committed data files.                     |
| committedDeleteFilesCount       | Counter | Number of delete files committed.                                          |
| committedDeleteFilesRecordCount | Counter | Number of records contained in the committed delete files.                 |
| committedDeleteFilesByteCount   | Counter | Number of bytes contained in the committed delete files.                   |
| elapsedSecondsSinceLastSuccessfulCommit| Gauge  | Elapsed time (in seconds) since last successful Iceberg commit.            |

`elapsedSecondsSinceLastSuccessfulCommit` is an ideal alerting metric
to detect failed or missing Iceberg commits.

* Iceberg commit happened after successful Flink checkpoint in the `notifyCheckpointComplete` callback.
  It could happen that Iceberg commits failed (for whatever reason), while Flink checkpoints succeeding.
* It could also happen that `notifyCheckpointComplete` wasn't triggered (for whatever bug).
  As a result, there won't be any Iceberg commits attempted.

If the checkpoint interval (and expected Iceberg commit interval) is 5 minutes, set up alert with rule like `elapsedSecondsSinceLastSuccessfulCommit > 60 minutes` to detect failed or missing Iceberg commits in the past hour.



## Options

### Write options

Flink write options are passed when configuring the FlinkSink, like this:

```java
FlinkSink.Builder builder = FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
    .table(table)
    .tableLoader(tableLoader)
    .set("write-format", "orc")
    .set(FlinkWriteOptions.OVERWRITE_MODE, "true");
```

For Flink SQL, write options can be passed in via SQL hints like this:

```sql
INSERT INTO tableName /*+ OPTIONS('upsert-enabled'='true') */
...
```

Check out all the options here: [write-options](../flink-configuration.md#write-options)

## Notes

Flink streaming write jobs rely on snapshot summary to keep the last committed checkpoint ID, and
store uncommitted data as temporary files. Therefore, [expiring snapshots](../maintenance.md#expire-snapshots)
and [deleting orphan files](../maintenance.md#delete-orphan-files) could possibly corrupt
the state of the Flink job. To avoid that, make sure to keep the last snapshot created by the Flink
job (which can be identified by the `flink.job-id` property in the summary), and only delete
orphan files that are old enough.
