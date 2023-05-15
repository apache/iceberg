---
title: "Flink Queries"
url: flink-queries
aliases:
   - "flink/flink-queries"
menu:
   main:
      parent: Flink
      identifier: flink_queries
      weight: 300
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

# Flink Queries

Iceberg support streaming and batch read With [Apache Flink](https://flink.apache.org/)'s DataStream API and Table API.

## Reading with SQL

Iceberg support both streaming and batch read in Flink. Execute the following sql command to switch execution mode from `streaming` to `batch`, and vice versa:

```sql
-- Execute the flink job in streaming mode for current session context
SET execution.runtime-mode = streaming;

-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
```

### Flink batch read

Submit a Flink __batch__ job using the following sentences:

```sql
-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
SELECT * FROM sample;
```

### Flink streaming read

Iceberg supports processing incremental data in Flink streaming jobs which starts from a historical snapshot-id:

```sql
-- Submit the flink job in streaming mode for current session.
SET execution.runtime-mode = streaming;

-- Enable this switch because streaming read SQL will provide few job options in flink SQL hint options.
SET table.dynamic-table-options.enabled=true;

-- Read all the records from the iceberg current snapshot, and then read incremental data starting from that snapshot.
SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;

-- Read all incremental data starting from the snapshot-id '3821550127947089987' (records from this snapshot will be excluded).
SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-snapshot-id'='3821550127947089987')*/ ;
```

There are some options that could be set in Flink SQL hint options for streaming job, see [read options](#Read-options) for details.

### FLIP-27 source for SQL

Here are the SQL settings for the [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface) source. All other SQL settings and options documented above are applicable to the FLIP-27 source.

```sql
-- Opt in the FLIP-27 source. Default is false.
SET table.exec.iceberg.use-flip27-source = true;
```

### Reading branches and tags with SQL
Branch and tags can be read via SQL by specifying options. For more details
refer to [Flink Configuration](../flink-configuration/#read-options)

```sql
--- Read from branch b1
SELECT * FROM table /*+ OPTIONS('branch'='b1') */ ;

--- Read from tag t1
SELECT * FROM table /*+ OPTIONS('tag'='t1') */;

--- Incremental scan from tag t1 to tag t2
SELECT * FROM table /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-tag'='t1', 'end-tag'='t2') */;
```

## Reading with DataStream

Iceberg support streaming or batch read in Java API now.

### Batch Read

This example will read all records from iceberg table and then print to the stdout console in flink batch job:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");
DataStream<RowData> batch = FlinkSource.forRowData()
     .env(env)
     .tableLoader(tableLoader)
     .streaming(false)
     .build();

// Print all records to stdout.
batch.print();

// Submit and execute this batch read job.
env.execute("Test Iceberg Batch Read");
```

### Streaming read

This example will read incremental records which start from snapshot-id '3821550127947089987' and print to stdout console in flink streaming job:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");
DataStream<RowData> stream = FlinkSource.forRowData()
     .env(env)
     .tableLoader(tableLoader)
     .streaming(true)
     .startSnapshotId(3821550127947089987L)
     .build();

// Print all records to stdout.
stream.print();

// Submit and execute this streaming read job.
env.execute("Test Iceberg Streaming Read");
```

There are other options that can be set, please see the [FlinkSource#Builder](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/flink/source/FlinkSource.html).

## Reading with DataStream (FLIP-27 source)

[FLIP-27 source interface](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)
was introduced in Flink 1.12. It aims to solve several shortcomings of the old `SourceFunction`
streaming source interface. It also unifies the source interfaces for both batch and streaming executions.
Most source connectors (like Kafka, file) in Flink repo have  migrated to the FLIP-27 interface.
Flink is planning to deprecate the old `SourceFunction` interface in the near future.

A FLIP-27 based Flink `IcebergSource` is added in `iceberg-flink` module. The FLIP-27 `IcebergSource` is currently an experimental feature.

### Batch Read

This example will read all records from iceberg table and then print to the stdout console in flink batch job:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");

IcebergSource<RowData> source = IcebergSource.forRowData()
    .tableLoader(tableLoader)
    .assignerFactory(new SimpleSplitAssignerFactory())
    .build();

DataStream<RowData> batch = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "My Iceberg Source",
    TypeInformation.of(RowData.class));

// Print all records to stdout.
batch.print();

// Submit and execute this batch read job.
env.execute("Test Iceberg Batch Read");
```

### Streaming read

This example will start the streaming read from the latest table snapshot (inclusive).
Every 60s, it polls Iceberg table to discover new append-only snapshots.
CDC read is not supported yet.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");

IcebergSource source = IcebergSource.forRowData()
    .tableLoader(tableLoader)
    .assignerFactory(new SimpleSplitAssignerFactory())
    .streaming(true)
    .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
    .monitorInterval(Duration.ofSeconds(60))
    .build()

DataStream<RowData> stream = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "My Iceberg Source",
    TypeInformation.of(RowData.class));

// Print all records to stdout.
stream.print();

// Submit and execute this streaming read job.
env.execute("Test Iceberg Streaming Read");
```

There are other options that could be set by Java API, please see the
[IcebergSource#Builder](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/flink/source/IcebergSource.html).

### Reading branches and tags with DataStream
Branches and tags can also be read via the DataStream API

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");
// Read from branch
DataStream<RowData> batch = FlinkSource.forRowData()
    .env(env)
    .tableLoader(tableLoader)
    .branch("test-branch")
    .streaming(false)
    .build();

// Read from tag
DataStream<RowData> batch = FlinkSource.forRowData()
    .env(env)
    .tableLoader(tableLoader)
    .tag("test-tag")
    .streaming(false)
    .build();

// Streaming read from start-tag
DataStream<RowData> batch = FlinkSource.forRowData()
    .env(env)
    .tableLoader(tableLoader)
    .streaming(true)
    .startTag("test-tag")
    .build();
```

### Read as Avro GenericRecord

FLIP-27 Iceberg source provides `AvroGenericRecordReaderFunction` that converts
Flink `RowData` Avro `GenericRecord`. You can use the convert to read from
Iceberg table as Avro GenericRecord DataStream.

Please make sure `flink-avro` jar is included in the classpath.
Also `iceberg-flink-runtime` shaded bundle jar can't be used
because the runtime jar shades the avro package.
Please use non-shaded `iceberg-flink` jar instead.

```java
TableLoader tableLoader = ...;
Table table;
try (TableLoader loader = tableLoader) {
    loader.open();
    table = loader.loadTable();
}

AvroGenericRecordReaderFunction readerFunction = AvroGenericRecordReaderFunction.fromTable(table);

IcebergSource<GenericRecord> source =
    IcebergSource.<GenericRecord>builder()
        .tableLoader(tableLoader)
        .readerFunction(readerFunction)
        .assignerFactory(new SimpleSplitAssignerFactory())
        ...
        .build();

DataStream<Row> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
    "Iceberg Source as Avro GenericRecord", new GenericRecordAvroTypeInfo(avroSchema));
```

## Options

### Read options

Flink read options are passed when configuring the Flink IcebergSource:

```
IcebergSource.forRowData()
    .tableLoader(TableLoader.fromCatalog(...))
    .assignerFactory(new SimpleSplitAssignerFactory())
    .streaming(true)
    .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
    .startSnapshotId(3821550127947089987L)
    .monitorInterval(Duration.ofMillis(10L)) // or .set("monitor-interval", "10s") \ set(FlinkReadOptions.MONITOR_INTERVAL, "10s")
    .build()
```

For Flink SQL, read options can be passed in via SQL hints like this:

```
SELECT * FROM tableName /*+ OPTIONS('monitor-interval'='10s') */
...
```

Options can be passed in via Flink configuration, which will be applied to current session. Note that not all options support this mode.

```
env.getConfig()
    .getConfiguration()
    .set(FlinkReadOptions.SPLIT_FILE_OPEN_COST_OPTION, 1000L);
...
```

Check out all the options here: [read-options](/flink-configuration#read-options) 

## Inspecting tables

To inspect a table's history, snapshots, and other metadata, Iceberg supports metadata tables.

Metadata tables are identified by adding the metadata table name after the original table name. For example, history for `db.table` is read using `db.table$history`.

### History

To show table history:

```sql
SELECT * FROM prod.db.table$history;
```

| made_current_at         | snapshot_id         | parent_id           | is_current_ancestor |
| ----------------------- | ------------------- | ------------------- | ------------------- |
| 2019-02-08 03:29:51.215 | 5781947118336215154 | NULL                | true                |
| 2019-02-08 03:47:55.948 | 5179299526185056830 | 5781947118336215154 | true                |
| 2019-02-09 16:24:30.13  | 296410040247533544  | 5179299526185056830 | false               |
| 2019-02-09 16:32:47.336 | 2999875608062437330 | 5179299526185056830 | true                |
| 2019-02-09 19:42:03.919 | 8924558786060583479 | 2999875608062437330 | true                |
| 2019-02-09 19:49:16.343 | 6536733823181975045 | 8924558786060583479 | true                |

{{< hint info >}}
**This shows a commit that was rolled back.** In this example, snapshot 296410040247533544 and 2999875608062437330 have the same parent snapshot 5179299526185056830. Snapshot 296410040247533544 was rolled back and is *not* an ancestor of the current table state.
{{< /hint >}}

### Metadata Log Entries

To show table metadata log entries:

```sql
SELECT * from prod.db.table$metadata_log_entries;
```

| timestamp               | file                                                         | latest_snapshot_id | latest_schema_id | latest_sequence_number |
| ----------------------- | ------------------------------------------------------------ | ------------------ | ---------------- | ---------------------- |
| 2022-07-28 10:43:52.93  | s3://.../table/metadata/00000-9441e604-b3c2-498a-a45a-6320e8ab9006.metadata.json | null               | null             | null                   |
| 2022-07-28 10:43:57.487 | s3://.../table/metadata/00001-f30823df-b745-4a0a-b293-7532e0c99986.metadata.json | 170260833677645300 | 0                | 1                      |
| 2022-07-28 10:43:58.25  | s3://.../table/metadata/00002-2cc2837a-02dc-4687-acc1-b4d86ea486f4.metadata.json | 958906493976709774 | 0                | 2                      |

### Snapshots

To show the valid snapshots for a table:

```sql
SELECT * FROM prod.db.table$snapshots;
```

| committed_at            | snapshot_id    | parent_id | operation | manifest_list                                      | summary                                                      |
| ----------------------- | -------------- | --------- | --------- | -------------------------------------------------- | ------------------------------------------------------------ |
| 2019-02-08 03:29:51.215 | 57897183625154 | null      | append    | s3://.../table/metadata/snap-57897183625154-1.avro | { added-records -> 2478404, total-records -> 2478404, added-data-files -> 438, total-data-files -> 438, flink.job-id -> 2e274eecb503d85369fb390e8956c813 } |

You can also join snapshots to table history. For example, this query will show table history, with the application ID that wrote each snapshot:

```sql
select
    h.made_current_at,
    s.operation,
    h.snapshot_id,
    h.is_current_ancestor,
    s.summary['flink.job-id']
from prod.db.table$history h
join prod.db.table$snapshots s
  on h.snapshot_id = s.snapshot_id
order by made_current_at
```

| made_current_at         | operation | snapshot_id    | is_current_ancestor | summary[flink.job-id]            |
| ----------------------- | --------- | -------------- | ------------------- | -------------------------------- |
| 2019-02-08 03:29:51.215 | append    | 57897183625154 | true                | 2e274eecb503d85369fb390e8956c813 |

### Files

To show a table's current data files:

```sql
SELECT * FROM prod.db.table$files;
```

| content | file_path                                                    | file_format | spec_id | partition        | record_count | file_size_in_bytes | column_sizes       | value_counts     | null_value_counts | nan_value_counts | lower_bounds    | upper_bounds    | key_metadata | split_offsets | equality_ids | sort_order_id |
| ------- | ------------------------------------------------------------ | ----------- | ------- | ---------------- | ------------ | ------------------ | ------------------ | ---------------- | ----------------- | ---------------- | --------------- | --------------- | ------------ | ------------- | ------------ | ------------- |
| 0       | s3:/.../table/data/00000-3-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 0       | {1999-01-01, 01} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> c] | [1 -> , 2 -> c] | null         | [4]           | null         | null          |
| 0       | s3:/.../table/data/00001-4-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 0       | {1999-01-01, 02} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> b] | [1 -> , 2 -> b] | null         | [4]           | null         | null          |
| 0       | s3:/.../table/data/00002-5-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 0       | {1999-01-01, 03} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> a] | [1 -> , 2 -> a] | null         | [4]           | null         | null          |

### Manifests

To show a table's current file manifests:

```sql
SELECT * FROM prod.db.table$manifests;
```

| path                                                         | length | partition_spec_id | added_snapshot_id   | added_data_files_count | existing_data_files_count | deleted_data_files_count | partition_summaries                  |
| ------------------------------------------------------------ | ------ | ----------------- | ------------------- | ---------------------- | ------------------------- | ------------------------ | ------------------------------------ |
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
SELECT * FROM prod.db.table$partitions;
```

| partition      | record_count | file_count | spec_id |
| -------------- | ------------ | ---------- | ------- |
| {20211001, 11} | 1            | 1          | 0       |
| {20211002, 11} | 1            | 1          | 0       |
| {20211001, 10} | 1            | 1          | 0       |
| {20211002, 10} | 1            | 1          | 0       |

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
SELECT * FROM prod.db.table$all_data_files;
```

| content | file_path                                                    | file_format | partition  | record_count | file_size_in_bytes | column_sizes       | value_counts       | null_value_counts | nan_value_counts | lower_bounds            | upper_bounds            | key_metadata | split_offsets | equality_ids | sort_order_id |
| ------- | ------------------------------------------------------------ | ----------- | ---------- | ------------ | ------------------ | ------------------ | ------------------ | ----------------- | ---------------- | ----------------------- | ----------------------- | ------------ | ------------- | ------------ | ------------- |
| 0       | s3://.../dt=20210102/00000-0-756e2512-49ae-45bb-aae3-c0ca475e7879-00001.parquet | PARQUET     | {20210102} | 14           | 2444               | {1 -> 94, 2 -> 17} | {1 -> 14, 2 -> 14} | {1 -> 0, 2 -> 0}  | {}               | {1 -> 1, 2 -> 20210102} | {1 -> 2, 2 -> 20210102} | null         | [4]           | null         | 0             |
| 0       | s3://.../dt=20210103/00000-0-26222098-032f-472b-8ea5-651a55b21210-00001.parquet | PARQUET     | {20210103} | 14           | 2444               | {1 -> 94, 2 -> 17} | {1 -> 14, 2 -> 14} | {1 -> 0, 2 -> 0}  | {}               | {1 -> 1, 2 -> 20210103} | {1 -> 3, 2 -> 20210103} | null         | [4]           | null         | 0             |
| 0       | s3://.../dt=20210104/00000-0-a3bb1927-88eb-4f1c-bc6e-19076b0d952e-00001.parquet | PARQUET     | {20210104} | 14           | 2444               | {1 -> 94, 2 -> 17} | {1 -> 14, 2 -> 14} | {1 -> 0, 2 -> 0}  | {}               | {1 -> 1, 2 -> 20210104} | {1 -> 3, 2 -> 20210104} | null         | [4]           | null         | 0             |

#### All Manifests

To show all of the table's manifest files:

```sql
SELECT * FROM prod.db.table$all_manifests;
```

| path                                                         | length | partition_spec_id | added_snapshot_id   | added_data_files_count | existing_data_files_count | deleted_data_files_count | partition_summaries                  |
| ------------------------------------------------------------ | ------ | ----------------- | ------------------- | ---------------------- | ------------------------- | ------------------------ | ------------------------------------ |
| s3://.../metadata/a85f78c5-3222-4b37-b7e4-faf944425d48-m0.avro | 6376   | 0                 | 6272782676904868561 | 2                      | 0                         | 0                        | [{false, false, 20210101, 20210101}] |

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
SELECT * FROM prod.db.table$refs;
```

| name    | type   | snapshot_id         | max_reference_age_in_ms | min_snapshots_to_keep | max_snapshot_age_in_ms |
| ------- | ------ | ------------------- | ----------------------- | --------------------- | ---------------------- |
| main    | BRANCH | 4686954189838128572 | 10                      | 20                    | 30                     |
| testTag | TAG    | 4686954189838128572 | 10                      | null                  | null                   |

