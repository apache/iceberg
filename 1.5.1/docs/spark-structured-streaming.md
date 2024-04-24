---
title: "Structured Streaming"
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

# Spark Structured Streaming

Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations. Spark DSv2 is an evolving API with different levels of support in Spark versions.

## Streaming Reads

Iceberg supports processing incremental data in spark structured streaming jobs which starts from a historical timestamp:

```scala
val df = spark.readStream
    .format("iceberg")
    .option("stream-from-timestamp", Long.toString(streamStartTimestamp))
    .load("database.table_name")
```

!!! warning
    Iceberg only supports reading data from append snapshots. Overwrite snapshots cannot be processed and will cause an exception by default. Overwrites may be ignored by setting `streaming-skip-overwrite-snapshots=true`. Similarly, delete snapshots will cause an exception by default, and deletes may be ignored by setting `streaming-skip-delete-snapshots=true`.

## Streaming Writes

To write values from streaming query to Iceberg table, use `DataStreamWriter`:

```scala
data.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
    .option("checkpointLocation", checkpointPath)
    .toTable("database.table_name")
```

If you're using Spark 3.0 or earlier, you need to use `.option("path", "database.table_name").start()`, instead of `.toTable("database.table_name")`.

In the case of the directory-based Hadoop catalog:

```scala
data.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
    .option("path", "hdfs://nn:8020/path/to/table") 
    .option("checkpointLocation", checkpointPath)
    .start()
```

Iceberg supports `append` and `complete` output modes:

* `append`: appends the rows of every micro-batch to the table
* `complete`: replaces the table contents every micro-batch

Prior to starting the streaming query, ensure you created the table. Refer to the [SQL create table](../spark-ddl.md#create-table) documentation to learn how to create the Iceberg table.

Iceberg doesn't support experimental [continuous processing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing), as it doesn't provide the interface to "commit" the output.

### Partitioned table

Iceberg requires sorting data by partition per task prior to writing the data. In Spark tasks are split by Spark partition.
against partitioned table. For batch queries you're encouraged to do explicit sort to fulfill the requirement
(see [here](../spark-writes.md#writing-distribution-modes)), but the approach would bring additional latency as
repartition and sort are considered as heavy operations for streaming workload. To avoid additional latency, you can
enable fanout writer to eliminate the requirement.

```scala
data.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
    .option("fanout-enabled", "true")
    .option("checkpointLocation", checkpointPath)
    .toTable("database.table_name")
```

Fanout writer opens the files per partition value and doesn't close these files till the write task finishes. Avoid using the fanout writer for batch writing, as explicit sort against output rows is cheap for batch workloads.

## Maintenance for streaming tables

Streaming writes can create new table versions quickly, creating lots of table metadata to track those versions.
Maintaining metadata by tuning the rate of commits, expiring old snapshots, and automatically cleaning up metadata files
is highly recommended.

### Tune the rate of commits

Having a high rate of commits produces data files, manifests, and snapshots which leads to additional maintenance. It is recommended to have a trigger interval of 1 minute at the minimum and increase the interval if needed.

The triggers section in [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers)
documents how to configure the interval.

### Expire old snapshots

Each batch written to a table produces a new snapshot. Iceberg tracks snapshots in table metadata until they are expired. Snapshots accumulate quickly with frequent commits, so it is highly recommended that tables written by streaming queries are [regularly maintained](../maintenance.md#expire-snapshots). [Snapshot expiration](../spark-procedures.md#expire_snapshots) is the procedure of removing the metadata and any data files that are no longer needed. By default, the procedure will expire the snapshots older than five days. 

### Compacting data files

The amount of data written from a streaming process is typically small, which can cause the table metadata to track lots of small files. [Compacting small files into larger files](../maintenance.md#compact-data-files) reduces the metadata needed by the table, and increases query efficiency. Iceberg and Spark [comes with the `rewrite_data_files` procedure](../spark-procedures.md#rewrite_data_files).

### Rewrite manifests

To optimize write latency on a streaming workload, Iceberg can write the new snapshot with a "fast" append that does not automatically compact manifests.
This could lead lots of small manifest files. Iceberg can [rewrite the number of manifest files to improve query performance](../maintenance.md#rewrite-manifests). Iceberg and Spark [come with the `rewrite_manifests` procedure](../spark-procedures.md#rewrite_manifests).
