---
title: "Structured Streaming"
url: spark-structured-streaming
aliases:
    - "spark/spark-structured-streaming"
menu:
    main:
        parent: Spark
        identifier: spark_structured_streaming
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

# Spark Structured Streaming

Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations. Spark DSv2 is an evolving API
with different levels of support in Spark versions.

As of Spark 3, DataFrame reads and writes are supported.

## Streaming Reads

Iceberg supports processing incremental data in spark structured streaming jobs which starts from a historical timestamp:

```scala
val df = spark.readStream
    .format("iceberg")
    .option("stream-from-timestamp", Long.toString(streamStartTimestamp))
    .load("database.table_name")
```

{{< hint warning >}}
Iceberg only supports reading data from append snapshots. Overwrite snapshots cannot be processed and will cause an exception by default. Overwrites may be ignored by setting `streaming-skip-overwrite-snapshots=true`. Similarly, delete snapshots will cause an exception by default, and deletes may be ignored by setting `streaming-skip-delete-snapshots=true`.
{{</ hint >}}

## Streaming Writes

To write values from streaming query to Iceberg table, use `DataStreamWriter`:

```scala
val tableIdentifier: String = ...
data.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
    .option("path", tableIdentifier)
    .option("checkpointLocation", checkpointPath)
    .start()
```

The `tableIdentifier` can be:

* The fully-qualified path to a HDFS table, like `hdfs://nn:8020/path/to/table`
* A table name if the table is tracked by a catalog, like `database.table_name`

Iceberg doesn't support "continuous processing", as it doesn't provide the interface to "commit" the output.

Iceberg supports `append` and `complete` output modes:

* `append`: appends the rows of every micro-batch to the table
* `complete`: replaces the table contents every micro-batch

The table should be created in prior to start the streaming query. Refer [SQL create table](../spark-ddl/#create-table)
on Spark page to see how to create the Iceberg table.

### Writing against partitioned table

Iceberg requires the data to be sorted according to the partition spec per task (Spark partition) in prior to write
against partitioned table. For batch queries you're encouraged to do explicit sort to fulfill the requirement
(see [here](../spark-writes/#writing-to-partitioned-tables)), but the approach would bring additional latency as
repartition and sort are considered as heavy operations for streaming workload. To avoid additional latency, you can
enable fanout writer to eliminate the requirement.

```scala
val tableIdentifier: String = ...
data.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
    .option("path", tableIdentifier)
    .option("fanout-enabled", "true")
    .option("checkpointLocation", checkpointPath)
    .start()
```

Fanout writer opens the files per partition value and doesn't close these files till write task is finished.
This functionality is discouraged for batch query, as explicit sort against output rows isn't expensive for batch workload.

## Maintenance for streaming tables

Streaming queries can create new table versions quickly, which creates lots of table metadata to track those versions.
Maintaining metadata by tuning the rate of commits, expiring old snapshots, and automatically cleaning up metadata files
is highly recommended.

### Tune the rate of commits

Having high rate of commits would produce lots of data files, manifests, and snapshots which leads the table hard
to maintain. We encourage having trigger interval 1 minute at minimum, and increase the interval if needed.

The triggers section in [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers)
documents how to configure the interval.

### Expire old snapshots

Each micro-batch written to a table produces a new snapshot, which are tracked in table metadata until they are expired to remove the metadata and any data files that are no longer needed. Snapshots accumulate quickly with frequent commits, so it is highly recommended that tables written by streaming queries are [regularly maintained](../maintenance#expire-snapshots).

### Compacting data files

The amount of data written in a micro batch is typically small, which can cause the table metadata to track lots of small files. [Compacting small files into larger files](../maintenance#compact-data-files) reduces the metadata needed by the table, and increases query efficiency.

### Rewrite manifests

To optimize write latency on streaming workload, Iceberg may write the new snapshot with a "fast" append that does not automatically compact manifests.
This could lead lots of small manifest files. Manifests can be [rewritten to optimize queries and to compact](../maintenance#rewrite-manifests).
