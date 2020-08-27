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

As of Spark 3.0, DataFrame reads and writes are supported.

| Feature support                                  | Spark 3.0| Spark 2.4  | Notes                                          |
|--------------------------------------------------|----------|------------|------------------------------------------------|
| [DataFrame write](#writing-with-streaming-query) | ✔        | ✔          |                                                |

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

The table should be created in prior to start the streaming query. Refer [SQL create table](/spark/#create-table)
on Spark page to see how to create the Iceberg table.

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

The amount of data written in a micro batch is typically small, which can cause the table metadata to track lots of small files. Compacting small files into larger files reduces the metadata needed by the table, and increases query efficiency.

### Rewrite manifests

To optimize write latency on streaming workload, Iceberg may write the new snapshot with a "fast" append that does not automatically compact manifests.
This could lead lots of small manifest files. Manifests can be [rewritten to optimize queries and to compact](../maintenance#rewrite-manifests).

## Appendix

### Retrieve Table instance in Hadoop catalog

```scala
// ... assume catalogName, dbName, tableName are present ...
import org.apache.iceberg.hadoop.HadoopTables
val warehousePath = spark.sparkContext.getConf.get(s"spark.sql.catalog.$catalogName.warehouse")
val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf)
val table = hadoopTables.load(s"$warehousePath/$dbName/$tableName")
```

### Retrieve Table instance in Hive catalog

```scala
// ... assume dbName, tableName are present ...
import org.apache.iceberg.hive.HiveCatalog;

val catalog = new HiveCatalog(spark.sessionState.newHadoopConf)
val table = catalog.loadTable(TableIdentifier.of(dbName, tableName))
```
