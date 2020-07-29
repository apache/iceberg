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

As of Spark 3.0, the new API on reading/writing table on table identifier is not yet added on streaming query.

| Feature support                                  | Spark 3.0| Spark 2.4  | Notes                                          |
|--------------------------------------------------|----------|------------|------------------------------------------------|
| [DataFrame write](#writing-with-streaming-query) | ✔        | ✔          |                                                |

## Writing with streaming query

To write values from streaming query to Iceberg table, use `DataStreamWriter`:

```scala
data.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
    .option("path", pathToTable)
    .option("checkpointLocation", checkpointPath)
    .start()
```

Iceberg supports below output modes:

* append
* complete

The table should be created in prior to start the streaming query.

## Maintenance

Streaming queries can create new table versions quickly, which creates lots of table metadata to track those versions.
Maintaining metadata by tuning the rate of commits, expiring old snapshots, and automatically cleaning up metadata files
is highly recommended.

### Tune the rate of commits

Having high rate of commits would produce lots of data files, manifests, and snapshots which leads the table hard
to maintain. We encourage having trigger interval 1 minute at minimum, and increase the interval if you encounter
issues.

### Retain recent metadata files in Hadoop catalog

If you are using HadoopCatalog, you may want to enable `write.metadata.delete-after-commit.enabled` in the table
properties, and reduce `write.metadata.previous-versions-max` as well (if necessary) to retain only specific number of
metadata files.

Please refer the [table write properties](/configuration/#write-properties) for more details.

### Expire old snapshots

You may want to run [expireSnapshots()](/javadoc/master/org/apache/iceberg/Table.html#expireSnapshots--) periodically
to prune the old version of snapshots, which reduces the size of metadata file, as well as cleans up data files and
manifest files which are no longer referenced.

For example, below code executes expiring snapshots which are older than 1 day.

```scala
// ... assuming there's a Table instance `table` ...
val tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 24) // 1 day
table.expireSnapshots()
     .expireOlderThan(tsToExpire)
     .commit()
```

`Table.expireSnapshots()` provides `ExpireSnapshots`, which provides various methods to control the criteria on
expiration. Please refer the javadoc of [ExpireSnapshots](/javadoc/master/org/apache/iceberg/ExpireSnapshots.html) to
see more.

Please keep in mind that expiring old version of snapshots means you no longer be able to do time travel before the
time on condition for expiration.

### Rewriting manifests

To optimize write latency on streaming workload, Iceberg may write the new snapshot on fast append, which writes
a new metadata per commit. This would lead lots of small manifest files, which is inefficient on reading the table.

You may want to run this action periodically to cluster the manifest files and pack into smaller number of files.
The action can be also used to optimize read performance for specific partition spec.

```scala
// ... assuming there's a Table instance `table` ...
val actions: Actions = Actions.forTable(table)
actions.rewriteManifests()
       .execute()
```

`actions.rewriteManifests()` provides `RewriteManifestsAction`, which provides various methods to control how to group
the manifest files, and how to exclude the files on grouping. Please refer the javadoc of
[RewriteManifestsAction](/javadoc/master/org/apache/iceberg/actions/RewriteManifestsAction.html) to see more.

### Rewriting data files (data compaction)

Lots of (small) data files may lead bigger manifest and bring inefficiency to read the table. The action of rewriting
data files clusters the data files and pack into smaller number of files. The action can be also used to optimize read
performance for specific partition spec.

```scala
// ... assuming there's a Table instance `table` ...
val actions: Actions = Actions.forTable(table)
actions.rewriteDataFiles()
       .execute()
```

`actions.rewriteDataFiles()` provides `RewriteDataFilesAction`, which provides various methods to control how to group
the data files, and how to exclude the files on grouping. Please refer the javadoc of
[RewriteDataFilesAction](/javadoc/master/org/apache/iceberg/actions/RewriteDataFilesAction.html) to see more.

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
// ... assume tableName (with multi-part identifier) is present ...
import org.apache.iceberg.hive.HiveCatalog;

val catalog = new HiveCatalog(spark.sessionState.newHadoopConf)
val table = catalog.loadTable(TableIdentifier.of(tableName))
```
