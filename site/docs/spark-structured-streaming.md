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

The required value in `pathToTable` depends on the catalog:

* Hadoop Catalog: the location of the table
* Hive Catalog: the table identifier represented by String (`catalog.database.table`)

Iceberg doesn't support "continuous processing", as it doesn't provide the interface to "commit" the output.

Iceberg supports below output modes:

* append: appends the output of every micro-batch to the table
* complete: replaces the table contents every micro-batch

The table should be created in prior to start the streaming query. Please refer [SQL create table](/spark/#create-table)
on Spark page to see how to create the Iceberg table.

## Maintenance

Streaming queries can create new table versions quickly, which creates lots of table metadata to track those versions.
Maintaining metadata by tuning the rate of commits, expiring old snapshots, and automatically cleaning up metadata files
is highly recommended.

### Tune the rate of commits

Having high rate of commits would produce lots of data files, manifests, and snapshots which leads the table hard
to maintain. We encourage having trigger interval 1 minute at minimum, and increase the interval if you encounter
issues.

Please read through the [Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
if you're not familiar with configuring batch interval yet.

### Expire old snapshots

Run [expireSnapshots()](/javadoc/master/org/apache/iceberg/Table.html#expireSnapshots--) regularly to prune the old
version of snapshots, which reduces the size of metadata file, as well as cleans up data files and manifest files which
are no longer referenced.

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

### Rewriting manifests (manifest compaction / rearrangement)

To optimize write latency on streaming workload, Iceberg may write the new snapshot on fast append, which writes
a new metadata per commit. This would lead lots of small manifest files, which affects badly on file system (e.g.
"small files problem" in HDFS), and brings inefficient on reading the table.

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

Please note that rewriting manifests creates a new snapshot with optimized manifests. It doesn't rewrite old snapshots,
meaning it doesn't optimize querying against older snapshot (via time-travel), and old manifest files cannot be removed
until we expire old snapshots referring these files.

### Rewriting data files (data compaction / rearrangement)

Lots of (small) data files may lead bigger manifest and bring inefficiency to read the table. As we mentioned problems
in the previous section, you would not prefer to have small files in general.

You may want to run this action periodically to clusters the data files and pack into smaller number of files.
The action can be also used to optimize read performance for specific partition spec.

```scala
// ... assuming there's a Table instance `table` ...
val actions: Actions = Actions.forTable(table)
actions.rewriteDataFiles()
       .execute()
```

`actions.rewriteDataFiles()` provides `RewriteDataFilesAction`, which provides various methods to control how to group
the data files, and how to exclude the files on grouping. Please refer the javadoc of
[RewriteDataFilesAction](/javadoc/master/org/apache/iceberg/actions/RewriteDataFilesAction.html) to see more.

Please note that rewriting data files creates a new snapshot with optimized data files. It doesn't rewrite old
snapshots, meaning it doesn't optimize querying against older snapshot (via time-travel), and old data files cannot be
removed until we expire old snapshots referring these files.

### Removed old metadata files in Hadoop Catalog

If you are using HadoopCatalog, you may want to enable `write.metadata.delete-after-commit.enabled` in the table
properties, and reduce `write.metadata.previous-versions-max` as well (if necessary) to retain only specific number of
metadata files.

Please refer the [table write properties](/configuration/#write-properties) for more details.

### Remove orphan files

Expiring old snapshots may leave orphan data and manifest files. `expireSnapshots()` takes care of removing files
which are no longer referenced, but there may be still some cases orphan files may exist. You can execute removing
orphan files explicitly to clean up files.

```scala
// ... assuming there's a Table instance `table` ...
val actions: Actions = Actions.forTable(table)
actions.removeOrphanFiles()
       .execute()
```

`actions.removeOrphanFiles()` provides `RemoveOrphanFilesAction`, which provides various methods to control how to
clean up orphan files. Please refer the javadoc of
[RemoveOrphanFilesAction](/javadoc/master/org/apache/iceberg/actions/RemoveOrphanFilesAction.html) to see more.

This action may take huge time to finish if you have lots of files in data and metadata directories. It's recommended
to execute this periodically, but you may not need to execute this often.

It is dangerous to call this action with a short retention interval as it might corrupt the state of the table if
another operation is writing at the same time. To prevent the case, the action will only remove files that are older
than 3 days by default.

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
