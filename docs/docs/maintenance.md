---
title: Maintenance
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

# Maintenance

!!! info
    Maintenance operations require the `Table` instance. Please refer to the [Java API quickstart](java-api-quickstart.md#create-a-table) page to learn how to load an existing table.

## Recommended Maintenance

### Expire Snapshots

Each write to an Iceberg table creates a new _snapshot_, or version, of a table. Snapshots can be used for time-travel queries, or the table can be rolled back to any valid snapshot.

Snapshots accumulate until they are expired by the [`expireSnapshots`](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/Table.html#expireSnapshots--) operation. Regularly expiring snapshots is recommended to delete data files that are no longer needed, and to keep the size of table metadata small.

This example expires snapshots that are older than 1 day:

```java
Table table = ...
long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 24); // 1 day
table.expireSnapshots()
     .expireOlderThan(tsToExpire)
     .commit();
```

See the [`ExpireSnapshots` Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/ExpireSnapshots.html) to see more configuration options.

There is also a Spark action that can run table expiration in parallel for large tables:

```java
Table table = ...
SparkActions
    .get()
    .expireSnapshots(table)
    .expireOlderThan(tsToExpire)
    .execute();
```

Expiring old snapshots removes them from metadata, so they are no longer available for time travel queries.

!!! info
    Data files are not deleted until they are no longer referenced by a snapshot that may be used for time travel or rollback.
    Regularly expiring snapshots deletes unused data files.

### Remove old metadata files

Iceberg keeps track of table metadata using JSON files. Each change to a table produces a new metadata file to provide atomicity.

Old metadata files are kept for history by default. Tables with frequent commits, like those written by streaming jobs, may need to regularly clean metadata files.

Each metadata file tracks the older metadata files in the `metadata-log` field.  The number of metadata files being tracked is defined by `write.metadata.previous-versions-max`.

To automatically delete older metadata files, set `write.metadata.delete-after-commit.enabled=true` in table properties. This will keep some metadata files as tracked (up to `write.metadata.previous-versions-max`), and will delete the oldest metadata file every time a new one is created.
Note that this will only delete metadata files that are **tracked** in the metadata log and will not delete orphaned metadata files.

Untracked metadata files are also deleted as part of [orphan file deletion](#delete-orphan-files).

| Property                                             | Default    | Description                                                                                      |
|------------------------------------------------------|------------|--------------------------------------------------------------------------------------------------|
| write.metadata.delete-after-commit.enabled           | false      | Controls whether to delete the oldest **tracked** version metadata files after each table commit |
| write.metadata.previous-versions-max                 | 100        | The max number of previous version metadata files to track                                       |

Examples:

* With `write.metadata.delete-after-commit.enabled=false` and `write.metadata.previous-versions-max=10`, after 100 commits, one will have 10 tracked metadata files and 90 orphaned metadata files. These 90 orphaned metadata files cannot be deleted by setting `write.metadata.delete-after-commit.enabled=true` because they are already untracked. They can only be cleaned with an orphan file deletion procedure.
* With `write.metadata.delete-after-commit.enabled=true` and `write.metadata.previous-versions-max=20`, after 21 commits, one will have 20 tracked metadata files, with the oldest metadata file being deleted by the writer after committing. With each additional commit, the oldest metadata file will be deleted.

See [table write properties](configuration.md#write-properties) for more details.

### Delete orphan files

In Spark and other distributed processing engines, task or job failures can leave files that are not referenced by table metadata, and in some cases normal snapshot expiration may not be able to determine a file is no longer needed and delete it.

To clean up these "orphan" files under a table location, use the `deleteOrphanFiles` action.

```java
Table table = ...
SparkActions
    .get()
    .deleteOrphanFiles(table)
    .execute();
```

See the [DeleteOrphanFiles Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/actions/DeleteOrphanFiles.html) to see more configuration options.

This action may take a long time to finish if you have lots of files in data and metadata directories. It is recommended to execute this periodically, but you may not need to execute this often.

!!! info
    It is dangerous to remove orphan files with a retention interval shorter than the time expected for any write to complete because it
    might corrupt the table if in-progress files are considered orphaned and are deleted. The default interval is 3 days.

!!! info
    Iceberg uses the string representations of paths when determining which files need to be removed. On some file systems,
    the path can change over time, but it still represents the same file. For example, if you change authorities for an HDFS cluster,
    none of the old path urls used during creation will match those that appear in a current listing. *This will lead to data loss when
    RemoveOrphanFiles is run*. Please be sure the entries in your MetadataTables match those listed by the Hadoop
    FileSystem API to avoid unintentional deletion.

## Optional Maintenance

Some tables require additional maintenance. For example, streaming queries may produce small data files that should be [compacted into larger files](#compact-data-files). And some tables can benefit from [rewriting manifest files](#rewrite-manifests) to make locating data for queries much faster.

### Compact data files

Iceberg tracks each data file in a table. More data files leads to more metadata stored in manifest files, and small data files causes an unnecessary amount of metadata and less efficient queries from file open costs.

Iceberg can compact data files in parallel using Spark with the `rewriteDataFiles` action. This will combine small files into larger files to reduce metadata overhead and runtime file open cost.

```java
Table table = ...
SparkActions
    .get()
    .rewriteDataFiles(table)
    .filter(Expressions.equal("date", "2020-08-18"))
    .option("target-file-size-bytes", Long.toString(500 * 1024 * 1024)) // 500 MB
    .execute();
```

The `files` metadata table is useful for inspecting data file sizes and determining when to compact partitions.

See the [`RewriteDataFiles` Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/actions/RewriteDataFiles.html) to see more configuration options.

### Rewrite manifests

Iceberg uses metadata in its manifest list and manifest files to speed up query planning and to prune unnecessary data files. The metadata tree functions as an index over a table's data.

Manifests in the metadata tree are automatically compacted in the order they are added, which makes queries faster when the write pattern aligns with read filters. For example, writing hourly-partitioned data as it arrives is aligned with time range query filters.

When a table's write pattern doesn't align with the query pattern, metadata can be rewritten to re-group data files into manifests using `rewriteManifests` or the `rewriteManifests` action (for parallel rewrites using Spark).

This example rewrites small manifests and groups data files by the first partition field.

```java
Table table = ...
SparkActions
    .get()
    .rewriteManifests(table)
    .rewriteIf(file -> file.length() < 10 * 1024 * 1024) // 10 MB
    .execute();
```

See the [`RewriteManifests` Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/actions/RewriteManifests.html) to see more configuration options.

### Rewrite position delete files

Iceberg can rewrite position delete files, which serves two purposes:

* Minor compaction: Compact small position delete files into larger ones. This reduces the size of metadata stored in manifest files and the overhead of opening small delete files.
* Remove dangling deletes: Filter out position delete records that refer to data files that are no longer live. After `rewriteDataFiles`, position delete records pointing to the rewritten data files are not always marked for removal, and can remain tracked by the table's live snapshot metadata. This is known as the "dangling delete" problem.

```java
Table table = ...
SparkActions
    .get()
    .rewritePositionDeletes(table)
    .execute();
```

Dangling deletes are always filtered out during rewriting.

See the [`RewritePositionDeleteFiles` Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/actions/RewritePositionDeleteFiles.html) to see more configuration options. In Spark SQL, this action is also available as the [`rewrite_position_delete_files` procedure](spark-procedures.md#rewrite_position_delete_files).

### Remove dangling delete files

A delete file is dangling if its deletes no longer apply to any live data files. The `removeDanglingDeleteFiles` action scans the current snapshot and removes:

* Position delete files with a data sequence number less than that of any data file in the same partition
* Equality delete files with a data sequence number less than or equal to that of any data file in the same partition

This is a metadata-only operation: dangling delete files are dropped from table metadata, and no data or delete files are rewritten.

```java
Table table = ...
SparkActions
    .get()
    .removeDanglingDeleteFiles(table)
    .execute();
```

There is no Spark SQL procedure for this action, but the `rewriteDataFiles` action can remove dangling deletes as part of compaction when the `remove-dangling-deletes` option is set to `true`, and the `rewritePositionDeletes` action always filters out dangling position deletes.

See the [`RemoveDanglingDeleteFiles` Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/actions/RemoveDanglingDeleteFiles.html) for more details.

### Compute table statistics

The `computeTableStats` action collects Number of Distinct Values (NDV) statistics for table columns, writes them to a [Puffin](../../puffin-spec.md) statistics file, and registers the file in table metadata. Query engines can use these statistics for cost-based optimization.

By default, statistics are collected for all columns using the table's current snapshot. The action can be configured to use a specific snapshot and/or a subset of columns.

```java
Table table = ...
SparkActions
    .get()
    .computeTableStats(table)
    .columns("col1", "col2")
    .execute();
```

See the [`ComputeTableStats` Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/actions/ComputeTableStats.html) to see more configuration options. In Spark SQL, this action is also available as the [`compute_table_stats` procedure](spark-procedures.md#compute_table_stats).

### Compute partition statistics

The `computePartitionStats` action computes [partition statistics](../../spec.md#partition-statistics) for the table and registers the resulting partition statistics file in table metadata. Statistics are computed incrementally from the last snapshot that has a partition statistics file up to the chosen snapshot (the current snapshot by default); a full computation is performed if no previous partition statistics file exists.

```java
Table table = ...
SparkActions
    .get()
    .computePartitionStats(table)
    .execute();
```

See the [`ComputePartitionStats` Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/actions/ComputePartitionStats.html) to see more configuration options. In Spark SQL, this action is also available as the [`compute_partition_stats` procedure](spark-procedures.md#compute_partition_stats).

## Other Actions

The following actions are not part of routine table maintenance, but are useful for cleaning up after dropping a table and for copying a table to a new location.

### Delete reachable files

The `deleteReachableFiles` action deletes all files referenced by a table metadata file: data files, delete files, manifests, manifest lists, and metadata files. It can be used to clean up the underlying storage after a table is dropped with `purge` disabled, for example when the catalog cannot delete files itself.

```java
String metadataLocation = ... // path to the table's metadata.json file
FileIO io = ...               // FileIO able to read and delete the table's files
SparkActions
    .get()
    .deleteReachableFiles(metadataLocation)
    .io(io)
    .execute();
```

Note that this action takes the path of a `metadata.json` file rather than a `Table` instance, because it is intended to run after the table has been dropped from the catalog.

!!! danger
    This action irreversibly deletes all reachable files of a table. Only use it once the table is dropped and its data is no longer needed. If other tables share files with this table (for example, tables created by the `snapshot` action or procedure), their data will be corrupted.

See the [`DeleteReachableFiles` Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/actions/DeleteReachableFiles.html) to see more configuration options.

### Rewrite table path

The `rewriteTablePath` action stages a copy of the table's metadata files in which every absolute path starting with the source prefix is replaced by the target prefix. It can be used as the starting point to fully or incrementally copy a table to a new location, for example for disaster recovery.

```java
Table table = ...
SparkActions
    .get()
    .rewriteTablePath(table)
    .rewriteLocationPrefix("s3://bucket/old-table-location", "s3://bucket/new-table-location")
    .execute();
```

The action returns the name of the latest rewritten `metadata.json` and the location of a file list containing the source and target paths of all files to copy. The action only stages rewritten metadata files and prepares the copy plan; actually copying the data and metadata files to the target location is done separately with a file-copy tool.

See the [`RewriteTablePath` Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/actions/RewriteTablePath.html) to see more configuration options. In Spark SQL, this action is also available as the [`rewrite_table_path` procedure](spark-procedures.md#rewrite_table_path).
