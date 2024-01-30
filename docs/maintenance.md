---
title: Maintenance
url: maintenance
aliases:
    - "tables/maintenance"
menu:
    main:
        parent: Tables
        identifier: tables_maintenance
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

# Maintenance

{{< hint info >}}
Maintenance operations require the `Table` instance. Please refer [Java API quickstart](../java-api-quickstart/#create-a-table) page to refer how to load an existing table.
{{< /hint >}}
## Recommended Maintenance

### Expire Snapshots

Each write to an Iceberg table creates a new _snapshot_, or version, of a table. Snapshots can be used for time-travel queries, or the table can be rolled back to any valid snapshot.

Snapshots accumulate until they are expired by the [`expireSnapshots`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/Table.html#expireSnapshots--) operation. Regularly expiring snapshots is recommended to delete data files that are no longer needed, and to keep the size of table metadata small.

This example expires snapshots that are older than 1 day:

```java
Table table = ...
long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 24); // 1 day
table.expireSnapshots()
     .expireOlderThan(tsToExpire)
     .commit();
```

See the [`ExpireSnapshots` Javadoc](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/ExpireSnapshots.html) to see more configuration options.

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

{{< hint info >}}
Data files are not deleted until they are no longer referenced by a snapshot that may be used for time travel or rollback.
Regularly expiring snapshots deletes unused data files.
{{< /hint >}}

### Remove old metadata files

Iceberg keeps track of table metadata using JSON files. Each change to a table produces a new metadata file to provide atomicity.

Old metadata files are kept for history by default. Tables with frequent commits, like those written by streaming jobs, may need to regularly clean metadata files.

To automatically clean metadata files, set `write.metadata.delete-after-commit.enabled=true` in table properties. This will keep some metadata files (up to `write.metadata.previous-versions-max`) and will delete the oldest metadata file after each new one is created.

| Property                                     | Description                                                              |
| -------------------------------------------- |--------------------------------------------------------------------------|
| `write.metadata.delete-after-commit.enabled` | Whether to delete old **tracked** metadata files after each table commit |
| `write.metadata.previous-versions-max`       | The number of old metadata files to keep                                 |

Note that this will only delete metadata files that are **tracked** in the metadata log and will not delete orphaned metadata files.
Example: With `write.metadata.delete-after-commit.enabled=false` and `write.metadata.previous-versions-max=10`, one will have 10 tracked metadata files and 90 orphaned metadata files after 100 commits.
Configuring `write.metadata.delete-after-commit.enabled=true` and `write.metadata.previous-versions-max=20` will not automatically delete metadata files. Tracked metadata files would be deleted again when reaching `write.metadata.previous-versions-max=20`.

See [table write properties](../configuration/#write-properties) for more details.

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

See the [DeleteOrphanFiles Javadoc](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/actions/DeleteOrphanFiles.html) to see more configuration options.

This action may take a long time to finish if you have lots of files in data and metadata directories. It is recommended to execute this periodically, but you may not need to execute this often.

{{< hint info >}}
It is dangerous to remove orphan files with a retention interval shorter than the time expected for any write to complete because it
might corrupt the table if in-progress files are considered orphaned and are deleted. The default interval is 3 days.
{{< /hint >}}
    
{{< hint info >}}
Iceberg uses the string representations of paths when determining which files need to be removed. On some file systems,
the path can change over time, but it still represents the same file. For example, if you change authorities for an HDFS cluster, 
none of the old path urls used during creation will match those that appear in a current listing. *This will lead to data loss when 
RemoveOrphanFiles is run*. Please be sure the entries in your MetadataTables match those listed by the Hadoop
FileSystem API to avoid unintentional deletion. 
{{< /hint >}}

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

See the [`RewriteDataFiles` Javadoc](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/actions/RewriteDataFiles.html) to see more configuration options.

### Rewrite manifests

Iceberg uses metadata in its manifest list and manifest files speed up query planning and to prune unnecessary data files. The metadata tree functions as an index over a table's data.

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

See the [`RewriteManifests` Javadoc](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/actions/RewriteManifests.html) to see more configuration options.
