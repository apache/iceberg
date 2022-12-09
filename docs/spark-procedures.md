---
title: "Procedures"
url: spark-procedures
aliases:
    - "spark/spark-procedures"
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

# Spark Procedures

To use Iceberg in Spark, first configure [Spark catalogs](../spark-configuration). Stored procedures are only available when using [Iceberg SQL extensions](../spark-configuration#sql-extensions) in Spark 3.

## Usage

Procedures can be used from any configured Iceberg catalog with `CALL`. All procedures are in the namespace `system`.

`CALL` supports passing arguments by name (recommended) or by position. Mixing position and named arguments is not supported.

### Named arguments

All procedure arguments are named. When passing arguments by name, arguments can be in any order and any optional argument can be omitted.

```sql
CALL catalog_name.system.procedure_name(arg_name_2 => arg_2, arg_name_1 => arg_1)
```

### Positional arguments

When passing arguments by position, only the ending arguments may be omitted if they are optional.

```sql
CALL catalog_name.system.procedure_name(arg_1, arg_2, ... arg_n)
```

## Snapshot management

### `rollback_to_snapshot`

Roll back a table to a specific snapshot ID.

To roll back to a specific time, use [`rollback_to_timestamp`](#rollback_to_timestamp).

{{< hint info >}}
This procedure invalidates all cached Spark plans that reference the affected table.
{{< /hint >}}

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to update |
| `snapshot_id` | ✔️  | long   | Snapshot ID to rollback to |

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `previous_snapshot_id` | long | The current snapshot ID before the rollback |
| `current_snapshot_id`  | long | The new current snapshot ID |

#### Example

Roll back table `db.sample` to snapshot ID `1`:

```sql
CALL catalog_name.system.rollback_to_snapshot('db.sample', 1)
```

### `rollback_to_timestamp`

Roll back a table to the snapshot that was current at some time.

{{< hint info >}}
This procedure invalidates all cached Spark plans that reference the affected table.
{{< /hint >}}

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to update |
| `timestamp`   | ✔️  | timestamp | A timestamp to rollback to |

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `previous_snapshot_id` | long | The current snapshot ID before the rollback |
| `current_snapshot_id`  | long | The new current snapshot ID |

#### Example

Roll back `db.sample` to a specific day and time.
```sql
CALL catalog_name.system.rollback_to_timestamp('db.sample', TIMESTAMP '2021-06-30 00:00:00.000')
```

### `set_current_snapshot`

Sets the current snapshot ID for a table.

Unlike rollback, the snapshot is not required to be an ancestor of the current table state.

{{< hint info >}}
This procedure invalidates all cached Spark plans that reference the affected table.
{{< /hint >}}

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to update |
| `snapshot_id` | ✔️  | long   | Snapshot ID to set as current |

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `previous_snapshot_id` | long | The current snapshot ID before the rollback |
| `current_snapshot_id`  | long | The new current snapshot ID |

#### Example

Set the current snapshot for `db.sample` to 1:
```sql
CALL catalog_name.system.set_current_snapshot('db.sample', 1)
```

### `cherrypick_snapshot`

Cherry-picks changes from a snapshot into the current table state.

Cherry-picking creates a new snapshot from an existing snapshot without altering or removing the original.

Only append and dynamic overwrite snapshots can be cherry-picked.

{{< hint info >}}
This procedure invalidates all cached Spark plans that reference the affected table.
{{< /hint >}}

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to update |
| `snapshot_id` | ✔️  | long | The snapshot ID to cherry-pick |

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `source_snapshot_id` | long | The table's current snapshot before the cherry-pick |
| `current_snapshot_id` | long | The snapshot ID created by applying the cherry-pick |

#### Examples

Cherry-pick snapshot 1
```sql
CALL catalog_name.system.cherrypick_snapshot('my_table', 1)
```

Cherry-pick snapshot 1 with named args
```sql
CALL catalog_name.system.cherrypick_snapshot(snapshot_id => 1, table => 'my_table' )
```


## Metadata management

Many [maintenance actions](../maintenance) can be performed using Iceberg stored procedures.

### `expire_snapshots`

Each write/update/delete/upsert/compaction in Iceberg produces a new snapshot while keeping the old data and metadata
around for snapshot isolation and time travel. The `expire_snapshots` procedure can be used to remove older snapshots
and their files which are no longer needed.

This procedure will remove old snapshots and data files which are uniquely required by those old snapshots. This means
the `expire_snapshots` procedure will never remove files which are still required by a non-expired snapshot.

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to update |
| `older_than`  | ️   | timestamp | Timestamp before which snapshots will be removed (Default: 5 days ago) |
| `retain_last` |    | int       | Number of ancestor snapshots to preserve regardless of `older_than` (defaults to 1) |
| `max_concurrent_deletes` |    | int       | Size of the thread pool used for delete file actions (by default, no thread pool is used) |
| `stream_results` |    | boolean       | When true, deletion files will be sent to Spark driver by RDD partition (by default, all the files will be sent to Spark driver). This option is recommended to set to `true` to prevent Spark driver OOM from large file size |

If `older_than` and `retain_last` are omitted, the table's [expiration properties](../configuration/#table-behavior-properties) will be used.

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `deleted_data_files_count` | long | Number of data files deleted by this operation |
| `deleted_position_delete_files_count` | long | Number of position delete files deleted by this operation |
| `deleted_equality_delete_files_count` | long | Number of equality delete files deleted by this operation |
| `deleted_manifest_files_count` | long | Number of manifest files deleted by this operation |
| `deleted_manifest_lists_count` | long | Number of manifest List files deleted by this operation |

#### Examples

Remove snapshots older than specific day and time, but retain the last 100 snapshots:

```sql
CALL hive_prod.system.expire_snapshots('db.sample', TIMESTAMP '2021-06-30 00:00:00.000', 100)
```

### `remove_orphan_files`

Used to remove files which are not referenced in any metadata files of an Iceberg table and can thus be considered "orphaned".

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to clean |
| `older_than`  | ️   | timestamp | Remove orphan files created before this timestamp (Defaults to 3 days ago) |
| `location`    |    | string    | Directory to look for files in (defaults to the table's location) |
| `dry_run`     |    | boolean   | When true, don't actually remove files (defaults to false) |
| `max_concurrent_deletes` |    | int       | Size of the thread pool used for delete file actions (by default, no thread pool is used) |

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `orphan_file_location` | String | The path to each file determined to be an orphan by this command |

#### Examples

List all the files that are candidates for removal by performing a dry run of the `remove_orphan_files` command on this table without actually removing them:
```sql
CALL catalog_name.system.remove_orphan_files(table => 'db.sample', dry_run => true)
```

Remove any files in the `tablelocation/data` folder which are not known to the table `db.sample`.
```sql
CALL catalog_name.system.remove_orphan_files(table => 'db.sample', location => 'tablelocation/data')
```

### `rewrite_data_files`

Iceberg tracks each data file in a table. More data files leads to more metadata stored in manifest files, and small data files causes an unnecessary amount of metadata and less efficient queries from file open costs.

Iceberg can compact data files in parallel using Spark with the `rewriteDataFiles` action. This will combine small files into larger files to reduce metadata overhead and runtime file open cost.

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to update |
| `strategy`    |    | string | Name of the strategy - binpack or sort. Defaults to binpack strategy |
| `sort_order`  |    | string | For Zorder use a comma separated list of columns within zorder(). (Supported in Spark 3.2 and Above) Example: zorder(c1,c2,c3). <br/>Else, Comma separated sort orders in the format (ColumnName SortDirection NullOrder). <br/>Where SortDirection can be ASC or DESC. NullOrder can be NULLS FIRST or NULLS LAST. <br/>Defaults to the table's sort order |
| `options`     | ️   | map<string, string> | Options to be used for actions|
| `where`       | ️   | string | predicate as a string used for filtering the files. Note that all files that may contain data matching the filter will be selected for rewriting|


See the [`RewriteDataFiles` Javadoc](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/actions/RewriteDataFiles.html#field.summary),
<br/>  [`BinPackStrategy` Javadoc](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/actions/BinPackStrategy.html#field.summary)
and <br/> [`SortStrategy` Javadoc](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/actions/SortStrategy.html#field.summary)
for list of all the supported options for this action.

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `rewritten_data_files_count` | int | Number of data which were re-written by this command |
| `added_data_files_count`     | int | Number of new data files which were written by this command |

#### Examples

Rewrite the data files in table `db.sample` using the default rewrite algorithm of bin-packing to combine small files 
and also split large files according to the default write size of the table.
```sql
CALL catalog_name.system.rewrite_data_files('db.sample')
```

Rewrite the data files in table `db.sample` by sorting all the data on id and name 
using the same defaults as bin-pack to determine which files to rewrite.
```sql
CALL catalog_name.system.rewrite_data_files(table => 'db.sample', strategy => 'sort', sort_order => 'id DESC NULLS LAST,name ASC NULLS FIRST')
```

Rewrite the data files in table `db.sample` by zOrdering on column c1 and c2.
Using the same defaults as bin-pack to determine which files to rewrite.
```sql
CALL catalog_name.system.rewrite_data_files(table => 'db.sample', strategy => 'sort', sort_order => 'zorder(c1,c2)')
```

Rewrite the data files in table `db.sample` using bin-pack strategy in any partition where more than 2 or more files need to be rewritten.
```sql
CALL catalog_name.system.rewrite_data_files(table => 'db.sample', options => map('min-input-files','2'))
```

Rewrite the data files in table `db.sample` and select the files that may contain data matching the filter (id = 3 and name = "foo") to be rewritten.
```sql
CALL catalog_name.system.rewrite_data_files(table => 'db.sample', where => 'id = 3 and name = "foo"')
```

### `rewrite_manifests`

Rewrite manifests for a table to optimize scan planning.

Data files in manifests are sorted by fields in the partition spec. This procedure runs in parallel using a Spark job.

See the [`RewriteManifests` Javadoc](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/actions/RewriteManifests.html)
to see more configuration options.

{{< hint info >}}
This procedure invalidates all cached Spark plans that reference the affected table.
{{< /hint >}}

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to update |
| `use_caching` | ️   | boolean | Use Spark caching during operation (defaults to true) |

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `rewritten_manifests_count` | int | Number of manifests which were re-written by this command |
| `added_mainfests_count`     | int | Number of new manifest files which were written by this command |

#### Examples

Rewrite the manifests in table `db.sample` and align manifest files with table partitioning.
```sql
CALL catalog_name.system.rewrite_manifests('db.sample')
```

Rewrite the manifests in table `db.sample` and disable the use of Spark caching. This could be done to avoid memory issues on executors.
```sql
CALL catalog_name.system.rewrite_manifests('db.sample', false)
```

## Table migration

The `snapshot` and `migrate` procedures help test and migrate existing Hive or Spark tables to Iceberg.

### `snapshot`

Create a light-weight temporary copy of a table for testing, without changing the source table.

The newly created table can be changed or written to without affecting the source table, but the snapshot uses the original table's data files.

When inserts or overwrites run on the snapshot, new files are placed in the snapshot table's location rather than the original table location.

When finished testing a snapshot table, clean it up by running `DROP TABLE`.

{{< hint info >}}
Because tables created by `snapshot` are not the sole owners of their data files, they are prohibited from
actions like `expire_snapshots` which would physically delete data files. Iceberg deletes, which only effect metadata,
are still allowed. In addition, any operations which affect the original data files will disrupt the Snapshot's 
integrity. DELETE statements executed against the original Hive table will remove original data files and the
`snapshot` table will no longer be able to access them.
{{< /hint >}}

See [`migrate`](#migrate) to replace an existing table with an Iceberg table.

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `source_table`| ✔️  | string | Name of the table to snapshot |
| `table`       | ✔️  | string | Name of the new Iceberg table to create |
| `location`    |    | string | Table location for the new table (delegated to the catalog by default) |
| `properties`  | ️   | map<string, string> | Properties to add to the newly created table |

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `imported_files_count` | long | Number of files added to the new table |

#### Examples

Make an isolated Iceberg table which references table `db.sample` named `db.snap` at the
catalog's default location for `db.snap`.
```sql
CALL catalog_name.system.snapshot('db.sample', 'db.snap')
```

Migrate an isolated Iceberg table which references table `db.sample` named `db.snap` at
a manually specified location `/tmp/temptable/`.
```sql
CALL catalog_name.system.snapshot('db.sample', 'db.snap', '/tmp/temptable/')
```

### `migrate`

Replace a table with an Iceberg table, loaded with the source's data files.

Table schema, partitioning, properties, and location will be copied from the source table.

Migrate will fail if any table partition uses an unsupported format. Supported formats are Avro, Parquet, and ORC.
Existing data files are added to the Iceberg table's metadata and can be read using a name-to-id mapping created from the original table schema.

To leave the original table intact while testing, use [`snapshot`](#snapshot) to create new temporary table that shares source data files and schema.

By default, the original table is retained with the name `table_BACKUP_`.

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to migrate |
| `properties`  | ️   | map<string, string> | Properties for the new Iceberg table |
| `drop_backup` |   | boolean | When true, the original table will not be retained as backup (defaults to false) |

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `migrated_files_count` | long | Number of files appended to the Iceberg table |

#### Examples

Migrate the table `db.sample` in Spark's default catalog to an Iceberg table and add a property 'foo' set to 'bar':

```sql
CALL catalog_name.system.migrate('spark_catalog.db.sample', map('foo', 'bar'))
```

Migrate `db.sample` in the current catalog to an Iceberg table without adding any additional properties:
```sql
CALL catalog_name.system.migrate('db.sample')
```

### `add_files`

Attempts to directly add files from a Hive or file based table into a given Iceberg table. Unlike migrate or
snapshot, `add_files` can import files from a specific partition or partitions and does not create a new Iceberg table.
This command will create metadata for the new files and will not move them. This procedure will not analyze the schema 
of the files to determine if they actually match the schema of the Iceberg table. Upon completion, the Iceberg table 
will then treat these files as if they are part of the set of files  owned by Iceberg. This means any subsequent 
`expire_snapshot` calls will be able to physically delete the added files. This method should not be used if 
`migrate` or `snapshot` are possible.

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Table which will have files added to|
| `source_table`| ✔️  | string | Table where files should come from, paths are also possible in the form of \`file_format\`.\`path\` |
| `partition_filter`  | ️   | map<string, string> | A map of partitions in the source table to import from |

Warning : Schema is not validated, adding files with different schema to the Iceberg table will cause issues.

Warning : Files added by this method can be physically deleted by Iceberg operations

#### Examples

Add the files from table `db.src_table`, a Hive or Spark table registered in the session Catalog, to Iceberg table
`db.tbl`. Only add files that exist within partitions where `part_col_1` is equal to `A`.
```sql
CALL spark_catalog.system.add_files(
table => 'db.tbl',
source_table => 'db.src_tbl',
partition_filter => map('part_col_1', 'A')
)
```

Add files from a `parquet` file based table at location `path/to/table` to the Iceberg table `db.tbl`. Add all
files regardless of what partition they belong to.
```sql
CALL spark_catalog.system.add_files(
  table => 'db.tbl',
  source_table => '`parquet`.`path/to/table`'
)
```

## Metadata information

### `ancestors_of`

Report the live snapshot IDs of parents of a specified snapshot

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to report live snapshot IDs |
| `snapshot_id` |  ️  | long | Use a specified snapshot to get the live snapshot IDs of parents |

> tip : Using snapshot_id
> 
> Given snapshots history with roll back to B and addition of C' -> D'
> ```shell
> A -> B - > C -> D
>       \ -> C' -> (D')
> ```
> Not specifying the snapshot ID would return A -> B -> C' -> D', while providing the snapshot ID of
> D as an argument would return A-> B -> C -> D

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `snapshot_id` | long | the ancestor snapshot id |
| `timestamp` | long | snapshot creation time |

#### Examples

Get all the snapshot ancestors of current snapshots(default)
```sql
CALL spark_catalog.system.ancestors_of('db.tbl')
```

Get all the snapshot ancestors by a particular snapshot
```sql
CALL spark_catalog.system.ancestors_of('db.tbl', 1)
CALL spark_catalog.system.ancestors_of(snapshot_id => 1, table => 'db.tbl')
```
