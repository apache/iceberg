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

To use Iceberg in Spark, first configure [Spark catalogs](./spark-configuration.md). Stored procedures are only available when using [Iceberg SQL extensions](./spark-configuration.md#sql-extensions) in Spark 3.x.

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

**Note** this procedure invalidates all cached Spark plans that reference the affected table.

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

**Note** this procedure invalidates all cached Spark plans that reference the affected table.

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

Roll back `db.sample` to a day ago
```sql
CALL catalog_name.system.rollback_to_timestamp('db.sample', date_sub(current_date(), 1))
```

### `set_current_snapshot`

Sets the current snapshot ID for a table.

Unlike rollback, the snapshot is not required to be an ancestor of the current table state.

**Note** this procedure invalidates all cached Spark plans that reference the affected table.

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

**Note** this procedure invalidates all cached Spark plans that reference the affected table.

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

Many [maintenance actions](maintenance.md) can be performed using Iceberg stored procedures.

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

#### Output

| Output Name | Type | Description |
| ------------|------|-------------|
| `deleted_data_files_count` | long | Number of data files deleted by this operation |
| `deleted_manifest_files_count` | long | Number of manifest files deleted by this operation |
| `deleted_manifest_lists_count` | long | Number of manifest List files deleted by this operation |

#### Examples

Remove snapshots older than 10 days ago, but retain the last 100 snapshots:

```sql
CALL hive_prod.system.expire_snapshots('db.sample', date_sub(current_date(), 10), 100)
```

Erase all snapshots older than the current timestamp but retain the last 5 snapshots:

```sql
CALL hive_prod.system.expire_snapshots(table => 'db.sample', older_than => now(), retain_last => 5)
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

### `rewrite_manifests`

Rewrite manifests for a table to optimize scan planning.

Data files in manifests are sorted by fields in the partition spec. This procedure runs in parallel using a Spark job.

See the [`RewriteManifestsAction` Javadoc](./javadoc/{{ versions.iceberg }}/org/apache/iceberg/actions/RewriteManifestsAction.html)
to see more configuration options.

**Note** this procedure invalidates all cached Spark plans that reference the affected table.

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

**Note** Because tables created by `snapshot` are not the sole owners of their data files, they are prohibited from
actions like `expire_snapshots` which would physically delete data files. Iceberg deletes, which only effect metadata,
are still allowed. In addition, any operations which affect the original data files will disrupt the Snapshot's 
integrity. DELETE statements executed against the original Hive table will remove original data files and the
`snapshot` table will no longer be able to access them.

See [`migrate`](#migrate-table-procedure) to replace an existing table with an Iceberg table.

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

#### Usage

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
| `table`       | ✔️  | string | Name of the table to migrate |
| `properties`  | ️   | map<string, string> | Properties for the new Iceberg table |

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

