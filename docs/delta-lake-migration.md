---
title: "Delta Lake Migration"
url: delta-lake-migration
menu:
  main:
    parent: "Migration"
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

# Delta Lake Table Migration
Delta Lake is a table format that supports Parquet file format and provides time travel and versioning features. When migrating data from Delta Lake to Iceberg,
it is common to migrate all snapshots to maintain the history of the data.

Currently, Iceberg only supports the Snapshot Table action for migrating from Delta Lake to Iceberg tables. It is done via the `iceberg-delta-lake` module
by using [Delta Standalone](https://docs.delta.io/latest/delta-standalone.html) to read logs of Delta lake tables.
Since Delta Lake tables maintain snapshots, all available snapshots will be committed to the new Iceberg table as transactions in order.
For Delta Lake tables, any additional data files added after the initial migration will be included in their corresponding snapshots and subsequently added to the new Iceberg table using the Add Snapshot action.
The Add Snapshot action, a variant of the Add File action, is still under development.

## Enabling Migration from Delta Lake to Iceberg
The `iceberg-delta-lake` module is not bundled with Spark and Flink engine runtimes. To enable migration from delta lake features, the minimum required dependencies are:
- [iceberg-delta-lake](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-delta-lake/1.2.0/iceberg-delta-lake-1.2.0.jar)
- [delta-standalone-0.6.0](https://repo1.maven.org/maven2/io/delta/delta-standalone_2.13/0.6.0/delta-standalone_2.13-0.6.0.jar)
- [delta-storage-2.2.0](https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar)

### Compatibilities
The module is built and tested with `Delta Standalone:0.6.0` and supports Delta Lake tables with the following protocol version:
* `minReaderVersion`: 1
* `minWriterVersion`: 2

Please refer to [Delta Lake Table Protocol Versioning](https://docs.delta.io/latest/versioning.html) for more details about Delta Lake protocol versions.

### API
The `iceberg-delta-lake` module provides an interface named `DeltaLakeToIcebergMigrationActionsProvider`, which contains actions that helps converting from Delta Lake to Iceberg.
The supported actions are:
* `snapshotDeltaLakeTable`: snapshot an existing Delta Lake table to an Iceberg table

### Default Implementation
The `iceberg-delta-lake` module also provides a default implementation of the interface which can be accessed by
```java
DeltaLakeToIcebergMigrationActionsProvider defaultActions = DeltaLakeToIcebergMigrationActionsProvider.defaultActions()
```

## Snapshot Delta Lake Table to Iceberg
The action `snapshotDeltaLakeTable` reads the Delta Lake table's most recent snapshot and converts it to a new Iceberg table with the same schema and partitioning in one iceberg transaction.
The original Delta Lake table remains unchanged.

The newly created table can be changed or written to without affecting the source table, but the snapshot uses the original table's data files.
Existing data files are added to the Iceberg table's metadata and can be read using a name-to-id mapping created from the original table schema.

When inserts or overwrites run on the snapshot, new files are placed in the snapshot table's location. The location is default to be the same as that
of the source Delta Lake Table. Users can also specify a different location for the snapshot table.

{{< hint info >}}
Because tables created by `snapshotDeltaLakeTable` are not the sole owners of their data files, they are prohibited from
actions like `expire_snapshots` which would physically delete data files. Iceberg deletes, which only effect metadata,
are still allowed. In addition, any operations which affect the original data files will disrupt the Snapshot's
integrity. DELETE statements executed against the original Delta Lake table will remove original data files and the
`snapshotDeltaLakeTable` table will no longer be able to access them.
{{< /hint >}}

#### Usage
| Required Input                                  | Configured By                                                                                                                                                                                             |
|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Source Table Location                           | Argument [`sourceTableLocation`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/delta/DeltaLakeToIcebergMigrationActionsProvider.html#snapshotDeltaLakeTable(java.lang.String))             | 
| New Iceberg Table Identifier                    | Configuration API [`as`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/delta/SnapshotDeltaLakeTable.html#as(org.apache.iceberg.catalog.TableIdentifier))                                   |
| Iceberg Catalog To Create New Iceberg Table     | Configuration API [`icebergCatalog`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/delta/SnapshotDeltaLakeTable.html#icebergCatalog(org.apache.iceberg.catalog.Catalog))                   |
| Hadoop Configuration To Access Delta Lake Table | Configuration API [`deltaLakeConfiguration`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/delta/SnapshotDeltaLakeTable.html#deltaLakeConfiguration(org.apache.hadoop.conf.Configuration)) |

For detailed usage and other optional configurations, please refer to the [SnapshotDeltaLakeTable API](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/delta/SnapshotDeltaLakeTable.html)

#### Output
| Output Name | Type | Description |
| ------------|------|-------------|
| `imported_files_count` | long | Number of files added to the new table |

#### Added Table Properties
The following table properties are added to the Iceberg table to be created by default:

| Property Name                 | Value                                     | Description                                                        |
|-------------------------------|-------------------------------------------|--------------------------------------------------------------------|
| `snapshot_source`             | `delta`                                   | Indicates that the table is snapshot from a delta lake table       |
| `original_location`           | location of the delta lake table          | The absolute path to the location of the original delta lake table |
| `schema.name-mapping.default` | JSON name mapping derived from the schema | The name mapping string used to read Delta Lake table's data files |

#### Examples
```java
sourceDeltaLakeTableLocation = "s3://my-bucket/delta-table";
destTableIdentifier = TableIdentifier.of("my_db", "my_table");
destTableLocation = "s3://my-bucket/iceberg-table";
icebergCatalog = ...; // Iceberg Catalog fetched from engines like Spark or created via CatalogUtil.loadCatalog
hadoopConf = ...; // Hadoop Configuration fetched from engines like Spark and have proper file system configuration to access the Delta Lake table.
    
DeltaLakeToIcebergMigrationActionsProvider.defaultActions()
    .snapshotDeltaLakeTable(sourceDeltaLakeTableLocation)
    .as(destTableIdentifier)
    .icebergCatalog(icebergCatalog)
    .tableLocation(destTableLocation)
    .deltaLakeConfiguration(hadoopConf)
    .tableProperty("my_property", "my_value")
    .execute();
```

## Migrate Delta Lake Table To Iceberg
**Not Yet Support**. This action should read the Delta Lake table's most recent snapshot and convert it to a new Iceberg table with the same name, schema and partitioning in one iceberg transaction. The source Delta Lake table should be dropped as the completion of this action.

## Add Files From Delta Lake Table to Iceberg
**Not Yet Support**. This action should add files from a Delta version of a Delta Lake table to an existing Iceberg table
