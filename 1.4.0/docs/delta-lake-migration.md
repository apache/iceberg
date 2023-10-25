---
title: "Delta Lake Migration"
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

Currently, Iceberg supports the Snapshot Table action for migrating from Delta Lake to Iceberg tables.
Since Delta Lake tables maintain transactions, all available transactions will be committed to the new Iceberg table as transactions in order.
For Delta Lake tables, any additional data files added after the initial migration will be included in their corresponding transactions and subsequently added to the new Iceberg table using the Add Transaction action.
The Add Transaction action, a variant of the Add File action, is still under development.

## Enabling Migration from Delta Lake to Iceberg
The `iceberg-delta-lake` module is not bundled with Spark and Flink engine runtimes. To enable migration from delta lake features, the minimum required dependencies are:

- [iceberg-delta-lake](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-delta-lake/1.2.1/iceberg-delta-lake-1.2.1.jar)
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
The action `snapshotDeltaLakeTable` reads the Delta Lake table's transactions and converts them to a new Iceberg table with the same schema and partitioning in one iceberg transaction.
The original Delta Lake table remains unchanged.

The newly created table can be changed or written to without affecting the source table, but the snapshot uses the original table's data files.
Existing data files are added to the Iceberg table's metadata and can be read using a name-to-id mapping created from the original table schema.

When inserts or overwrites run on the snapshot, new files are placed in the snapshot table's location. The location is default to be the same as that
of the source Delta Lake Table. Users can also specify a different location for the snapshot table.

!!! info
    Because tables created by `snapshotDeltaLakeTable` are not the sole owners of their data files, they are prohibited from
    actions like `expire_snapshots` which would physically delete data files. Iceberg deletes, which only effect metadata,
    are still allowed. In addition, any operations which affect the original data files will disrupt the Snapshot's
    integrity. DELETE statements executed against the original Delta Lake table will remove original data files and the
    `snapshotDeltaLakeTable` table will no longer be able to access them.


#### Usage
| Required Input               | Configured By                                                                                                                                                                                             | Description                                                                     |
|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| Source Table Location        | Argument [`sourceTableLocation`](../../javadoc/latest/org/apache/iceberg/delta/DeltaLakeToIcebergMigrationActionsProvider.html#snapshotDeltaLakeTable(java.lang.String))             | The location of the source Delta Lake table                                     | 
| New Iceberg Table Identifier | Configuration API [`as`](../../javadoc/latest/org/apache/iceberg/delta/SnapshotDeltaLakeTable.html#as(org.apache.iceberg.catalog.TableIdentifier))                                   | The identifier specifies the namespace and table name for the new iceberg table |
| Iceberg Catalog              | Configuration API [`icebergCatalog`](../../javadoc/latest/org/apache/iceberg/delta/SnapshotDeltaLakeTable.html#icebergCatalog(org.apache.iceberg.catalog.Catalog))                   | The catalog used to create the new iceberg table                                |
| Hadoop Configuration         | Configuration API [`deltaLakeConfiguration`](../../javadoc/latest/org/apache/iceberg/delta/SnapshotDeltaLakeTable.html#deltaLakeConfiguration(org.apache.hadoop.conf.Configuration)) | The Hadoop Configuration used to read the source Delta Lake table.              |

For detailed usage and other optional configurations, please refer to the [SnapshotDeltaLakeTable API](../../javadoc/latest/org/apache/iceberg/delta/SnapshotDeltaLakeTable.html)

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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.Catalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.delta.DeltaLakeToIcebergMigrationActionsProvider;

String sourceDeltaLakeTableLocation = "s3://my-bucket/delta-table";
String destTableLocation = "s3://my-bucket/iceberg-table";
TableIdentifier destTableIdentifier = TableIdentifier.of("my_db", "my_table");
Catalog icebergCatalog = ...; // Iceberg Catalog fetched from engines like Spark or created via CatalogUtil.loadCatalog
Configuration hadoopConf = ...; // Hadoop Configuration fetched from engines like Spark and have proper file system configuration to access the Delta Lake table.
    
DeltaLakeToIcebergMigrationActionsProvider.defaultActions()
    .snapshotDeltaLakeTable(sourceDeltaLakeTableLocation)
    .as(destTableIdentifier)
    .icebergCatalog(icebergCatalog)
    .tableLocation(destTableLocation)
    .deltaLakeConfiguration(hadoopConf)
    .tableProperty("my_property", "my_value")
    .execute();
```
