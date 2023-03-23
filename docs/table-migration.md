---
title: "Table Migration"
url: table-migration
weight: 400
menu: main
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
# Hive and Spark Table Migration
Iceberg supports converting Hive and Spark tables to Iceberg tables through Spark Procedures. For specific instructions on how to use the procedures, please refer to the [Table Migration via Spark Procedures](../spark-procedures/#table-migration) page.

# Delta Lake Table Migration
Iceberg supports converting Delta Lake tables to Iceberg tables through the `iceberg-delta-lake` module
by using [Delta Standalone](https://docs.delta.io/latest/delta-standalone.html) to read logs of Delta lake tables.


## Enabling Migration from Delta Lake to Iceberg
The `iceberg-delta-lake` module is not bundled with Spark and Flink engine runtimes. Users need to manually include this module in their environment to enable the conversion.
Also, users need to provide [Delta Standalone](https://github.com/delta-io/connectors/releases/tag/v0.6.0) and [Delta Storage](https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/)
because they are what `iceberg-delta-lake` uses to read Delta Lake tables.

## Compatibilities
The module is built and tested with `Delta Standalone:0.6.0` and supports Delta Lake tables with the following protocol version:
* `minReaderVersion`: 1
* `minWriterVersion`: 2

Please refer to [Delta Lake Table Protocol Versioning](https://docs.delta.io/latest/versioning.html) for more details about Delta Lake protocol versions.

For delta lake table contains `TimestampType` columns, please make sure to set table property `read.parquet.vectorization.enabled` to `false` since the vectorized reader doesn't support `INT96` yet.
Such support is under development in the [PR#6962](https://github.com/apache/iceberg/pull/6962)

## API
The `iceberg-delta-lake` module provides an interface named `DeltaLakeToIcebergMigrationActionsProvider`, which contains actions that helps converting from Delta Lake to Iceberg.
The supported actions are:
* `snapshotDeltaLakeTable`: snapshot an existing Delta Lake table to an Iceberg table

## Default Implementation
The `iceberg-delta-lake` module also provides a default implementation of the interface which can be accessed by
```java
DeltaLakeToIcebergMigrationActionsProvider defaultActions = DeltaLakeToIcebergMigrationActionsProvider.defaultActions()
```

### `snapshotDeltaLakeTable`
The action reads the Delta Lake table's most recent snapshot and converts it to a new Iceberg table with the same schema and partitioning in one iceberg transaction.
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

#### Arguments
The delta table's location is required to be provided when initializing the action.

| Argument Name | Required? | Type | Description |
|---------------|-----------|------|-------------|
|`sourceTableLocation` | Yes | String | The location of the source Delta Lake table | 

#### Configurations
The configurations can be gave via method chaining

| Method Name | Arguments      | Required? | Type                                       | Description                                                                                                  |
|---------------------------|----------------|-----------|--------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| `as`                      | `identifier`   | Yes       | org.apache.iceberg.catalog.TableIdentifier | The identifier of the Iceberg table to be created.                                                           |
| `icebergCatalog`          | `catalog`      | Yes       | org.apache.iceberg.catalog.Catalog         | The Iceberg catalog for the Iceberg table to be created                                                      |
| `deltaLakeConfiguration`  | `conf`         | Yes       | org.apache.hadoop.conf.Configuration       | The Hadoop Configuration to access Delta Lake Table's log and datafiles                                      |
| `tableLocation`           | `location`     | No        | String                                     | The location of the Iceberg table to be created. Defaults to the same location as the given Delta Lake table |
| `tableProperty`           | `name`,`value` | No        | String, String                             | A property entry to add to the Iceberg table to be created                                                   |
| `tableProperties`         | `properties`   | No        | Map<String, String>                        | Properties to add to the the Iceberg table to be created                                                     |

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
Snapshot a Delta Lake table located at `s3://my-bucket/delta-table` to an Iceberg table named `my_table` in the `my_db` database in the `my_catalog` catalog.
```java
DeltaLakeToIcebergMigrationActionsProvider.defaultActions()
    .snapshotDeltaLakeTable("s3://my-bucket/delta-table")
    .as("my_db.my_table")
    .icebergCatalog("my_catalog")
    .deltaLakeConfiguration(new Configuration())
    .execute();
```
Snapshot a Delta Lake table located at `s3://my-bucket/delta-table` to an Iceberg table named `my_table` in the `my_db` database in the `my_catalog` catalog at a manually
specified location `s3://my-bucket/snapshot-loc`. Also, add a table property `my_property` with value `my_value` to the Iceberg table.
```java
DeltaLakeToIcebergMigrationActionsProvider.defaultActions()
    .snapshotDeltaLakeTable("s3://my-bucket/delta-table")
    .as("my_db.my_table")
    .icebergCatalog("my_catalog")
    .deltaLakeConfiguration(new Configuration())
    .tableLocation("s3://my-bucket/snapshot-loc")
    .tableProperty("my_property", "my_value")
    .execute();
```
