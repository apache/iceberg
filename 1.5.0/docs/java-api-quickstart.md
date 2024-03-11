---
title: "Java Quickstart"
search:
  exclude: true
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

# Java API Quickstart

## Create a table

Tables are created using either a [`Catalog`](../../javadoc/{{ icebergVersion }}/index.html?org/apache/iceberg/catalog/Catalog.html) or an implementation of the [`Tables`](../../javadoc/{{ icebergVersion }}/index.html?org/apache/iceberg/Tables.html) interface.

### Using a Hive catalog

The Hive catalog connects to a Hive metastore to keep track of Iceberg tables.
You can initialize a Hive catalog with a name and some properties.
(see: [Catalog properties](configuration.md#catalog-properties))


```java
import java.util.HashMap
import java.util.Map

import org.apache.iceberg.hive.HiveCatalog;

HiveCatalog catalog = new HiveCatalog();
catalog.setConf(spark.sparkContext().hadoopConfiguration());  // Optionally use Spark's Hadoop configuration

Map <String, String> properties = new HashMap<String, String>();
properties.put("warehouse", "...");
properties.put("uri", "...");

catalog.initialize("hive", properties);
```

`HiveCatalog` implements the `Catalog` interface, which defines methods for working with tables, like `createTable`, `loadTable`, `renameTable`, and `dropTable`.
To create a table, pass an `Identifier` and a `Schema` along with other initial metadata:

```java
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

TableIdentifier name = TableIdentifier.of("logging", "logs");
Table table = catalog.createTable(name, schema, spec);

// or to load an existing table, use the following line
Table table = catalog.loadTable(name);
```

The table's [schema](#create-a-schema) and [partition spec](#create-a-partition-spec) are created below.


### Using a Hadoop catalog

A Hadoop catalog doesn't need to connect to a Hive MetaStore, but can only be used with HDFS or similar file systems that support atomic rename. Concurrent writes with a Hadoop catalog are not safe with a local FS or S3. To create a Hadoop catalog:

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;

Configuration conf = new Configuration();
String warehousePath = "hdfs://host:8020/warehouse_path";
HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
```

Like the Hive catalog, `HadoopCatalog` implements `Catalog`, so it also has methods for working with tables, like `createTable`, `loadTable`, and `dropTable`.
                                                                                       
This example creates a table with Hadoop catalog:

```java
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

TableIdentifier name = TableIdentifier.of("logging", "logs");
Table table = catalog.createTable(name, schema, spec);

// or to load an existing table, use the following line
Table table = catalog.loadTable(name);
```

The table's [schema](#create-a-schema) and [partition spec](#create-a-partition-spec) are created below.


### Tables in Spark

Spark can work with table by name using `HiveCatalog`.

```java
// spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
// spark.sql.catalog.hive_prod.type = hive
spark.table("logging.logs");
```

Spark can also load table created by `HadoopCatalog` by path.
```java
spark.read.format("iceberg").load("hdfs://host:8020/warehouse_path/logging/logs");
```

## Schemas

### Create a schema

This example creates a schema for a `logs` table:

```java
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

Schema schema = new Schema(
      Types.NestedField.required(1, "level", Types.StringType.get()),
      Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
      Types.NestedField.required(3, "message", Types.StringType.get()),
      Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
    );
```

When using the Iceberg API directly, type IDs are required. Conversions from other schema formats, like Spark, Avro, and Parquet will automatically assign new IDs.

When a table is created, all IDs in the schema are re-assigned to ensure uniqueness.

### Convert a schema from Avro

To create an Iceberg schema from an existing Avro schema, use converters in `AvroSchemaUtil`:

```java
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.iceberg.avro.AvroSchemaUtil;

Schema avroSchema = new Parser().parse("{\"type\": \"record\" , ... }");
Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
```

### Convert a schema from Spark

To create an Iceberg schema from an existing table, use converters in `SparkSchemaUtil`:

```java
import org.apache.iceberg.spark.SparkSchemaUtil;

Schema schema = SparkSchemaUtil.schemaForTable(sparkSession, tableName);
```

## Partitioning

### Create a partition spec

Partition specs describe how Iceberg should group records into data files. Partition specs are created for a table's schema using a builder.

This example creates a partition spec for the `logs` table that partitions records by the hour of the log event's timestamp and by log level:

```java
import org.apache.iceberg.PartitionSpec;

PartitionSpec spec = PartitionSpec.builderFor(schema)
      .hour("event_time")
      .identity("level")
      .build();
```

For more information on the different partition transforms that Iceberg offers, visit [this page](../../spec.md#partitioning).

## Branching and Tagging

### Creating branches and tags

New branches and tags can be created via the Java library's ManageSnapshots API. 

```java

/* Create a branch test-branch which is retained for 1 week, and the latest 2 snapshots on test-branch will always be retained. 
Snapshots on test-branch which are created within the last hour will also be retained. */

String branch = "test-branch";
table.manageSnapshots()
    .createBranch(branch, 3)
    .setMinSnapshotsToKeep(branch, 2)
    .setMaxSnapshotAgeMs(branch, 3600000)
    .setMaxRefAgeMs(branch, 604800000)
    .commit();

// Create a tag historical-tag at snapshot 10 which is retained for a day
String tag = "historical-tag"
table.manageSnapshots()
    .createTag(tag, 10)
    .setMaxRefAgeMs(tag, 86400000)
    .commit();
```

### Committing to branches

Writing to a branch can be performed by specifying `toBranch` in the operation. For the full list refer to [UpdateOperations](api.md#update-operations). 
```java
// Append FILE_A to branch test-branch 
String branch = "test-branch";

table.newAppend()
    .appendFile(FILE_A)
    .toBranch(branch)
    .commit();


// Perform row level updates on "test-branch"
table.newRowDelta()
    .addRows(DATA_FILE)
    .addDeletes(DELETES)
    .toBranch(branch)
    .commit();


// Perform a rewrite operation replacing SMALL_FILE_1 and SMALL_FILE_2 on "test-branch" with compactedFile.
table.newRewrite()
    .rewriteFiles(ImmutableSet.of(SMALL_FILE_1, SMALL_FILE_2), ImmutableSet.of(compactedFile))
    .toBranch(branch)
    .commit();

```

### Reading from branches and tags
Reading from a branch or tag can be done as usual via the Table Scan API, by passing in a branch or tag in the `useRef` API. When a branch is passed in, the snapshot that's used is the head of the branch. Note that currently reading from a branch and specifying an `asOfSnapshotId` in the scan is not supported. 

```java
// Read from the head snapshot of test-branch
TableScan branchRead = table.newScan().useRef("test-branch");

// Read from the snapshot referenced by audit-tag
TableScan tagRead = table.newScan().useRef("audit-tag");
```

### Replacing and fast forwarding branches and tags

The snapshots which existing branches and tags point to can be updated via the `replace` APIs. The fast forward operation is similar to git fast-forwarding. Fast forward can be used to advance a target branch to the head of a source branch or a tag when the target branch is an ancestor of the source. For both fast forward and replace, retention properties of the target branch are maintained by default.

```java

// Update "test-branch" to point to snapshot 4
table.manageSnapshots()
     .replaceBranch(branch, 4)
     .commit()

String tag = "audit-tag";
// Replace "audit-tag" to point to snapshot 3 and update its retention
table.manageSnapshots()
     .replaceBranch(tag, 4)
     .setMaxRefAgeMs(1000)
     .commit()


```

### Updating retention properties

Retention properties for branches and tags can be updated as well.
Use the setMaxRefAgeMs for updating the retention property of the branch or tag itself. Branch snapshot retention properties can be updated via the `setMinSnapshotsToKeep` and `setMaxSnapshotAgeMs` APIs. 

```java
String branch = "test-branch";
// Update retention properties for test-branch
table.manageSnapshots()
    .setMinSnapshotsToKeep(branch, 10)
    .setMaxSnapshotAgeMs(branch, 7200000)
    .setMaxRefAgeMs(branch, 604800000)
    .commit();

// Update retention properties for test-tag
table.manageSnapshots()
    .setMaxRefAgeMs("test-tag", 604800000)
    .commit();
```

### Removing branches and tags

Branches and tags can be removed via the `removeBranch` and `removeTag` APIs respectively

```java
// Remove test-branch
table.manageSnapshots()
     .removeBranch("test-branch")
     .commit()

// Remove test-tag
table.manageSnapshots()
     .removeTag("test-tag")
     .commit()
```
