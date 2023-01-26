---
title: "Java API"
url: api
aliases:
    - "java/api"
menu:
    main:
        parent: "API"
        weight: 200
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

# Iceberg Java API

## Tables

The main purpose of the Iceberg API is to manage table metadata, like schema, partition spec, metadata, and data files that store table data.

Table metadata and operations are accessed through the `Table` interface. This interface will return table information.

### Table metadata

The [`Table` interface](../../../javadoc/{{% icebergVersion %}}/index.html?org/apache/iceberg/Table.html) provides access to the table metadata:

* `schema` returns the current table [schema](../schemas)
* `spec` returns the current table partition spec
* `properties` returns a map of key-value [properties](../configuration)
* `currentSnapshot` returns the current table snapshot
* `snapshots` returns all valid snapshots for the table
* `snapshot(id)` returns a specific snapshot by ID
* `location` returns the table's base location

Tables also provide `refresh` to update the table to the latest version, and expose helpers:

* `io` returns the `FileIO` used to read and write table files
* `locationProvider` returns a `LocationProvider` used to create paths for data and metadata files


### Scanning

#### File level

Iceberg table scans start by creating a `TableScan` object with `newScan`.

```java
TableScan scan = table.newScan();
```

To configure a scan, call `filter` and `select` on the `TableScan` to get a new `TableScan` with those changes.

```java
TableScan filteredScan = scan.filter(Expressions.equal("id", 5))
```

Calls to configuration methods create a new `TableScan` so that each `TableScan` is immutable and won't change unexpectedly if shared across threads.

When a scan is configured, `planFiles`, `planTasks`, and `schema` are used to return files, tasks, and the read projection.

```java
TableScan scan = table.newScan()
    .filter(Expressions.equal("id", 5))
    .select("id", "data");

Schema projection = scan.schema();
Iterable<CombinedScanTask> tasks = scan.planTasks();
```

Use `asOfTime` or `useSnapshot` to configure the table snapshot for time travel queries.

#### Row level

Iceberg table scans start by creating a `ScanBuilder` object with `IcebergGenerics.read`.

```java
ScanBuilder scanBuilder = IcebergGenerics.read(table)
```

To configure a scan, call `where` and `select` on the `ScanBuilder` to get a new `ScanBuilder` with those changes.

```java
scanBuilder.where(Expressions.equal("id", 5))
```

When a scan is configured, call method `build` to execute scan. `build` return `CloseableIterable<Record>`

```java
CloseableIterable<Record> result = IcebergGenerics.read(table)
        .where(Expressions.lessThan("id", 5))
        .build();
```
where `Record` is Iceberg record for iceberg-data module `org.apache.iceberg.data.Record`.

### Update operations

`Table` also exposes operations that update the table. These operations use a builder pattern, [`PendingUpdate`](../../../javadoc/{{% icebergVersion %}}/index.html?org/apache/iceberg/PendingUpdate.html), that commits when `PendingUpdate#commit` is called.

For example, updating the table schema is done by calling `updateSchema`, adding updates to the builder, and finally calling `commit` to commit the pending changes to the table:

```java
table.updateSchema()
    .addColumn("count", Types.LongType.get())
    .commit();
```

Available operations to update a table are:

* `updateSchema` -- update the table schema
* `updateProperties` -- update table properties
* `updateLocation` -- update the table's base location
* `newAppend` -- used to append data files
* `newFastAppend` -- used to append data files, will not compact metadata
* `newOverwrite` -- used to append data files and remove files that are overwritten
* `newDelete` -- used to delete data files
* `newRewrite` -- used to rewrite data files; will replace existing files with new versions
* `newTransaction` -- create a new table-level transaction
* `rewriteManifests` -- rewrite manifest data by clustering files, for faster scan planning
* `rollback` -- rollback the table state to a specific snapshot

### Transactions

Transactions are used to commit multiple table changes in a single atomic operation. A transaction is used to create individual operations using factory methods, like `newAppend`, just like working with a `Table`. Operations created by a transaction are committed as a group when `commitTransaction` is called.

For example, deleting and appending a file in the same transaction:
```java
Transaction t = table.newTransaction();

// commit operations to the transaction
t.newDelete().deleteFromRowFilter(filter).commit();
t.newAppend().appendFile(data).commit();

// commit all the changes to the table
t.commitTransaction();
```

### WriteData

The java api can write data into iceberg table.

First write data to the data file, then submit the data file, the data you write will take effect in the table.

For example, add 1000 pieces of data to the table.

```java
/**
 * The structure of this table is the same as the demo table in java-api-quickstart
 *
 * Schema schema = new Schema(
 *       Types.NestedField.required(1, "level", Types.StringType.get()),
 *       Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
 *       Types.NestedField.required(3, "message", Types.StringType.get()),
 *       Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
 *   );
 * PartitionSpec spec = PartitionSpec.builderFor(schema)
 *       .hour("event_time")
 *       .identity("level")
 *       .build();
 */

GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());
int partitionId = 1, taskId = 1;
FileFormat fileFormat = FileFormat.valueOf(table.properties().getOrDefault("write.format.default", "parquet").toUpperCase());
OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId).format(fileFormat).build();
// TaskWriter write records into files. (the same is ok for unpartiton table)
long targetFileSizeInBytes = 50L * 1024 * 1024;
GenericTaskWriter<Record> genericTaskWriter = new GenericTaskWriter(table.spec(), fileFormat, appenderFactory, outputFileFactory, table.io(), targetFileSizeInBytes);

GenericRecord genericRecord = GenericRecord.create(table.schema());
// assume write 1000 records
for (int i = 0; i < 1000; i++) {
    GenericRecord record = genericRecord.copy();
    record.setField("level",  i % 6 == 0 ? "error" : "info");
    record.setField("event_time", OffsetDateTime.now());
    record.setField("message", "Iceberg is a great table format");
    record.setField("call_stack", Collections.singletonList("NullPointerException"));
    genericTaskWriter.write(record);
}
// after the data file is written above,
// the written data file is submitted to the metadata of the table through Table's Api.
AppendFiles appendFiles = table.newAppend();
for (DataFile dataFile : genericTaskWriter.dataFiles()) {
    appendFiles.appendFile(dataFile);
}
// The submitted file generates a snapshot and submit it.
Snapshot res = appendFiles.apply(); // Of course, you can skip apply to commit directly.
appendFiles.commit();
```

## Types

Iceberg data types are located in the [`org.apache.iceberg.types` package](../../../javadoc/{{% icebergVersion %}}/index.html?org/apache/iceberg/types/package-summary.html).

### Primitives

Primitive type instances are available from static methods in each type class. Types without parameters use `get`, and types like `decimal` use factory methods:

```java
Types.IntegerType.get()    // int
Types.DoubleType.get()     // double
Types.DecimalType.of(9, 2) // decimal(9, 2)
```

### Nested types

Structs, maps, and lists are created using factory methods in type classes.

Like struct fields, map keys or values and list elements are tracked as nested fields. Nested fields track [field IDs](../evolution#correctness) and nullability.

Struct fields are created using `NestedField.optional` or `NestedField.required`. Map value and list element nullability is set in the map and list factory methods.

```java
// struct<1 id: int, 2 data: optional string>
StructType struct = Struct.of(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "data", Types.StringType.get())
  )
```
```java
// map<1 key: int, 2 value: optional string>
MapType map = MapType.ofOptional(
    1, 2,
    Types.IntegerType.get(),
    Types.StringType.get()
  )
```
```java
// array<1 element: int>
ListType list = ListType.ofRequired(1, IntegerType.get());
```


## Expressions

Iceberg's expressions are used to configure table scans. To create expressions, use the factory methods in [`Expressions`](../../../javadoc/{{% icebergVersion %}}/index.html?org/apache/iceberg/expressions/Expressions.html).

Supported predicate expressions are:

* `isNull`
* `notNull`
* `equal`
* `notEqual`
* `lessThan`
* `lessThanOrEqual`
* `greaterThan`
* `greaterThanOrEqual`
* `in`
* `notIn`
* `startsWith`
* `notStartsWith`

Supported expression operations are:

* `and`
* `or`
* `not`

Constant expressions are:

* `alwaysTrue`
* `alwaysFalse`

### Expression binding

When created, expressions are unbound. Before an expression is used, it will be bound to a data type to find the field ID the expression name represents, and to convert predicate literals.

For example, before using the expression `lessThan("x", 10)`, Iceberg needs to determine which column `"x"` refers to and convert `10` to that column's data type.

If the expression could be bound to the type `struct<1 x: long, 2 y: long>` or to `struct<11 x: int, 12 y: int>`.

### Expression example

```java
table.newScan()
    .filter(Expressions.greaterThanOrEqual("x", 5))
    .filter(Expressions.lessThan("x", 10))
```


## Modules

Iceberg table support is organized in library modules:

* `iceberg-common` contains utility classes used in other modules
* `iceberg-api` contains the public Iceberg API, including expressions, types, tables, and operations
* `iceberg-arrow` is an implementation of the Iceberg type system for reading and writing data stored in Iceberg tables using Apache Arrow as the in-memory data format
* `iceberg-aws` contains implementations of the Iceberg API to be used with tables stored on AWS S3 and/or for tables defined using the AWS Glue data catalog
* `iceberg-core` contains implementations of the Iceberg API and support for Avro data files, **this is what processing engines should depend on**
* `iceberg-parquet` is an optional module for working with tables backed by Parquet files
* `iceberg-orc` is an optional module for working with tables backed by ORC files (*experimental*)
* `iceberg-hive-metastore` is an implementation of Iceberg tables backed by the Hive metastore Thrift client

This project Iceberg also has modules for adding Iceberg support to processing engines and associated tooling:

* `iceberg-spark` is an implementation of Spark's Datasource V2 API for Iceberg with submodules for each spark versions (use runtime jars for a shaded version)
* `iceberg-flink` is an implementation of Flink's Table and DataStream API for Iceberg (use iceberg-flink-runtime for a shaded version)
* `iceberg-hive3` is an implementation of Hive 3 specific SerDe's for Timestamp, TimestampWithZone, and Date object inspectors (use iceberg-hive-runtime for a shaded version).
* `iceberg-mr` is an implementation of MapReduce and Hive InputFormats and SerDes for Iceberg (use iceberg-hive-runtime for a shaded version for use with Hive)
* `iceberg-nessie` is a module used to integrate Iceberg table metadata history and operations with [Project Nessie](https://projectnessie.org/)
* `iceberg-data` is a client library used to read Iceberg tables from JVM applications
* `iceberg-pig` is an implementation of Pig's LoadFunc API for Iceberg
* `iceberg-runtime` generates a shaded runtime jar for Spark to integrate with iceberg tables

