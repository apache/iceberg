# Iceberg Java API

## Tables

The main purpose of the Iceberg API is to manage table metadata, like schema, partition spec, metadata, and data files that store table data.

Table metadata and operations are accessed through the `Table` interface. This interface will return table information, 

### Table metadata

The [`Table` interface](/javadoc/master/index.html?org/apache/iceberg/Table.html) provides access table metadata:

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


### Update operations

`Table` also exposes operations that update the table. These operations use a builder pattern, [`PendingUpdate`](/javadoc/master/index.html?org/apache/iceberg/PendingUpdate.html), that commits when `PendingUpdate#commit` is called.

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
* `rollback` -- roll the table state back by pointing current to a specific snapshot

### Transactions

## Types

Iceberg data types are located in the [`org.apache.iceberg.types` package](/javadoc/master/index.html?org/apache/iceberg/types/package-summary.html).

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
    1, Types.IntegerType.get(),
    2, Types.StringType.get()
  )
```
```java
// array<1 element: int>
ListType list = ListType.ofRequired(1, IntegerType.get());
```


## Expressions

Iceberg's expressions are used to configure table scans. To create expressions, use the factory methods in [`Expressions`](/javadoc/master/index.html?org/apache/iceberg/expressions/Expressions.html).

Supported predicate expressions are:

* `isNull`
* `notNull`
* `equal`
* `notEqual`
* `lessThan`
* `lessThanOrEqual`
* `greaterThan`
* `greaterThanOrEqual`

Supported expression operations are:

* `and`
* `or`
* `not`

Constant expressions are:

* `alwaysTrue`
* `alwaysFalse`

### Expression binding

When created, expressions are unbound. Before an expression is used, it will be bound to a data type to find the field ID the expression name represents, and to convert predicate literals.

For example, before using the expression `lessThan("x", 10)`, Iceberg needs to determine which column `"x"` refers to and convert `10` to that colunn's data type.

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
* `iceberg-core` contains implementations of the Iceberg API and support for Avro data files, **this is what processing engines should depend on**
* `iceberg-parquet` is an optional module for working with tables backed by Parquet files
* `iceberg-orc` is an optional module for working with tables backed by ORC files (*experimental*)
* `iceberg-hive` is an implementation of iceberg tables backed by hive metastore thrift client

This project Iceberg also has modules for adding Iceberg support to processing engines:

* `iceberg-spark` is an implementation of Spark's Datasource V2 API for Iceberg (use iceberg-runtime for a shaded version)
* `iceberg-data` is a client library used to read Iceberg tables from JVM applications
* `iceberg-pig` is an implementation of Pig's LoadFunc API for Iceberg
* `iceberg-runtime` generates a shaded runtime jar for Spark to integrate with iceberg tables
* `iceberg-presto-runtime` generates a shaded runtime jar that is used by presto to integrate with iceberg tables

