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

# API Quickstart

## Create a table

Tables are created using either a [`Catalog`](/javadoc/master/index.html?org/apache/iceberg/catalog/Catalog.html) or an implementation of the [`Tables`](/javadoc/master/index.html?org/apache/iceberg/Tables.html) interface.

### Using a Hive catalog

The Hive catalog connects to a Hive MetaStore to keep track of Iceberg tables. This example uses Spark's Hadoop configuration to get a Hive catalog:

```scala
import org.apache.iceberg.hive.HiveCatalog

val catalog = new HiveCatalog(spark.sparkContext.hadoopConfiguration)
```

The `Catalog` interface defines methods for working with tables, like `createTable`, `loadTable`, `renameTable`, and `dropTable`.

To create a table, pass an `Identifier` and a `Schema` along with other initial metadata:

```scala
val name = TableIdentifier.of("logging", "logs")
val table = catalog.createTable(name, schema, spec)

// write into the new logs table with Spark 2.4
logsDF.write
    .format("iceberg")
    .save("logging.logs")
```

The logs [schema](#create-a-schema) and [partition spec](#create-a-partition-spec) are created below.

### Using Hadoop tables

Iceberg also supports tables that are stored in a directory in HDFS or the local file system. Directory tables don't support all catalog operations, like rename, so they use the `Tables` interface instead of `Catalog`.

To create a table in HDFS, use `HadoopTables`:

```scala
import org.apache.iceberg.hadoop.HadoopTables

val tables = new HadoopTables(conf)

val table = tables.create(schema, spec, "hdfs:/tables/logging/logs")

// write into the new logs table with Spark 2.4
logsDF.write
    .format("iceberg")
    .save("hdfs:/tables/logging/logs")
```

!!! Warning
    Hadoop tables shouldn't be used with file systems that do not support atomic rename. Iceberg relies on rename to synchronize concurrent commits for directory tables.

### Tables in Spark

Spark uses both `HiveCatalog` and `HadoopTables` to load tables. Hive is used when the identifier passed to `load` or `save` is not a path, otherwise Spark assumes it is a path-based table.

To read and write to tables from Spark see:

* [Reading a table in Spark](../spark#reading-an-iceberg-table)
* [Appending to a table in Spark](../spark#appending-data)
* [Overwriting data in a table in Spark](../spark#overwriting-data)


## Schemas

### Create a schema

This example creates a schema for a `logs` table:

```scala
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types._

val schema = new Schema(
    NestedField.required(1, "level", StringType.get()),
    NestedField.required(2, "event_time", TimestampType.withZone()),
    NestedField.required(3, "message", StringType.get()),
    NestedField.optional(4, "call_stack", ListType.ofRequired(5, StringType.get()))
  )
```

When using the Iceberg API directly, type IDs are required. Conversions from other schema formats, like Spark, Avro, and Parquet will automatically assign new IDs.

When a table is created, all IDs in the schema are re-assigned to ensure uniqueness.

### Convert a schema from Avro

To create an Iceberg schema from an existing Avro schema, use converters in `AvroSchemaUtil`:

```scala
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.avro.Schema.Parser

val avroSchema = new Parser().parse(
    """{ "type": "record", "name": "com.example.AvroType",
      |  "fields": [ ... ]
      |}""".stripMargin

val schema = AvroSchemaUtil.convert(avroSchema)
```

### Convert a schema from Spark

To create an Iceberg schema from an existing table, use converters in `SparkSchemaUtil`:

```scala
import org.apache.iceberg.spark.SparkSchemaUtil

val schema = SparkSchemaUtil.convert(spark.table("db.table").schema)
```


## Partitioning

### Create a partition spec

Partition specs describe how Iceberg should group records into data files. Partition specs are created for a table's schema using a builder.

This example creates a partition spec for the `logs` table that partitions records by the hour of the log event's timestamp and by log level:

```scala
import org.apache.iceberg.PartitionSpec

val spec = PartitionSpec.builderFor(schema)
                        .hour("event_time")
                        .identity("level")
                        .build()
```
