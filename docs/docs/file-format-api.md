---
title: "File Format API"
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

# File Format API

## Overview

The File Format API is the layer the Iceberg Java library uses to read and write data files and delete files. It separates two concerns that used to be mixed together in engine code:

- the **file format** on disk (Parquet, Avro, ORC), which owns parsing, encoding, statistics and encryption, and
- the **object model**, the in-memory representation an engine wants records in (Iceberg's generic `Record`, Spark's `InternalRow` or `ColumnarBatch`, Flink's `RowData`, Arrow batches).

Each supported combination of file format and object model is described by a [`FormatModel`](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/formats/FormatModel.html). Format models are kept in the [`FormatModelRegistry`](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/formats/FormatModelRegistry.html), and engines obtain readers and writers from the registry instead of calling format-specific classes directly. This is what lets a feature such as projection, filter pushdown, delete handling or variant shredding be implemented once per format and be available to every engine.

The API was introduced in Iceberg 1.11.0. It lives in the `org.apache.iceberg.formats` package of `iceberg-core` and is used by the generic data module, the Spark and Flink integrations, the Arrow reader and the MapReduce input format. It is intended for engine integrators and for people adding a new object model or file format; most users of an engine never call it directly.

## Concepts

### FormatModel

A `FormatModel<D, S>` ties one file format to one object model:

- `D` is the record type produced by readers and accepted by writers, for example `Record` or `InternalRow`.
- `S` is the engine's schema type for that record type, for example Spark's `StructType`. Models whose object model needs no extra schema information use `Void`.

The interface has five methods:

| Method | Purpose |
|---|---|
| `format()` | The `FileFormat` this model reads and writes |
| `type()` | The record class `D`; together with `format()` it is the model's key in the registry |
| `schemaType()` | The engine schema class `S` |
| `readBuilder(InputFile)` | A `ReadBuilder` for one file |
| `writeBuilder(EncryptedOutputFile)` | A `ModelWriteBuilder` for one file |

Iceberg ships a `FormatModel` implementation per built-in format: `ParquetFormatModel`, `AvroFormatModel` and `ORCFormatModel`. These are generic over the object model and are instantiated with two functions supplied by the engine: a writer function that builds a format-specific value writer for a schema, and a reader function that builds a format-specific value reader. Engines therefore do not implement `FormatModel` from scratch; they plug their converters into the format implementation Iceberg already has.

### FormatModelRegistry

The registry is a static map keyed by `(FileFormat, record class)`. At class load time it registers the models from every module it can find on the classpath:

- `org.apache.iceberg.data.GenericFormatModels` (generic `Record`)
- `org.apache.iceberg.arrow.vectorized.ArrowFormatModels` (Arrow `ColumnarBatch`)
- `org.apache.iceberg.flink.data.FlinkFormatModels` (Flink `RowData`)
- `org.apache.iceberg.spark.source.SparkFormatModels` (Spark `InternalRow` and `ColumnarBatch`)

A module that is not on the classpath is skipped and logged at `INFO` level; this is normal, for example when `iceberg-orc` is not bundled. Registering a second model for a key that is already taken fails with an `IllegalArgumentException`, so a registration either succeeds once or fails loudly.

Callers never look up a `FormatModel` themselves. They ask the registry for a builder:

| Registry method | Produces |
|---|---|
| `readBuilder(format, type, inputFile)` | `ReadBuilder<D, S>` |
| `dataWriteBuilder(format, type, outputFile)` | `FileWriterBuilder<DataWriter<D>, S>` |
| `equalityDeleteWriteBuilder(format, type, outputFile)` | `FileWriterBuilder<EqualityDeleteWriter<D>, S>` |
| `positionDeleteWriteBuilder(format, outputFile)` | `FileWriterBuilder<PositionDeleteWriter<D>, ?>` |

### Builders

[`ReadBuilder`](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/formats/ReadBuilder.html) configures a reader for one file and returns a `CloseableIterable<D>`. The options are the same for every format: `project` (the Iceberg projection, required), `split` (byte range), `filter` (an `Expression` to push down), `caseSensitive`, `idToConstant` (values that come from metadata rather than the file, such as partition columns), `withNameMapping`, `reuseContainers`, `recordsPerBatch` for vectorized readers, and `set`/`setAll` for format-specific properties. Readers may apply a pushed-down filter only partially, so callers must still evaluate the residual filter on the rows they get back.

[`FileWriterBuilder`](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/formats/FileWriterBuilder.html) configures a writer that produces a content file: a `DataWriter`, an `EqualityDeleteWriter` or a `PositionDeleteWriter`. Besides the file schema and format properties it takes the metadata Iceberg needs to describe the resulting `DataFile` or `DeleteFile`: `spec`, `partition`, `sortOrder`, `keyMetadata`, `metricsConfig` and, for equality deletes, `equalityFieldIds`. The writer collects column metrics while writing; once it is closed, it exposes the resulting `DataFile` or `DeleteFile`, ready to be committed to the table.

[`ModelWriteBuilder`](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/formats/ModelWriteBuilder.html) is the lower-level builder returned by `FormatModel.writeBuilder`. It produces a plain `FileAppender<D>` without Iceberg content-file metadata. Use it only when an appender is all that is needed; `FileWriterBuilder` wraps it for the common cases.

### Engine schemas

Some type information is not expressible in an Iceberg schema. Spark distinguishes `ByteType` and `ShortType` where Iceberg has only `int`; a variant column may be written shredded, which depends on engine metadata; a struct may need to be read into a specific class. Both builders accept the engine's representation alongside the Iceberg schema for this purpose: `ReadBuilder.engineProjection(S)` and the writers' `engineSchema(S)`. The engine schema must agree with the Iceberg schema; it may only add representation detail that Iceberg considers equivalent.

## Reading a file

Reading goes through the registry with the file's format and the record class the caller wants. This is how the generic data module opens a `FileScanTask`:

```java
InputFile input = io.newInputFile(task.file());
Map<Integer, ?> partition =
    PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);

ReadBuilder<Record, ?> builder =
    FormatModelRegistry.readBuilder(task.file().format(), Record.class, input);
if (reuseContainers) {
  builder = builder.reuseContainers();
}

return builder
    .project(fileProjection)
    .idToConstant(partition)
    .split(task.start(), task.length())
    .caseSensitive(caseSensitive)
    .filter(task.residual())
    .build();
```

The same call with `InternalRow.class` or `ColumnarBatch.class` returns Spark rows or batches, provided the Spark module is on the classpath. The caller's code does not change when the file format does, since the format comes from the data file's metadata.

## Writing files

Writers follow the same pattern. `RegistryBasedFileWriterFactory` in `iceberg-data` is the base class the generic, Spark and Flink writer factories extend, and its `newDataWriter` builds a writer like this (`properties` are the table properties and `metricsConfig` is the table's metrics configuration):

```java
FileWriterBuilder<DataWriter<T>, S> builder =
    FormatModelRegistry.dataWriteBuilder(dataFileFormat, inputType, file);
return builder
    .schema(dataSchema)
    .engineSchema(inputSchema())
    .setAll(properties)
    .setAll(writerProperties)
    .metricsConfig(metricsConfig)
    .spec(spec)
    .partition(partition)
    .keyMetadata(keyMetadata)
    .sortOrder(dataSortOrder)
    .overwrite()
    .build();
```

Equality delete writers add `equalityFieldIds` and use the delete row schema. Position delete writers are built with `positionDeleteWriteBuilder` and take `PositionDelete<D>` records. Position delete files of this kind are a format version 2 feature: version 3 tables store position deletes as deletion vectors in Puffin files, which engines write with `DVFileWriter` rather than through the registry.

## Built-in object models

| Module | Record type | Engine schema | Formats |
|---|---|---|---|
| `iceberg-data` | `Record` | none (`Void`), `Schema` for Parquet | Parquet, Avro, ORC |
| `iceberg-arrow` | `ColumnarBatch` | `Object` | Parquet (vectorized read only) |
| `iceberg-spark` | `InternalRow` | `StructType` | Parquet, Avro, ORC |
| `iceberg-spark` | `ColumnarBatch` | `StructType` | Parquet, ORC (vectorized read only) |
| `iceberg-flink` | `RowData` | `RowType` | Parquet, Avro, ORC |

The `iceberg-data`, `iceberg-spark` and `iceberg-flink` modules also register position delete models for the formats they support; the Arrow module is read-only and registers none.

## Registering a custom object model

An engine with its own row representation registers one model per file format it wants to support. The registration code consists of the record class, the engine schema class and the two converter functions. This is the generic Parquet registration from `GenericFormatModels`:

```java
FormatModelRegistry.register(
    ParquetFormatModel.create(
        Record.class,
        Schema.class,
        (icebergSchema, fileSchema, engineSchema) ->
            GenericParquetWriter.create(icebergSchema, fileSchema),
        (icebergSchema, fileSchema, engineSchema, idToConstant) ->
            GenericParquetReaders.buildReader(icebergSchema, fileSchema, idToConstant),
        new RecordVariantShreddingAnalyzer(),
        (Function<Schema, UnaryOperator<Record>>) ignored -> Record::copy));
```

The writer function receives the Iceberg schema, the format's own file schema (a Parquet `MessageType` here) and the optional engine schema, and returns a value writer for the format. The reader function receives the same plus the `idToConstant` map and returns a value reader. For Parquet, a `VariantShreddingAnalyzer` and a copy function can be supplied to support shredded variant writes and container reuse. `AvroFormatModel.create` and `ORCFormatModel.create` take the equivalent functions for their formats, and the ORC model has a second `create` overload for batch readers.

Registration must happen before the first read or write. The built-in modules do it from a static `register()` method that the registry calls on startup; a model outside the Iceberg project has to call `FormatModelRegistry.register` itself, for example from the engine's catalog or data source initialization.

## Adding a new file format

The registry is keyed on the `FileFormat` enum, so a new file format must first be added there, alongside `PARQUET`, `AVRO` and `ORC`. Adding a format is a change to the Iceberg project rather than a plugin dropped on the classpath. The format then provides its own `FormatModel` implementation, typically by extending `BaseFormatModel`, and the engine modules register their object models against it.

Work on new formats is ongoing in the project: a [Vortex](https://github.com/apache/iceberg/issues/15416) integration is in progress, and a [Technology Compatibility Kit](https://github.com/apache/iceberg/issues/15415) that exercises reads and writes across every object model is being built so new format implementations can be verified against the same expectations as Parquet, Avro and ORC.

## Further reading

- [Introducing the Apache Iceberg File Format API](https://iceberg.apache.org/blog/apache-iceberg-file-format-api/), the announcement with the motivation behind the design
- [`org.apache.iceberg.formats` Javadoc](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/formats/package-summary.html)
