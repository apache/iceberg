/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentFileWriterFactory;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFileWriter;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.BaseDeltaWriter;
import org.apache.iceberg.io.DeltaWriter;
import org.apache.iceberg.io.DeltaWriterFactory;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingContentFileWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class GenericDeltaWriterFactory implements DeltaWriterFactory<Record> {

  private final Schema schema;
  private final PartitionSpec spec;
  private final FileFormat format;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final Map<String, String> tableProperties;
  private final FileAppenderFactory<Record> appenderFactory;

  public GenericDeltaWriterFactory(Schema schema,
                                   PartitionSpec spec,
                                   FileFormat format,
                                   OutputFileFactory fileFactory,
                                   FileIO io,
                                   long targetFileSize,
                                   Map<String, String> tableProperties) {
    this.schema = schema;
    this.spec = spec;
    this.format = format;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.tableProperties = tableProperties;
    this.appenderFactory = createFileAppenderFactory();
  }

  @Override
  public DeltaWriter<Record> createDeltaWriter(PartitionKey partitionKey, Context ctxt) {
    RollingContentFileWriter<DataFile, Record> dataWriter = new RollingContentFileWriter<>(partitionKey,
        format, fileFactory, io, targetFileSize, createDataFileWriterFactory());

    if (!ctxt.allowPosDelete() && !ctxt.allowEqualityDelete()) {
      return new GenericDeltaWriter(dataWriter);
    }

    RollingContentFileWriter<DeleteFile, PositionDelete<Record>> posDeleteWriter =
        new RollingContentFileWriter<>(partitionKey,
            format, fileFactory, io, targetFileSize, createPosDeleteWriterFactory(ctxt.posDeleteRowSchema()));

    if (ctxt.allowPosDelete() && !ctxt.allowEqualityDelete()) {
      return new GenericDeltaWriter(dataWriter, posDeleteWriter);
    }

    Preconditions.checkState(ctxt.allowEqualityDelete(), "Should always allow equality-delete here.");
    Preconditions.checkState(ctxt.equalityFieldIds() != null && !ctxt.equalityFieldIds().isEmpty(),
        "Equality field id list shouldn't be null or emtpy.");

    RollingContentFileWriter<DeleteFile, Record> eqDeleteWriter = new RollingContentFileWriter<>(partitionKey,
        format, fileFactory, io, targetFileSize,
        createEqualityDeleteWriterFactory(ctxt.equalityFieldIds(), ctxt.eqDeleteRowSchema()));


    return new GenericDeltaWriter(dataWriter, posDeleteWriter, eqDeleteWriter, schema, ctxt.equalityFieldIds());
  }

  @Override
  public FileAppenderFactory<Record> createFileAppenderFactory() {
    return new GenericAppenderFactory(schema);
  }

  @Override
  public ContentFileWriterFactory<DataFile, Record> createDataFileWriterFactory() {
    return (partitionKey, outputFile, fileFormat) -> {
      FileAppender<Record> appender = appenderFactory.newAppender(outputFile.encryptingOutputFile(), fileFormat);

      return new DataFileWriter<>(appender,
          fileFormat,
          outputFile.encryptingOutputFile().location(),
          partitionKey,
          spec, outputFile.keyMetadata());
    };
  }

  @Override
  public ContentFileWriterFactory<DeleteFile, Record> createEqualityDeleteWriterFactory(
      List<Integer> equalityFieldIds, Schema rowSchema) {
    return (partitionKey, outputFile, fileFormat) -> {

      MetricsConfig metricsConfig = MetricsConfig.fromProperties(tableProperties);
      try {
        switch (fileFormat) {
          case AVRO:
            return Avro.writeDeletes(outputFile.encryptingOutputFile())
                .createWriterFunc(DataWriter::create)
                .withPartition(partitionKey)
                .overwrite()
                .setAll(tableProperties)
                .rowSchema(rowSchema)
                .withSpec(spec)
                .withKeyMetadata(outputFile.keyMetadata())
                .equalityFieldIds(equalityFieldIds)
                .buildEqualityWriter();

          case PARQUET:
            return Parquet.writeDeletes(outputFile.encryptingOutputFile())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .withPartition(partitionKey)
                .overwrite()
                .setAll(tableProperties)
                .metricsConfig(metricsConfig)
                .rowSchema(rowSchema)
                .withSpec(spec)
                .withKeyMetadata(outputFile.keyMetadata())
                .equalityFieldIds(equalityFieldIds)
                .buildEqualityWriter();

          case ORC:
            throw new UnsupportedOperationException("Orc file format does not support writing equality delete.");

          default:
            throw new UnsupportedOperationException("Cannot write unknown file format: " + fileFormat);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  @Override
  public ContentFileWriterFactory<DeleteFile, PositionDelete<Record>> createPosDeleteWriterFactory(Schema rowSchema) {
    return (partitionKey, outputFile, fileFormat) -> {
      MetricsConfig metricsConfig = MetricsConfig.fromProperties(tableProperties);
      try {
        switch (fileFormat) {
          case AVRO:
            return Avro.writeDeletes(outputFile.encryptingOutputFile())
                .createWriterFunc(DataWriter::create)
                .withPartition(partitionKey)
                .overwrite()
                .setAll(tableProperties)
                .rowSchema(rowSchema) // it's a nullable field.
                .withSpec(spec)
                .withKeyMetadata(outputFile.keyMetadata())
                .buildPositionWriter();

          case PARQUET:
            return Parquet.writeDeletes(outputFile.encryptingOutputFile())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .withPartition(partitionKey)
                .overwrite()
                .setAll(tableProperties)
                .metricsConfig(metricsConfig)
                .rowSchema(rowSchema) // it's a nullable field.
                .withSpec(spec)
                .withKeyMetadata(outputFile.keyMetadata())
                .buildPositionWriter();

          case ORC:
            throw new UnsupportedOperationException("Orc file format does not support writing positional delete.");

          default:
            throw new UnsupportedOperationException("Cannot write unknown file format: " + fileFormat);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  private static class GenericDeltaWriter extends BaseDeltaWriter<Record> {

    GenericDeltaWriter(RollingContentFileWriter<DataFile, Record> dataWriter) {
      this(dataWriter, null);
    }

    GenericDeltaWriter(RollingContentFileWriter<DataFile, Record> dataWriter,
                       RollingContentFileWriter<DeleteFile, PositionDelete<Record>> posDeleteWriter) {
      this(dataWriter, posDeleteWriter, null, null, null);
    }

    GenericDeltaWriter(RollingContentFileWriter<DataFile, Record> dataWriter,
                       RollingContentFileWriter<DeleteFile, PositionDelete<Record>> posDeleteWriter,
                       RollingContentFileWriter<DeleteFile, Record> equalityDeleteWriter, Schema tableSchema,
                       List<Integer> equalityFieldIds) {
      super(dataWriter, posDeleteWriter, equalityDeleteWriter, tableSchema, equalityFieldIds);
    }

    @Override
    protected StructLike asKey(Record row) {
      return row;
    }

    @Override
    protected StructLike asCopiedKey(Record row) {
      return row.copy();
    }
  }
}
