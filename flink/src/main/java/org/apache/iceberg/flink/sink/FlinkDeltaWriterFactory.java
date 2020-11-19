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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.io.DatumWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
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
import org.apache.iceberg.deletes.DeletesUtil;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.FlinkAvroWriter;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.io.BaseDeltaWriter;
import org.apache.iceberg.io.DeltaWriter;
import org.apache.iceberg.io.DeltaWriterFactory;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingContentFileWriter;
import org.apache.iceberg.io.RollingPosDeleteWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FlinkDeltaWriterFactory implements DeltaWriterFactory<RowData> {

  private final Schema schema;
  private final RowType flinkSchema;
  private final PartitionSpec spec;
  private final FileFormat format;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final Map<String, String> tableProperties;
  private final FileAppenderFactory<RowData> appenderFactory;

  public FlinkDeltaWriterFactory(Schema schema,
                                 RowType flinkSchema,
                                 PartitionSpec spec,
                                 FileFormat format,
                                 OutputFileFactory fileFactory,
                                 FileIO io,
                                 long targetFileSize,
                                 Map<String, String> tableProperties) {
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.spec = spec;
    this.format = format;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.tableProperties = tableProperties;
    this.appenderFactory = createFileAppenderFactory();
  }

  @Override
  public DeltaWriter<RowData> createDeltaWriter(PartitionKey partitionKey, Context ctxt) {
    RollingContentFileWriter<DataFile, RowData> dataWriter = new RollingContentFileWriter<>(partitionKey,
        format, fileFactory, io, targetFileSize, createDataFileWriterFactory());

    if (!ctxt.allowPosDelete() && !ctxt.allowEqualityDelete()) {
      return new RowDataDeltaWriter(dataWriter);
    }

    RollingPosDeleteWriter<RowData> posDeleteWriter = new RollingPosDeleteWriter<>(partitionKey,
        format, fileFactory, io, targetFileSize, createPosDeleteWriterFactory(ctxt.posDeleteRowSchema()));

    if (ctxt.allowPosDelete() && !ctxt.allowEqualityDelete()) {
      return new RowDataDeltaWriter(dataWriter, posDeleteWriter);
    }

    Preconditions.checkState(ctxt.allowEqualityDelete(), "Should always allow equality-delete here.");
    Preconditions.checkState(ctxt.equalityFieldIds() != null && !ctxt.equalityFieldIds().isEmpty(),
        "Equality field id list shouldn't be null or emtpy.");

    RollingContentFileWriter<DeleteFile, RowData> eqDeleteWriter = new RollingContentFileWriter<>(partitionKey,
        format, fileFactory, io, targetFileSize,
        createEqualityDeleteWriterFactory(ctxt.equalityFieldIds(), ctxt.eqDeleteRowSchema()));

    return new RowDataDeltaWriter(dataWriter, posDeleteWriter, eqDeleteWriter, schema, ctxt.equalityFieldIds());
  }

  @Override
  public FileAppenderFactory<RowData> createFileAppenderFactory() {
    return new FlinkFileAppenderFactory(schema, flinkSchema, tableProperties);
  }

  @Override
  public ContentFileWriterFactory<DataFile, RowData> createDataFileWriterFactory() {
    // TODO Move this to its default function ???
    return (partitionKey, outputFile, fileFormat) -> {
      FileAppender<RowData> appender = appenderFactory.newAppender(outputFile.encryptingOutputFile(), fileFormat);

      return new DataFileWriter<>(appender,
          fileFormat,
          outputFile.encryptingOutputFile().location(),
          partitionKey,
          spec, outputFile.keyMetadata());
    };
  }

  @Override
  public ContentFileWriterFactory<DeleteFile, RowData> createEqualityDeleteWriterFactory(List<Integer> equalityFieldIds,
                                                                                         Schema rowSchema) {
    Preconditions.checkNotNull(rowSchema, "Row schema shouldn't be null for equality deletes.");
    RowType flinkRowSchema = FlinkSchemaUtil.convert(rowSchema);

    return (partitionKey, outputFile, fileFormat) -> {

      MetricsConfig metricsConfig = MetricsConfig.fromProperties(tableProperties);
      try {
        switch (fileFormat) {
          case AVRO:
            return Avro.writeDeletes(outputFile.encryptingOutputFile())
                .createWriterFunc(ignore -> new FlinkAvroWriter(flinkRowSchema))
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
                .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(flinkRowSchema, msgType))
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
  public ContentFileWriterFactory<DeleteFile, PositionDelete<RowData>> createPosDeleteWriterFactory(Schema rowSchema) {

    return (partitionKey, outputFile, fileFormat) -> {
      MetricsConfig metricsConfig = MetricsConfig.fromProperties(tableProperties);
      try {
        switch (fileFormat) {
          case AVRO:
            // Produce the positional delete schema that writer will use for the file.
            Function<org.apache.avro.Schema, DatumWriter<?>> writeFunc =
                rowSchema == null ? null : ignore -> new FlinkAvroWriter(FlinkSchemaUtil.convert(rowSchema));
            return Avro.writeDeletes(outputFile.encryptingOutputFile())
                .createWriterFunc(writeFunc)
                .withPartition(partitionKey)
                .overwrite()
                .setAll(tableProperties)
                .rowSchema(rowSchema) // it's a nullable field.
                .withSpec(spec)
                .withKeyMetadata(outputFile.keyMetadata())
                .buildPositionWriter();

          case PARQUET:
            RowType flinkParquetRowType = FlinkSchemaUtil.convert(DeletesUtil.posDeleteSchema(rowSchema));
            return Parquet.writeDeletes(outputFile.encryptingOutputFile())
                .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(flinkParquetRowType, msgType))
                .withPartition(partitionKey)
                .overwrite()
                .setAll(tableProperties)
                .metricsConfig(metricsConfig)
                .rowSchema(rowSchema) // it's a nullable field.
                .withSpec(spec)
                .withKeyMetadata(outputFile.keyMetadata())
                .buildPositionWriter(RowDataPositionAccessor.INSTANCE);

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

  private class RowDataDeltaWriter extends BaseDeltaWriter<RowData> {
    private final RowDataWrapper rowDataWrapper;

    RowDataDeltaWriter(RollingContentFileWriter<DataFile, RowData> dataWriter) {
      this(dataWriter, null);
    }

    RowDataDeltaWriter(RollingContentFileWriter<DataFile, RowData> dataWriter,
                       RollingPosDeleteWriter<RowData> posDeleteWriter) {
      this(dataWriter, posDeleteWriter, null, null, null);
    }

    RowDataDeltaWriter(RollingContentFileWriter<DataFile, RowData> dataWriter,
                       RollingPosDeleteWriter<RowData> posDeleteWriter,
                       RollingContentFileWriter<DeleteFile, RowData> equalityDeleteWriter, Schema tableSchema,
                       List<Integer> equalityFieldIds) {
      super(dataWriter, posDeleteWriter, equalityDeleteWriter, tableSchema, equalityFieldIds);

      this.rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    }

    @Override
    protected StructLike asKey(RowData row) {
      return rowDataWrapper.wrap(row);
    }

    @Override
    protected StructLike asCopiedKey(RowData row) {
      return rowDataWrapper.copy().wrap(row);
    }
  }

  private static class RowDataPositionAccessor implements Parquet.PositionAccessor<StringData, Long> {
    private static final RowDataPositionAccessor INSTANCE = new RowDataPositionAccessor();

    @Override
    public StringData accessPath(CharSequence path) {
      return StringData.fromString(path.toString());
    }

    @Override
    public Long accessPos(long pos) {
      return pos;
    }
  }
}
