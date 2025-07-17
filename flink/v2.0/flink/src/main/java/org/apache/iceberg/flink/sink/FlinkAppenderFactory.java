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
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.DataWriteBuilder;
import org.apache.iceberg.data.EqualityDeleteWriteBuilder;
import org.apache.iceberg.data.FormatModelRegistry;
import org.apache.iceberg.data.PositionDeleteWriteBuilder;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.data.FlinkFormatModels;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.WriteBuilder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

public class FlinkAppenderFactory implements FileAppenderFactory<RowData>, Serializable {
  private static final Schema EMPTY_SCHEMA = new Schema();
  private final Schema schema;
  private final RowType flinkSchema;
  private final Map<String, String> props;
  private final PartitionSpec spec;
  private final int[] equalityFieldIds;
  private final Schema eqDeleteRowSchema;
  private final Schema posDeleteRowSchema;
  private final Table table;

  private RowType eqDeleteFlinkSchema = null;
  private RowType posDeleteFlinkSchema = null;

  public FlinkAppenderFactory(
      Table table,
      Schema schema,
      RowType flinkSchema,
      Map<String, String> props,
      PartitionSpec spec,
      int[] equalityFieldIds,
      Schema eqDeleteRowSchema,
      Schema posDeleteRowSchema) {
    Preconditions.checkNotNull(table, "Table shouldn't be null");
    this.table = table;
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.props = props;
    this.spec = spec;
    this.equalityFieldIds = equalityFieldIds;
    this.eqDeleteRowSchema = eqDeleteRowSchema;
    this.posDeleteRowSchema = posDeleteRowSchema;
  }

  private RowType lazyEqDeleteFlinkSchema() {
    if (eqDeleteFlinkSchema == null) {
      Preconditions.checkNotNull(eqDeleteRowSchema, "Equality delete row schema shouldn't be null");
      this.eqDeleteFlinkSchema = FlinkSchemaUtil.convert(eqDeleteRowSchema);
    }

    return eqDeleteFlinkSchema;
  }

  private RowType lazyPosDeleteFlinkSchema() {
    if (posDeleteFlinkSchema == null) {
      if (posDeleteRowSchema == null) {
        this.posDeleteFlinkSchema = FlinkSchemaUtil.convert(EMPTY_SCHEMA);
      } else {
        this.posDeleteFlinkSchema = FlinkSchemaUtil.convert(posDeleteRowSchema);
      }
    }

    return this.posDeleteFlinkSchema;
  }

  @Override
  public FileAppender<RowData> newAppender(OutputFile outputFile, FileFormat format) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      WriteBuilder<?, RowData> builder =
          FormatModelRegistry.writeBuilder(
              format,
              FlinkFormatModels.MODEL_NAME,
              EncryptedFiles.plainAsEncryptedOutput(outputFile));
      return new WrappedFileAppender(
          builder.set(props).fileSchema(schema).metricsConfig(metricsConfig).overwrite().build(),
          flinkSchema,
          schema.asStruct());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public DataWriter<RowData> newDataWriter(
      EncryptedOutputFile file, FileFormat format, StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      DataWriteBuilder<?, RowData> builder =
          FormatModelRegistry.dataWriteBuilder(format, FlinkFormatModels.MODEL_NAME, file);
      return new WrappedDataWriter(
          builder
              .set(props)
              .fileSchema(schema)
              .metricsConfig(metricsConfig)
              .overwrite()
              .spec(spec)
              .partition(partition)
              .keyMetadata(file.keyMetadata())
              .build(),
          flinkSchema,
          schema.asStruct());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public EqualityDeleteWriter<RowData> newEqDeleteWriter(
      EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    Preconditions.checkState(
        equalityFieldIds != null && equalityFieldIds.length > 0,
        "Equality field ids shouldn't be null or empty when creating equality-delete writer");
    Preconditions.checkNotNull(
        eqDeleteRowSchema,
        "Equality delete row schema shouldn't be null when creating equality-delete writer");

    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      EqualityDeleteWriteBuilder<?, RowData> builder =
          FormatModelRegistry.equalityDeleteWriteBuilder(
              format, FlinkFormatModels.MODEL_NAME, outputFile);
      return new WrappedEqualityDeleteWriter(
          builder
              .overwrite()
              .set(props)
              .metricsConfig(metricsConfig)
              .partition(partition)
              .rowSchema(eqDeleteRowSchema)
              .spec(spec)
              .keyMetadata(outputFile.keyMetadata())
              .equalityFieldIds(equalityFieldIds)
              .build(),
          lazyEqDeleteFlinkSchema(),
          eqDeleteRowSchema.asStruct());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public PositionDeleteWriter<RowData> newPosDeleteWriter(
      EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.forPositionDelete(table);
    try {
      PositionDeleteWriteBuilder<?, RowData> builder =
          FormatModelRegistry.positionDeleteWriteBuilder(
              format, FlinkFormatModels.MODEL_NAME, outputFile);
      return new WrappedPositionDeleteWriter(
          builder
              .overwrite()
              .set(props)
              .metricsConfig(metricsConfig)
              .partition(partition)
              .rowSchema(posDeleteRowSchema)
              .spec(spec)
              .keyMetadata(outputFile.keyMetadata())
              .build(),
          lazyPosDeleteFlinkSchema(),
          posDeleteRowSchema == null ? EMPTY_SCHEMA.asStruct() : posDeleteRowSchema.asStruct());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static class WrappedFileAppender implements FileAppender<RowData> {
    private final FileAppender<RowData> delegate;
    private final RowDataTransformer transformer;

    private WrappedFileAppender(
        FileAppender<RowData> delegate, RowType rowType, Types.StructType struct) {
      this.delegate = delegate;
      this.transformer = new RowDataTransformer(rowType, struct);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public void add(RowData datum) {
      delegate.add(transformer.wrap(datum));
    }

    @Override
    public Metrics metrics() {
      return delegate.metrics();
    }

    @Override
    public long length() {
      return delegate.length();
    }

    @Override
    public List<Long> splitOffsets() {
      return delegate.splitOffsets();
    }
  }

  private static class WrappedDataWriter extends DataWriter<RowData> {
    private final DataWriter<RowData> delegate;
    private final RowDataTransformer transformer;

    private WrappedDataWriter(
        DataWriter<RowData> delegate, RowType rowType, Types.StructType struct) {
      super(delegate);
      this.delegate = delegate;
      this.transformer = new RowDataTransformer(rowType, struct);
    }

    @Override
    public void write(RowData row) {
      delegate.write(transformer.wrap(row));
    }
  }

  private static class WrappedEqualityDeleteWriter extends EqualityDeleteWriter<RowData> {
    private final EqualityDeleteWriter<RowData> delegate;
    private final RowDataTransformer transformer;

    private WrappedEqualityDeleteWriter(
        EqualityDeleteWriter<RowData> delegate, RowType rowType, Types.StructType struct) {
      super(delegate);
      this.delegate = delegate;
      this.transformer = new RowDataTransformer(rowType, struct);
    }

    @Override
    public void write(RowData row) {
      delegate.write(transformer.wrap(row));
    }
  }

  private static class WrappedPositionDeleteWriter extends PositionDeleteWriter<RowData> {
    private final PositionDeleteWriter<RowData> delegate;
    private final RowDataTransformer transformer;

    private WrappedPositionDeleteWriter(
        PositionDeleteWriter<RowData> delegate, RowType rowType, Types.StructType struct) {
      super(delegate);
      this.delegate = delegate;
      this.transformer = new RowDataTransformer(rowType, struct);
    }

    @Override
    public void write(PositionDelete<RowData> row) {
      delegate.write(transformer.wrap(row));
    }
  }
}
