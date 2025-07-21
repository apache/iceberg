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
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.DataWriteBuilder;
import org.apache.iceberg.data.EqualityDeleteWriteBuilder;
import org.apache.iceberg.data.FormatModelRegistry;
import org.apache.iceberg.data.PositionDeleteWriteBuilder;
import org.apache.iceberg.data.RowTransformer;
import org.apache.iceberg.data.TransformingWriters;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.data.FlinkFormatModels;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.WriteBuilder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FlinkAppenderFactory implements FileAppenderFactory<RowData>, Serializable {
  private final Schema schema;
  private final RowType flinkSchema;
  private final Map<String, String> props;
  private final PartitionSpec spec;
  private final int[] equalityFieldIds;
  private final Schema eqDeleteRowSchema;
  private final Schema posDeleteRowSchema;
  private final Table table;

  private RowTransformer<RowData> rowTransformer = null;

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

  private RowTransformer<RowData> lazyRowTransformer() {
    if (rowTransformer == null) {
      this.rowTransformer = new RowDataTransformer(flinkSchema, schema.asStruct());
    }

    return rowTransformer;
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
      return TransformingWriters.of(
          builder.set(props).fileSchema(schema).metricsConfig(metricsConfig).overwrite().build(),
          lazyRowTransformer());
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
      return TransformingWriters.of(
          builder
              .set(props)
              .fileSchema(schema)
              .metricsConfig(metricsConfig)
              .overwrite()
              .spec(spec)
              .partition(partition)
              .keyMetadata(file.keyMetadata())
              .build(),
          lazyRowTransformer());
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
      return TransformingWriters.of(
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
          row -> row);
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
      return TransformingWriters.of(
          builder
              .overwrite()
              .set(props)
              .metricsConfig(metricsConfig)
              .partition(partition)
              .rowSchema(posDeleteRowSchema)
              .spec(spec)
              .keyMetadata(outputFile.keyMetadata())
              .build(),
          row -> row);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
