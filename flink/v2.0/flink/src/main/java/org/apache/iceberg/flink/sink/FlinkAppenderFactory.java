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
import org.apache.iceberg.data.ObjectModelRegistry;
import org.apache.iceberg.data.PositionDeleteWriteBuilder;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.data.FlinkObjectModels;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
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
      this.posDeleteFlinkSchema =
          FlinkSchemaUtil.convert(DeleteSchemaUtil.posDeleteSchema(posDeleteRowSchema));
    }
    return this.posDeleteFlinkSchema;
  }

  @Override
  public FileAppender<RowData> newAppender(OutputFile outputFile, FileFormat format) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      WriteBuilder<?, RowType, RowData> builder =
          ObjectModelRegistry.writeBuilder(
              format,
              FlinkObjectModels.FLINK_OBJECT_MODEL,
              EncryptedFiles.plainAsEncryptedOutput(outputFile));
      return builder
          .modelSchema(flinkSchema)
          .set(props)
          .fileSchema(schema)
          .metricsConfig(metricsConfig)
          .overwrite()
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public DataWriter<RowData> newDataWriter(
      EncryptedOutputFile file, FileFormat format, StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      DataWriteBuilder<?, RowType, RowData> builder =
          ObjectModelRegistry.dataWriteBuilder(format, FlinkObjectModels.FLINK_OBJECT_MODEL, file);
      return builder
          .modelSchema(flinkSchema)
          .set(props)
          .fileSchema(schema)
          .metricsConfig(metricsConfig)
          .overwrite()
          .spec(spec)
          .partition(partition)
          .keyMetadata(file.keyMetadata())
          .build();
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
      EqualityDeleteWriteBuilder<?, RowType, RowData> builder =
          ObjectModelRegistry.equalityDeleteWriteBuilder(
              format, FlinkObjectModels.FLINK_OBJECT_MODEL, outputFile);
      return builder
          .overwrite()
          .set(props)
          .metricsConfig(metricsConfig)
          .modelSchema(lazyEqDeleteFlinkSchema())
          .partition(partition)
          .rowSchema(eqDeleteRowSchema)
          .spec(spec)
          .keyMetadata(outputFile.keyMetadata())
          .equalityFieldIds(equalityFieldIds)
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public PositionDeleteWriter<RowData> newPosDeleteWriter(
      EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.forPositionDelete(table);
    try {
      PositionDeleteWriteBuilder<?, RowType, RowData> builder =
          ObjectModelRegistry.positionDeleteWriteBuilder(
              format, FlinkObjectModels.FLINK_OBJECT_MODEL, outputFile);
      return builder
          .overwrite()
          .set(props)
          .metricsConfig(metricsConfig)
          .modelSchema(lazyPosDeleteFlinkSchema())
          .partition(partition)
          .rowSchema(posDeleteRowSchema)
          .spec(spec)
          .keyMetadata(outputFile.keyMetadata())
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
