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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.data.FlinkAvroWriter;
import org.apache.iceberg.flink.data.FlinkOrcWriter;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.formats.DataWriteBuilder;
import org.apache.iceberg.formats.EqualityDeleteWriteBuilder;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.PositionDeleteWriteBuilder;
import org.apache.iceberg.formats.WriteBuilder;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FlinkAppenderFactory implements FileAppenderFactory<RowData>, Serializable {
  private final Schema schema;
  private final RowType flinkSchema;
  private final Map<String, String> props;
  private final PartitionSpec spec;
  private final int[] equalityFieldIds;
  private final Schema eqDeleteRowSchema;
  private Schema posDeleteRowSchema;
  private final Table table;

  private RowType eqDeleteFlinkSchema = null;
  private RowType posDeleteFlinkSchema = null;

  /**
   * @deprecated This constructor is deprecated as of version 1.11.0 and will be removed in 1.12.0.
   *     Position deletes that include row data are no longer supported. Use {@link
   *     #FlinkAppenderFactory(Table, Schema, RowType, Map, PartitionSpec, int[], Schema)} instead.
   */
  @Deprecated
  public FlinkAppenderFactory(
      Table table,
      Schema schema,
      RowType flinkSchema,
      Map<String, String> props,
      PartitionSpec spec,
      int[] equalityFieldIds,
      Schema eqDeleteRowSchema,
      Schema posDeleteRowSchema) {
    this(table, schema, flinkSchema, props, spec, equalityFieldIds, eqDeleteRowSchema);
    this.posDeleteRowSchema = posDeleteRowSchema;
  }

  public FlinkAppenderFactory(
      Table table,
      Schema schema,
      RowType flinkSchema,
      Map<String, String> props,
      PartitionSpec spec,
      int[] equalityFieldIds,
      Schema eqDeleteRowSchema) {
    Preconditions.checkNotNull(table, "Table shouldn't be null");
    this.table = table;
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.props = props;
    this.spec = spec;
    this.equalityFieldIds = equalityFieldIds;
    this.eqDeleteRowSchema = eqDeleteRowSchema;
    this.posDeleteRowSchema = null;
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
      Preconditions.checkNotNull(posDeleteRowSchema, "Pos-delete row schema shouldn't be null");
      this.posDeleteFlinkSchema = FlinkSchemaUtil.convert(posDeleteRowSchema);
    }
    return this.posDeleteFlinkSchema;
  }

  @Override
  public FileAppender<RowData> newAppender(OutputFile outputFile, FileFormat format) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      WriteBuilder builder =
          FormatModelRegistry.writeBuilder(
              format, RowData.class, EncryptedFiles.plainAsEncryptedOutput(outputFile));
      return builder
          .setAll(props)
          .schema(schema)
          .inputSchema(flinkSchema)
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
      DataWriteBuilder<RowData, RowType> builder =
          FormatModelRegistry.dataWriteBuilder(format, RowData.class, file);
      return builder
          .setAll(props)
          .schema(schema)
          .inputSchema(flinkSchema)
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
      EqualityDeleteWriteBuilder<RowData, RowType> builder =
          FormatModelRegistry.equalityDeleteWriteBuilder(format, RowData.class, outputFile);
      return builder
          .overwrite()
          .setAll(props)
          .metricsConfig(metricsConfig)
          .partition(partition)
          .rowSchema(eqDeleteRowSchema)
          .inputSchema(lazyEqDeleteFlinkSchema())
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
      if (posDeleteRowSchema == null && posDeleteFlinkSchema == null) {
        PositionDeleteWriteBuilder builder =
            FormatModelRegistry.positionDeleteWriteBuilder(format, outputFile);
        return builder
            .overwrite()
            .setAll(props)
            .metricsConfig(metricsConfig)
            .partition(partition)
            .spec(spec)
            .keyMetadata(outputFile.keyMetadata())
            .build();
      } else {
        switch (format) {
          case AVRO:
            return Avro.writeDeletes(outputFile.encryptingOutputFile())
                .createWriterFunc(ignore -> new FlinkAvroWriter(lazyPosDeleteFlinkSchema()))
                .withPartition(partition)
                .overwrite()
                .setAll(props)
                .metricsConfig(metricsConfig)
                .rowSchema(posDeleteRowSchema)
                .withSpec(spec)
                .withKeyMetadata(outputFile.keyMetadata())
                .buildPositionWriter();

          case ORC:
            RowType orcPosDeleteSchema =
                FlinkSchemaUtil.convert(DeleteSchemaUtil.posDeleteSchema(posDeleteRowSchema));
            return ORC.writeDeletes(outputFile.encryptingOutputFile())
                .createWriterFunc(
                    (iSchema, typDesc) -> FlinkOrcWriter.buildWriter(orcPosDeleteSchema, iSchema))
                .withPartition(partition)
                .overwrite()
                .setAll(props)
                .metricsConfig(metricsConfig)
                .rowSchema(posDeleteRowSchema)
                .withSpec(spec)
                .withKeyMetadata(outputFile.keyMetadata())
                .transformPaths(path -> StringData.fromString(path.toString()))
                .buildPositionWriter();

          case PARQUET:
            RowType flinkPosDeleteSchema =
                FlinkSchemaUtil.convert(DeleteSchemaUtil.posDeleteSchema(posDeleteRowSchema));
            return Parquet.writeDeletes(outputFile.encryptingOutputFile())
                .createWriterFunc(
                    msgType -> FlinkParquetWriters.buildWriter(flinkPosDeleteSchema, msgType))
                .withPartition(partition)
                .overwrite()
                .setAll(props)
                .metricsConfig(metricsConfig)
                .rowSchema(posDeleteRowSchema)
                .withSpec(spec)
                .withKeyMetadata(outputFile.keyMetadata())
                .transformPaths(path -> StringData.fromString(path.toString()))
                .buildPositionWriter();

          default:
            throw new UnsupportedOperationException(
                "Cannot write pos-deletes for unsupported file format: " + format);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
