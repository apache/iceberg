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

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_ROW_FIELD_NAME;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFileWriterService;
import org.apache.iceberg.DataFileWriterServiceRegistry;
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
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileFormatAppenderBuilder;
import org.apache.iceberg.io.FileFormatDataWriterBuilder;
import org.apache.iceberg.io.FileFormatEqualityDeleteWriterBuilder;
import org.apache.iceberg.io.FileFormatPositionDeleteWriterBuilder;
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
  private final Schema posDeleteRowSchema;
  private final Table table;

  private RowType eqDeleteFlinkSchema = null;

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

  @Override
  public FileAppender<RowData> newAppender(OutputFile outputFile, FileFormat format) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      return DataFileWriterServiceRegistry.appenderBuilder(
              format, RowData.class, EncryptedFiles.plainAsEncryptedOutput(outputFile), flinkSchema)
          .setAll(props)
          .schema(schema)
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
    return new DataWriter<>(
        newAppender(file.encryptingOutputFile(), format),
        format,
        file.encryptingOutputFile().location(),
        spec,
        partition,
        file.keyMetadata());
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
      return DataFileWriterServiceRegistry.equalityDeleteWriterBuilder(
              format, RowData.class, outputFile, lazyEqDeleteFlinkSchema())
          .withPartition(partition)
          .overwrite()
          .setAll(props)
          .metricsConfig(metricsConfig)
          .schema(eqDeleteRowSchema)
          .withSpec(spec)
          .withKeyMetadata(outputFile.keyMetadata())
          .equalityFieldIds(equalityFieldIds)
          .buildEqualityWriter();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public PositionDeleteWriter<RowData> newPosDeleteWriter(
      EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.forPositionDelete(table);
    try {
      RowType flinkPosDeleteSchema =
          FlinkSchemaUtil.convert(DeleteSchemaUtil.posDeleteSchema(posDeleteRowSchema));
      return DataFileWriterServiceRegistry.positionDeleteWriterBuilder(
              format, RowData.class, outputFile, flinkPosDeleteSchema)
          .withPartition(partition)
          .overwrite()
          .setAll(props)
          .metricsConfig(metricsConfig)
          .schema(posDeleteRowSchema)
          .withSpec(spec)
          .withKeyMetadata(outputFile.keyMetadata())
          .buildPositionWriter();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static class AvroWriterService implements DataFileWriterService<RowType> {
    @Override
    public FileFormat format() {
      return FileFormat.AVRO;
    }

    @Override
    public Class<?> returnType() {
      return RowData.class;
    }

    @Override
    public FileFormatAppenderBuilder<?> appenderBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return Avro.write(outputFile).createWriterFunc(ignore -> new FlinkAvroWriter(rowType));
    }

    @Override
    public FileFormatDataWriterBuilder<?> dataWriterBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return Avro.writeData(outputFile.encryptingOutputFile())
          .createWriterFunc(ignore -> new FlinkAvroWriter(rowType));
    }

    @Override
    public FileFormatEqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return Avro.writeDeletes(outputFile.encryptingOutputFile())
          .createWriterFunc(ignore -> new FlinkAvroWriter(rowType));
    }

    @Override
    public FileFormatPositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      int rowFieldIndex = rowType != null ? rowType.getFieldIndex(DELETE_FILE_ROW_FIELD_NAME) : -1;
      return Avro.writeDeletes(outputFile.encryptingOutputFile())
          .createWriterFunc(
              ignore ->
                  new FlinkAvroWriter(
                      rowFieldIndex == -1
                          ? rowType
                          : (RowType) rowType.getFields().get(rowFieldIndex).getType()));
    }
  }

  public static class ORCWriterService implements DataFileWriterService<RowType> {
    @Override
    public FileFormat format() {
      return FileFormat.ORC;
    }

    @Override
    public Class<?> returnType() {
      return RowData.class;
    }

    @Override
    public FileFormatAppenderBuilder<?> appenderBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return ORC.write(outputFile)
          .createWriterFunc((iSchema, typDesc) -> FlinkOrcWriter.buildWriter(rowType, iSchema));
    }

    @Override
    public FileFormatDataWriterBuilder<?> dataWriterBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return ORC.writeData(outputFile.encryptingOutputFile())
          .createWriterFunc((iSchema, typDesc) -> FlinkOrcWriter.buildWriter(rowType, iSchema));
    }

    @Override
    public FileFormatEqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return ORC.writeDeletes(outputFile.encryptingOutputFile())
          .transformPaths(path -> StringData.fromString(path.toString()))
          .createWriterFunc((iSchema, typDesc) -> FlinkOrcWriter.buildWriter(rowType, iSchema));
    }

    @Override
    public FileFormatPositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return ORC.writeDeletes(outputFile.encryptingOutputFile())
          .transformPaths(path -> StringData.fromString(path.toString()))
          .createWriterFunc((iSchema, typDesc) -> FlinkOrcWriter.buildWriter(rowType, iSchema));
    }
  }

  public static class ParquetWriterService implements DataFileWriterService<RowType> {
    @Override
    public FileFormat format() {
      return FileFormat.PARQUET;
    }

    @Override
    public Class<?> returnType() {
      return RowData.class;
    }

    @Override
    public FileFormatAppenderBuilder<?> appenderBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return Parquet.write(outputFile)
          .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(rowType, msgType));
    }

    @Override
    public FileFormatDataWriterBuilder<?> dataWriterBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return Parquet.writeData(outputFile.encryptingOutputFile())
          .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(rowType, msgType));
    }

    @Override
    public FileFormatEqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return Parquet.writeDeletes(outputFile.encryptingOutputFile())
          .transformPaths(path -> StringData.fromString(path.toString()))
          .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(rowType, msgType));
    }

    @Override
    public FileFormatPositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
        EncryptedOutputFile outputFile, RowType rowType) {
      return Parquet.writeDeletes(outputFile.encryptingOutputFile())
          .transformPaths(path -> StringData.fromString(path.toString()))
          .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(rowType, msgType));
    }
  }
}
