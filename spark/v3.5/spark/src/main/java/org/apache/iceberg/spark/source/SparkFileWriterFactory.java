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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_ROW_FIELD_NAME;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;

import java.util.Map;
import org.apache.iceberg.DataFileWriterService;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.RegistryBasedFileWriterFactory;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileFormatAppenderBuilder;
import org.apache.iceberg.io.FileFormatDataWriterBuilder;
import org.apache.iceberg.io.FileFormatEqualityDeleteWriterBuilder;
import org.apache.iceberg.io.FileFormatPositionDeleteWriterBuilder;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.SparkAvroWriter;
import org.apache.iceberg.spark.data.SparkOrcWriter;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

class SparkFileWriterFactory extends RegistryBasedFileWriterFactory<InternalRow, StructType> {

  SparkFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      StructType dataSparkType,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      StructType equalityDeleteSparkType,
      SortOrder equalityDeleteSortOrder,
      Schema positionDeleteRowSchema,
      StructType positionDeleteSparkType,
      Map<String, String> writeProperties) {

    super(
        table,
        dataFileFormat,
        InternalRow.class,
        dataSchema,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSortOrder,
        positionDeleteRowSchema,
        writeProperties,
        dataSparkType == null
            ? dataSchema == null ? null : SparkSchemaUtil.convert(dataSchema)
            : dataSparkType,
        equalityDeleteSparkType == null
            ? equalityDeleteRowSchema == null
                ? null
                : SparkSchemaUtil.convert(equalityDeleteRowSchema)
            : equalityDeleteSparkType,
        positionDeleteSparkType == null
            ? positionDeleteRowSchema == null
                ? null
                : SparkSchemaUtil.convert(DeleteSchemaUtil.posDeleteSchema(positionDeleteRowSchema))
            : positionDeleteSparkType);
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
  }

  @Override
  protected void configureDataWrite(Avro.DataWriteBuilder builder) {
    builder.createWriterFunc(ignored -> new SparkAvroWriter(rowSchemaType()));
    builder.setAll(writeProperties());
  }

  @Override
  protected void configureEqualityDelete(Avro.DeleteWriteBuilder builder) {
    builder.createWriterFunc(ignored -> new SparkAvroWriter(equalityDeleteRowSchemaType()));
    builder.setAll(writeProperties());
  }

  @Override
  protected void configurePositionDelete(Avro.DeleteWriteBuilder builder) {
    boolean withRow =
        positionDeleteRowSchemaType().getFieldIndex(DELETE_FILE_ROW_FIELD_NAME).isDefined();
    if (withRow) {
      // SparkAvroWriter accepts just the Spark type of the row ignoring the path and pos
      StructField rowField = positionDeleteRowSchemaType().apply(DELETE_FILE_ROW_FIELD_NAME);
      StructType positionDeleteRowSparkType = (StructType) rowField.dataType();
      builder.createWriterFunc(ignored -> new SparkAvroWriter(positionDeleteRowSparkType));
    }

    builder.setAll(writeProperties());
  }

  @Override
  protected void configureDataWrite(Parquet.DataWriteBuilder builder) {
    builder.createWriterFunc(msgType -> SparkParquetWriters.buildWriter(rowSchemaType(), msgType));
    builder.setAll(writeProperties());
  }

  @Override
  protected void configureEqualityDelete(Parquet.DeleteWriteBuilder builder) {
    builder.createWriterFunc(
        msgType -> SparkParquetWriters.buildWriter(equalityDeleteRowSchemaType(), msgType));
    builder.setAll(writeProperties());
  }

  @Override
  protected void configurePositionDelete(Parquet.DeleteWriteBuilder builder) {
    builder.createWriterFunc(
        msgType -> SparkParquetWriters.buildWriter(positionDeleteRowSchemaType(), msgType));
    builder.transformPaths(path -> UTF8String.fromString(path.toString()));
    builder.setAll(writeProperties());
  }

  @Override
  protected void configureDataWrite(ORC.DataWriteBuilder builder) {
    builder.createWriterFunc(SparkOrcWriter::new);
    builder.setAll(writeProperties());
  }

  @Override
  protected void configureEqualityDelete(ORC.DeleteWriteBuilder builder) {
    builder.createWriterFunc(SparkOrcWriter::new);
    builder.setAll(writeProperties());
  }

  @Override
  protected void configurePositionDelete(ORC.DeleteWriteBuilder builder) {
    builder.createWriterFunc(SparkOrcWriter::new);
    builder.transformPaths(path -> UTF8String.fromString(path.toString()));
    builder.setAll(writeProperties());
  }

  static class Builder {
    private final Table table;
    private FileFormat dataFileFormat;
    private Schema dataSchema;
    private StructType dataSparkType;
    private SortOrder dataSortOrder;
    private FileFormat deleteFileFormat;
    private int[] equalityFieldIds;
    private Schema equalityDeleteRowSchema;
    private StructType equalityDeleteSparkType;
    private SortOrder equalityDeleteSortOrder;
    private Schema positionDeleteRowSchema;
    private StructType positionDeleteSparkType;
    private Map<String, String> writeProperties;

    Builder(Table table) {
      this.table = table;

      Map<String, String> properties = table.properties();

      String dataFileFormatName =
          properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
      this.dataFileFormat = FileFormat.fromString(dataFileFormatName);

      String deleteFileFormatName =
          properties.getOrDefault(DELETE_DEFAULT_FILE_FORMAT, dataFileFormatName);
      this.deleteFileFormat = FileFormat.fromString(deleteFileFormatName);
    }

    Builder dataFileFormat(FileFormat newDataFileFormat) {
      this.dataFileFormat = newDataFileFormat;
      return this;
    }

    Builder dataSchema(Schema newDataSchema) {
      this.dataSchema = newDataSchema;
      return this;
    }

    Builder dataSparkType(StructType newDataSparkType) {
      this.dataSparkType = newDataSparkType;
      return this;
    }

    Builder dataSortOrder(SortOrder newDataSortOrder) {
      this.dataSortOrder = newDataSortOrder;
      return this;
    }

    Builder deleteFileFormat(FileFormat newDeleteFileFormat) {
      this.deleteFileFormat = newDeleteFileFormat;
      return this;
    }

    Builder equalityFieldIds(int[] newEqualityFieldIds) {
      this.equalityFieldIds = newEqualityFieldIds;
      return this;
    }

    Builder equalityDeleteRowSchema(Schema newEqualityDeleteRowSchema) {
      this.equalityDeleteRowSchema = newEqualityDeleteRowSchema;
      return this;
    }

    Builder equalityDeleteSparkType(StructType newEqualityDeleteSparkType) {
      this.equalityDeleteSparkType = newEqualityDeleteSparkType;
      return this;
    }

    Builder equalityDeleteSortOrder(SortOrder newEqualityDeleteSortOrder) {
      this.equalityDeleteSortOrder = newEqualityDeleteSortOrder;
      return this;
    }

    Builder positionDeleteRowSchema(Schema newPositionDeleteRowSchema) {
      this.positionDeleteRowSchema = newPositionDeleteRowSchema;
      return this;
    }

    Builder positionDeleteSparkType(StructType newPositionDeleteSparkType) {
      this.positionDeleteSparkType = newPositionDeleteSparkType;
      return this;
    }

    Builder writeProperties(Map<String, String> properties) {
      this.writeProperties = properties;
      return this;
    }

    SparkFileWriterFactory build() {
      boolean noEqualityDeleteConf = equalityFieldIds == null && equalityDeleteRowSchema == null;
      boolean fullEqualityDeleteConf = equalityFieldIds != null && equalityDeleteRowSchema != null;
      Preconditions.checkArgument(
          noEqualityDeleteConf || fullEqualityDeleteConf,
          "Equality field IDs and equality delete row schema must be set together");

      return new SparkFileWriterFactory(
          table,
          dataFileFormat,
          dataSchema,
          dataSparkType,
          dataSortOrder,
          deleteFileFormat,
          equalityFieldIds,
          equalityDeleteRowSchema,
          equalityDeleteSparkType,
          equalityDeleteSortOrder,
          positionDeleteRowSchema,
          positionDeleteSparkType,
          writeProperties);
    }
  }

  public static class AvroWriterService implements DataFileWriterService<StructType> {
    @Override
    public FileFormat format() {
      return FileFormat.AVRO;
    }

    @Override
    public Class<?> returnType() {
      return InternalRow.class;
    }

    @Override
    public FileFormatAppenderBuilder<?> appenderBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return Avro.write(outputFile).createWriterFunc(ignore -> new SparkAvroWriter(rowType));
    }

    @Override
    public FileFormatDataWriterBuilder<?> dataWriterBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return Avro.writeData(outputFile.encryptingOutputFile())
          .createWriterFunc(ignore -> new SparkAvroWriter(rowType));
    }

    @Override
    public FileFormatEqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return Avro.writeDeletes(outputFile.encryptingOutputFile())
          .createWriterFunc(ignore -> new SparkAvroWriter(rowType));
    }

    @Override
    public FileFormatPositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      Avro.DeleteWriteBuilder builder = Avro.writeDeletes(outputFile.encryptingOutputFile());
      boolean withRow =
          rowType != null && rowType.getFieldIndex(DELETE_FILE_ROW_FIELD_NAME).isDefined();
      if (withRow) {
        // SparkAvroWriter accepts just the Spark type of the row ignoring the path and pos
        StructField rowField = rowType.apply(DELETE_FILE_ROW_FIELD_NAME);
        StructType positionDeleteRowSparkType = (StructType) rowField.dataType();
        builder.createWriterFunc(ignored -> new SparkAvroWriter(positionDeleteRowSparkType));
      } else {
        builder.createWriterFunc(ignore -> new SparkAvroWriter(rowType));
      }

      return builder;
    }
  }

  public static class ORCWriterService implements DataFileWriterService<StructType> {
    @Override
    public FileFormat format() {
      return FileFormat.ORC;
    }

    @Override
    public Class<?> returnType() {
      return InternalRow.class;
    }

    @Override
    public FileFormatAppenderBuilder<?> appenderBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return ORC.write(outputFile).createWriterFunc(SparkOrcWriter::new);
    }

    @Override
    public FileFormatDataWriterBuilder<?> dataWriterBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return ORC.writeData(outputFile.encryptingOutputFile()).createWriterFunc(SparkOrcWriter::new);
    }

    @Override
    public FileFormatEqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return ORC.writeDeletes(outputFile.encryptingOutputFile())
          .transformPaths(path -> UTF8String.fromString(path.toString()))
          .createWriterFunc(SparkOrcWriter::new);
    }

    @Override
    public FileFormatPositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return ORC.writeDeletes(outputFile.encryptingOutputFile())
          .transformPaths(path -> UTF8String.fromString(path.toString()))
          .createWriterFunc(SparkOrcWriter::new);
    }
  }

  public static class ParquetWriterService implements DataFileWriterService<StructType> {
    @Override
    public FileFormat format() {
      return FileFormat.PARQUET;
    }

    @Override
    public Class<?> returnType() {
      return InternalRow.class;
    }

    @Override
    public FileFormatAppenderBuilder<?> appenderBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return Parquet.write(outputFile)
          .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(rowType, msgType));
    }

    @Override
    public FileFormatDataWriterBuilder<?> dataWriterBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return Parquet.writeData(outputFile.encryptingOutputFile())
          .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(rowType, msgType));
    }

    @Override
    public FileFormatEqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return Parquet.writeDeletes(outputFile.encryptingOutputFile())
          .transformPaths(path -> UTF8String.fromString(path.toString()))
          .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(rowType, msgType));
    }

    @Override
    public FileFormatPositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
        EncryptedOutputFile outputFile, StructType rowType) {
      return Parquet.writeDeletes(outputFile.encryptingOutputFile())
          .transformPaths(path -> UTF8String.fromString(path.toString()))
          .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(rowType, msgType));
    }
  }
}
