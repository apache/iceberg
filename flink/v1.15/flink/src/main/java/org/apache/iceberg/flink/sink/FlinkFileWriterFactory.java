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
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;

import java.io.Serializable;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.BaseFileWriterFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.data.FlinkAvroWriter;
import org.apache.iceberg.flink.data.FlinkOrcWriter;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class FlinkFileWriterFactory extends BaseFileWriterFactory<RowData> implements Serializable {
  private RowType dataFlinkType;
  private RowType equalityDeleteFlinkType;
  private RowType positionDeleteFlinkType;

  FlinkFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      RowType dataFlinkType,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      RowType equalityDeleteFlinkType,
      SortOrder equalityDeleteSortOrder,
      Schema positionDeleteRowSchema,
      RowType positionDeleteFlinkType) {

    super(
        table,
        dataFileFormat,
        dataSchema,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSortOrder,
        positionDeleteRowSchema);

    this.dataFlinkType = dataFlinkType;
    this.equalityDeleteFlinkType = equalityDeleteFlinkType;
    this.positionDeleteFlinkType = positionDeleteFlinkType;
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
  }

  @Override
  protected void configureDataWrite(Avro.DataWriteBuilder builder) {
    builder.createWriterFunc(ignore -> new FlinkAvroWriter(dataFlinkType()));
  }

  @Override
  protected void configureEqualityDelete(Avro.DeleteWriteBuilder builder) {
    builder.createWriterFunc(ignored -> new FlinkAvroWriter(equalityDeleteFlinkType()));
  }

  @Override
  protected void configurePositionDelete(Avro.DeleteWriteBuilder builder) {
    int rowFieldIndex = positionDeleteFlinkType().getFieldIndex(DELETE_FILE_ROW_FIELD_NAME);
    if (rowFieldIndex >= 0) {
      // FlinkAvroWriter accepts just the Flink type of the row ignoring the path and pos
      RowType positionDeleteRowFlinkType =
          (RowType) positionDeleteFlinkType().getTypeAt(rowFieldIndex);
      builder.createWriterFunc(ignored -> new FlinkAvroWriter(positionDeleteRowFlinkType));
    }
  }

  @Override
  protected void configureDataWrite(Parquet.DataWriteBuilder builder) {
    builder.createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(dataFlinkType(), msgType));
  }

  @Override
  protected void configureEqualityDelete(Parquet.DeleteWriteBuilder builder) {
    builder.createWriterFunc(
        msgType -> FlinkParquetWriters.buildWriter(equalityDeleteFlinkType(), msgType));
  }

  @Override
  protected void configurePositionDelete(Parquet.DeleteWriteBuilder builder) {
    builder.createWriterFunc(
        msgType -> FlinkParquetWriters.buildWriter(positionDeleteFlinkType(), msgType));
    builder.transformPaths(path -> StringData.fromString(path.toString()));
  }

  @Override
  protected void configureDataWrite(ORC.DataWriteBuilder builder) {
    builder.createWriterFunc(
        (iSchema, typDesc) -> FlinkOrcWriter.buildWriter(dataFlinkType(), iSchema));
  }

  @Override
  protected void configureEqualityDelete(ORC.DeleteWriteBuilder builder) {
    builder.createWriterFunc(
        (iSchema, typDesc) -> FlinkOrcWriter.buildWriter(equalityDeleteFlinkType(), iSchema));
  }

  @Override
  protected void configurePositionDelete(ORC.DeleteWriteBuilder builder) {
    builder.createWriterFunc(
        (iSchema, typDesc) -> FlinkOrcWriter.buildWriter(positionDeleteFlinkType(), iSchema));
    builder.transformPaths(path -> StringData.fromString(path.toString()));
  }

  private RowType dataFlinkType() {
    if (dataFlinkType == null) {
      Preconditions.checkNotNull(dataSchema(), "Data schema must not be null");
      this.dataFlinkType = FlinkSchemaUtil.convert(dataSchema());
    }

    return dataFlinkType;
  }

  private RowType equalityDeleteFlinkType() {
    if (equalityDeleteFlinkType == null) {
      Preconditions.checkNotNull(
          equalityDeleteRowSchema(), "Equality delete schema must not be null");
      this.equalityDeleteFlinkType = FlinkSchemaUtil.convert(equalityDeleteRowSchema());
    }

    return equalityDeleteFlinkType;
  }

  private RowType positionDeleteFlinkType() {
    if (positionDeleteFlinkType == null) {
      // wrap the optional row schema into the position delete schema that contains path and
      // position
      Schema positionDeleteSchema = DeleteSchemaUtil.posDeleteSchema(positionDeleteRowSchema());
      this.positionDeleteFlinkType = FlinkSchemaUtil.convert(positionDeleteSchema);
    }

    return positionDeleteFlinkType;
  }

  static class Builder {
    private final Table table;
    private FileFormat dataFileFormat;
    private Schema dataSchema;
    private RowType dataFlinkType;
    private SortOrder dataSortOrder;
    private FileFormat deleteFileFormat;
    private int[] equalityFieldIds;
    private Schema equalityDeleteRowSchema;
    private RowType equalityDeleteFlinkType;
    private SortOrder equalityDeleteSortOrder;
    private Schema positionDeleteRowSchema;
    private RowType positionDeleteFlinkType;

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

    /**
     * Sets a Flink type for data.
     *
     * <p>If not set, the value is derived from the provided Iceberg schema.
     */
    Builder dataFlinkType(RowType newDataFlinkType) {
      this.dataFlinkType = newDataFlinkType;
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

    /**
     * Sets a Flink type for equality deletes.
     *
     * <p>If not set, the value is derived from the provided Iceberg schema.
     */
    Builder equalityDeleteFlinkType(RowType newEqualityDeleteFlinkType) {
      this.equalityDeleteFlinkType = newEqualityDeleteFlinkType;
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

    /**
     * Sets a Flink type for position deletes.
     *
     * <p>If not set, the value is derived from the provided Iceberg schema.
     */
    Builder positionDeleteFlinkType(RowType newPositionDeleteFlinkType) {
      this.positionDeleteFlinkType = newPositionDeleteFlinkType;
      return this;
    }

    FlinkFileWriterFactory build() {
      boolean noEqualityDeleteConf = equalityFieldIds == null && equalityDeleteRowSchema == null;
      boolean fullEqualityDeleteConf = equalityFieldIds != null && equalityDeleteRowSchema != null;
      Preconditions.checkArgument(
          noEqualityDeleteConf || fullEqualityDeleteConf,
          "Equality field IDs and equality delete row schema must be set together");

      return new FlinkFileWriterFactory(
          table,
          dataFileFormat,
          dataSchema,
          dataFlinkType,
          dataSortOrder,
          deleteFileFormat,
          equalityFieldIds,
          equalityDeleteRowSchema,
          equalityDeleteFlinkType,
          equalityDeleteSortOrder,
          positionDeleteRowSchema,
          positionDeleteFlinkType);
    }
  }
}
