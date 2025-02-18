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

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;

import java.io.Serializable;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.RegistryBasedFileWriterFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.data.FlinkObjectModels;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

class FlinkFileWriterFactory extends RegistryBasedFileWriterFactory<RowData, RowType>
    implements Serializable {

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
        FlinkObjectModels.FLINK_OBJECT_MODEL,
        dataSchema,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSortOrder,
        positionDeleteRowSchema,
        ImmutableMap.of(),
        dataFlinkType == null ? FlinkSchemaUtil.convert(dataSchema) : dataFlinkType,
        equalityDeleteFlinkType == null
            ? equalityDeleteRowSchema == null
                ? null
                : FlinkSchemaUtil.convert(equalityDeleteRowSchema)
            : equalityDeleteFlinkType,
        positionDeleteFlinkType == null
            ? positionDeleteRowSchema == null
                ? null
                : FlinkSchemaUtil.convert(DeleteSchemaUtil.posDeleteSchema(positionDeleteRowSchema))
            : positionDeleteFlinkType);
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
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
