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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.RegistryBasedFileWriterFactory;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.SparkAvroWriter;
import org.apache.iceberg.spark.data.SparkOrcWriter;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkFileWriterFactory extends RegistryBasedFileWriterFactory<InternalRow, StructType> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkFileWriterFactory.class);
  private StructType positionDeleteSparkType;
  private boolean useDeprecatedPositionDeleteWriter = false;
  private final Schema positionDeleteRowSchema;
  private final Table table;
  private final FileFormat format;
  private final Map<String, String> writeProperties;

  /**
   * @deprecated This constructor is deprecated as of version 1.11.0 and will be removed in 1.12.0.
   *     Position deletes that include row data are no longer supported. Use {@link
   *     #SparkFileWriterFactory(Table, FileFormat, Schema, StructType, SortOrder, FileFormat,
   *     int[], Schema, StructType, SortOrder, Map)} instead.
   */
  @Deprecated
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
        writeProperties,
        calculateSparkType(dataSparkType, dataSchema),
        calculateSparkType(equalityDeleteSparkType, equalityDeleteRowSchema));

    this.table = table;
    this.format = dataFileFormat;
    this.writeProperties = writeProperties != null ? writeProperties : ImmutableMap.of();
    this.positionDeleteRowSchema = positionDeleteRowSchema;
    this.positionDeleteSparkType = positionDeleteSparkType;
    this.useDeprecatedPositionDeleteWriter =
        positionDeleteRowSchema != null
            || (positionDeleteSparkType != null
                && positionDeleteSparkType.getFieldIndex(DELETE_FILE_ROW_FIELD_NAME).isDefined());
  }

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
        writeProperties,
        calculateSparkType(dataSparkType, dataSchema),
        calculateSparkType(equalityDeleteSparkType, equalityDeleteRowSchema));

    this.table = table;
    this.format = dataFileFormat;
    this.writeProperties = writeProperties != null ? writeProperties : ImmutableMap.of();
    this.positionDeleteRowSchema = null;
    this.positionDeleteSparkType = null;
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
  }

  private StructType positionDeleteSparkType() {
    if (positionDeleteSparkType == null) {
      // wrap the optional row schema into the position delete schema containing path and position
      Schema positionDeleteSchema = DeleteSchemaUtil.posDeleteSchema(positionDeleteRowSchema);
      this.positionDeleteSparkType = SparkSchemaUtil.convert(positionDeleteSchema);
    }

    return positionDeleteSparkType;
  }

  @Override
  public PositionDeleteWriter<InternalRow> newPositionDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    if (!useDeprecatedPositionDeleteWriter) {
      return super.newPositionDeleteWriter(file, spec, partition);
    } else {
      LOG.info(
          "Deprecated feature used. Position delete row schema is used to create the position delete writer.");
      MetricsConfig metricsConfig =
          table != null
              ? MetricsConfig.forPositionDelete(table)
              : MetricsConfig.fromProperties(ImmutableMap.of());

      try {
        switch (format) {
          case AVRO:
            StructType positionDeleteRowSparkType =
                (StructType) positionDeleteSparkType().apply(DELETE_FILE_ROW_FIELD_NAME).dataType();

            return Avro.writeDeletes(file)
                .createWriterFunc(ignored -> new SparkAvroWriter(positionDeleteRowSparkType))
                .withPartition(partition)
                .overwrite()
                .rowSchema(positionDeleteRowSchema)
                .withSpec(spec)
                .withKeyMetadata(file.keyMetadata())
                .setAll(writeProperties)
                .metricsConfig(metricsConfig)
                .buildPositionWriter();

          case ORC:
            return ORC.writeDeletes(file)
                .createWriterFunc(SparkOrcWriter::new)
                .transformPaths(path -> UTF8String.fromString(path.toString()))
                .withPartition(partition)
                .overwrite()
                .rowSchema(positionDeleteRowSchema)
                .withSpec(spec)
                .withKeyMetadata(file.keyMetadata())
                .setAll(writeProperties)
                .metricsConfig(metricsConfig)
                .buildPositionWriter();

          case PARQUET:
            return Parquet.writeDeletes(file)
                .createWriterFunc(
                    msgType -> SparkParquetWriters.buildWriter(positionDeleteSparkType(), msgType))
                .transformPaths(path -> UTF8String.fromString(path.toString()))
                .withPartition(partition)
                .overwrite()
                .metricsConfig(metricsConfig)
                .rowSchema(positionDeleteRowSchema)
                .withSpec(spec)
                .withKeyMetadata(file.keyMetadata())
                .setAll(writeProperties)
                .metricsConfig(metricsConfig)
                .buildPositionWriter();

          default:
            throw new UnsupportedOperationException(
                "Cannot write pos-deletes for unsupported file format: " + format);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
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

    /**
     * @deprecated This method is deprecated as of version 1.11.0 and will be removed in 1.12.0.
     *     Position deletes that include row data are no longer supported.
     */
    @Deprecated
    Builder positionDeleteRowSchema(Schema newPositionDeleteRowSchema) {
      this.positionDeleteRowSchema = newPositionDeleteRowSchema;
      return this;
    }

    /**
     * @deprecated This method is deprecated as of version 1.11.0 and will be removed in 1.12.0.
     *     Position deletes that include row data are no longer supported.
     */
    @Deprecated
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

  private static StructType calculateSparkType(StructType sparkType, Schema schema) {
    if (sparkType != null) {
      return sparkType;
    } else if (schema != null) {
      return SparkSchemaUtil.convert(schema);
    } else {
      return null;
    }
  }
}
