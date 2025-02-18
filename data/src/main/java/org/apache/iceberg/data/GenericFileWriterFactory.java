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
package org.apache.iceberg.data;

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
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GenericFileWriterFactory extends RegistryBasedFileWriterFactory<Record, Schema> {
  private static final Logger LOG = LoggerFactory.getLogger(GenericFileWriterFactory.class);

  private Table table;
  private FileFormat format;
  private Schema positionDeleteRowSchema;

  GenericFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      SortOrder equalityDeleteSortOrder,
      Schema positionDeleteRowSchema) {
    super(
        table,
        dataFileFormat,
        Record.class,
        dataSchema,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSortOrder,
        ImmutableMap.of(),
        dataSchema,
        equalityDeleteRowSchema);
    this.table = table;
    this.format = dataFileFormat;
    this.positionDeleteRowSchema = positionDeleteRowSchema;
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. It won't be called starting 1.10.0 as the
   *     configuration is done by the {@link FormatModelRegistry}.
   */
  @Deprecated
  protected void configureDataWrite(Avro.DataWriteBuilder builder) {
    throwUnsupportedOperationException();
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. It won't be called starting 1.10.0 as the
   *     configuration is done by the {@link FormatModelRegistry}.
   */
  @Deprecated
  protected void configureEqualityDelete(Avro.DeleteWriteBuilder builder) {
    throwUnsupportedOperationException();
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. It won't be called starting 1.10.0 as the
   *     configuration is done by the {@link FormatModelRegistry}.
   */
  @Deprecated
  protected void configurePositionDelete(Avro.DeleteWriteBuilder builder) {
    throwUnsupportedOperationException();
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. It won't be called starting 1.10.0 as the
   *     configuration is done by the {@link FormatModelRegistry}.
   */
  @Deprecated
  protected void configureDataWrite(Parquet.DataWriteBuilder builder) {
    throwUnsupportedOperationException();
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. It won't be called starting 1.10.0 as the
   *     configuration is done by the {@link FormatModelRegistry}.
   */
  @Deprecated
  protected void configureEqualityDelete(Parquet.DeleteWriteBuilder builder) {
    throwUnsupportedOperationException();
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. It won't be called starting 1.10.0 as the
   *     configuration is done by the {@link FormatModelRegistry}.
   */
  @Deprecated
  protected void configurePositionDelete(Parquet.DeleteWriteBuilder builder) {
    throwUnsupportedOperationException();
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. It won't be called starting 1.10.0 as the
   *     configuration is done by the {@link FormatModelRegistry}.
   */
  @Deprecated
  protected void configureDataWrite(ORC.DataWriteBuilder builder) {
    throwUnsupportedOperationException();
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. It won't be called starting 1.10.0 as the
   *     configuration is done by the {@link FormatModelRegistry}.
   */
  @Deprecated
  protected void configureEqualityDelete(ORC.DeleteWriteBuilder builder) {
    throwUnsupportedOperationException();
  }

  /**
   * @deprecated Since 1.10.0, will be removed in 1.11.0. It won't be called starting 1.10.0 as the
   *     configuration is done by the {@link FormatModelRegistry}.
   */
  @Deprecated
  protected void configurePositionDelete(ORC.DeleteWriteBuilder builder) {
    throwUnsupportedOperationException();
  }

  private void throwUnsupportedOperationException() {
    throw new UnsupportedOperationException(
        "Method is deprecated and should not be called. "
            + "Configuration is already done by the registry.");
  }

  @Override
  public PositionDeleteWriter<Record> newPositionDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    if (positionDeleteRowSchema == null) {
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
            return Avro.writeDeletes(file)
                .createWriterFunc(DataWriter::create)
                .withPartition(partition)
                .overwrite()
                .rowSchema(positionDeleteRowSchema)
                .withSpec(spec)
                .withKeyMetadata(file.keyMetadata())
                .buildPositionWriter();

          case ORC:
            return ORC.writeDeletes(file)
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .withPartition(partition)
                .overwrite()
                .rowSchema(positionDeleteRowSchema)
                .withSpec(spec)
                .withKeyMetadata(file.keyMetadata())
                .buildPositionWriter();

          case PARQUET:
            return Parquet.writeDeletes(file)
                .createWriterFunc(GenericParquetWriter::create)
                .withPartition(partition)
                .overwrite()
                .metricsConfig(metricsConfig)
                .rowSchema(positionDeleteRowSchema)
                .withSpec(spec)
                .withKeyMetadata(file.keyMetadata())
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
    private SortOrder dataSortOrder;
    private FileFormat deleteFileFormat;
    private int[] equalityFieldIds;
    private Schema equalityDeleteRowSchema;
    private SortOrder equalityDeleteSortOrder;
    private Schema positionDeleteRowSchema;

    Builder(Table table) {
      this.table = table;
      this.dataSchema = table.schema();

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

    Builder equalityDeleteSortOrder(SortOrder newEqualityDeleteSortOrder) {
      this.equalityDeleteSortOrder = newEqualityDeleteSortOrder;
      return this;
    }

    @Deprecated
    Builder positionDeleteRowSchema(Schema newPositionDeleteRowSchema) {
      this.positionDeleteRowSchema = newPositionDeleteRowSchema;
      return this;
    }

    GenericFileWriterFactory build() {
      boolean noEqualityDeleteConf = equalityFieldIds == null && equalityDeleteRowSchema == null;
      boolean fullEqualityDeleteConf = equalityFieldIds != null && equalityDeleteRowSchema != null;
      Preconditions.checkArgument(
          noEqualityDeleteConf || fullEqualityDeleteConf,
          "Equality field IDs and equality delete row schema must be set together");

      return new GenericFileWriterFactory(
          table,
          dataFileFormat,
          dataSchema,
          dataSortOrder,
          deleteFileFormat,
          equalityFieldIds,
          equalityDeleteRowSchema,
          equalityDeleteSortOrder,
          positionDeleteRowSchema);
    }
  }
}
