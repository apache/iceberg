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
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.formats.DataWriteBuilder;
import org.apache.iceberg.formats.EqualityDeleteWriteBuilder;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.PositionDeleteWriteBuilder;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * A base writer factory to be extended by query engine integrations.
 *
 * @param <T> row type
 */
public abstract class RegistryBasedFileWriterFactory<T, S> implements FileWriterFactory<T> {
  private final Table table;
  private final FileFormat dataFileFormat;
  private final Class<T> inputType;
  private final Schema dataSchema;
  private final SortOrder dataSortOrder;
  private final FileFormat deleteFileFormat;
  private final int[] equalityFieldIds;
  private final Schema equalityDeleteRowSchema;
  private final SortOrder equalityDeleteSortOrder;
  private final Map<String, String> writeProperties;
  private final S inputSchema;
  private final S equalityDeleteInputSchema;

  protected RegistryBasedFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Class<T> inputType,
      Schema dataSchema,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      SortOrder equalityDeleteSortOrder,
      Map<String, String> writeProperties,
      S inputSchema,
      S equalityDeleteInputSchema) {
    this.table = table;
    this.dataFileFormat = dataFileFormat;
    this.inputType = inputType;
    this.dataSchema = dataSchema;
    this.dataSortOrder = dataSortOrder;
    this.deleteFileFormat = deleteFileFormat;
    this.equalityFieldIds = equalityFieldIds;
    this.equalityDeleteRowSchema = equalityDeleteRowSchema;
    this.equalityDeleteSortOrder = equalityDeleteSortOrder;
    this.writeProperties = writeProperties != null ? writeProperties : ImmutableMap.of();
    this.inputSchema = inputSchema;
    this.equalityDeleteInputSchema = equalityDeleteInputSchema;
  }

  protected S inputSchema() {
    return inputSchema;
  }

  protected S equalityDeleteInputSchema() {
    return equalityDeleteInputSchema;
  }

  @Override
  public DataWriter<T> newDataWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    Preconditions.checkNotNull(dataSchema, "Data schema must not be null");
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    Map<String, String> properties = table.properties();
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);

    try {
      DataWriteBuilder<T, S> builder =
          FormatModelRegistry.dataWriteBuilder(dataFileFormat, inputType, file);
      return builder
          .schema(dataSchema)
          .inputSchema(inputSchema())
          .setAll(properties)
          .setAll(writeProperties)
          .metricsConfig(metricsConfig)
          .spec(spec)
          .partition(partition)
          .keyMetadata(keyMetadata)
          .sortOrder(dataSortOrder)
          .overwrite()
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public EqualityDeleteWriter<T> newEqualityDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    Preconditions.checkNotNull(equalityDeleteRowSchema, "Equality delete schema must not be null");

    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    Map<String, String> properties = table.properties();
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);

    try {
      EqualityDeleteWriteBuilder<T, S> builder =
          FormatModelRegistry.equalityDeleteWriteBuilder(deleteFileFormat, inputType, file);
      return builder
          .setAll(properties)
          .setAll(writeProperties)
          .metricsConfig(metricsConfig)
          .rowSchema(equalityDeleteRowSchema)
          .inputSchema(equalityDeleteInputSchema())
          .equalityFieldIds(equalityFieldIds)
          .spec(spec)
          .partition(partition)
          .keyMetadata(keyMetadata)
          .sortOrder(equalityDeleteSortOrder)
          .overwrite()
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new equality delete writer", e);
    }
  }

  @Override
  public PositionDeleteWriter<T> newPositionDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    Map<String, String> properties = table.properties();
    MetricsConfig metricsConfig = MetricsConfig.forPositionDelete(table);

    try {
      PositionDeleteWriteBuilder builder =
          FormatModelRegistry.positionDeleteWriteBuilder(deleteFileFormat, file);
      return builder
          .setAll(properties)
          .setAll(writeProperties)
          .metricsConfig(metricsConfig)
          .spec(spec)
          .partition(partition)
          .keyMetadata(keyMetadata)
          .overwrite()
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new position delete writer", e);
    }
  }
}
