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
package org.apache.iceberg.io.datafile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ArrayUtil;

/**
 * Builder for generating {@link FileAppender}s, {@link DataWriter}, {@link EqualityDeleteWriter}
 * and {@link PositionDeleteWriter}s. The builder wraps the file format specific {@link
 * AppenderBuilder}. To allow further engine and file format specific configuration changes for the
 * given type of writer it uses {@link AppenderBuilder.Initializer} to finalize the appender
 * configuration before generating the actual writer.
 *
 * @param <A> type of the appender
 * @param <T> engine specific native type of the input records used for appender initialization
 */
public class WriteBuilder<A extends AppenderBuilder<A>, T> {
  private final AppenderBuilder<A> appenderBuilder;
  private final AppenderBuilder.Initializer initializer;
  private final String location;
  private final FileFormat format;
  private PartitionSpec spec = null;
  private StructLike partition = null;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder = null;
  private Schema rowSchema = null;
  private int[] equalityFieldIds = null;
  private T nativeType;

  WriteBuilder(
      AppenderBuilder<A> appenderBuilder,
      AppenderBuilder.Initializer initializer,
      String location,
      FileFormat format) {
    this.appenderBuilder = appenderBuilder;
    this.initializer = initializer;
    this.location = location;
    this.format = format;
  }

  /**
   * Sets the configurations coming from the table like {@link #schema(Schema)}, {@link #set(Map)}
   * and {@link #metricsConfig(MetricsConfig)}.
   */
  public WriteBuilder<A, T> forTable(Table table) {
    appenderBuilder.forTable(table);
    return this;
  }

  /** Set the file schema. */
  public WriteBuilder<A, T> schema(Schema newSchema) {
    appenderBuilder.schema(newSchema);
    return this;
  }

  /** Set the file schema's root name. */
  public WriteBuilder<A, T> named(String newName) {
    appenderBuilder.named(newName);
    return this;
  }

  /**
   * Set a writer configuration property.
   *
   * <p>Write configuration affects writer behavior. To add file metadata properties, use {@link
   * #meta(String, String)} or {@link #meta(Map)}.
   *
   * @param property a writer config property name
   * @param value config value
   * @return this for method chaining
   */
  public WriteBuilder<A, T> set(String property, String value) {
    appenderBuilder.set(property, value);
    return this;
  }

  /**
   * Set a writer configuration properties Map.
   *
   * <p>Write configuration affects writer behavior. To add file metadata properties, use {@link
   * #meta(String, String)} or {@link #meta(Map)}.
   *
   * @param properties a map of writer config properties
   * @return this for method chaining
   */
  public WriteBuilder<A, T> set(Map<String, String> properties) {
    properties.forEach(appenderBuilder::set);
    return this;
  }

  /**
   * Set a file metadata property.
   *
   * <p>Metadata properties are written into file metadata. To alter a writer configuration
   * property, use {@link #set(String, String)} or {@link #set(Map)}.
   *
   * @param property a file metadata property name
   * @param value config value
   * @return this for method chaining
   */
  public WriteBuilder<A, T> meta(String property, String value) {
    appenderBuilder.meta(property, value);
    return this;
  }

  /**
   * Set a file metadata properties Map.
   *
   * <p>Metadata properties are written into file metadata. To alter a writer configuration
   * property, use {@link #set(String, String)}.
   *
   * @param properties a map of file metadata properties
   * @return this for method chaining
   */
  public WriteBuilder<A, T> meta(Map<String, String> properties) {
    properties.forEach(appenderBuilder::meta);
    return this;
  }

  /** Sets the metrics configuration used for collecting column metrics for the created file. */
  public WriteBuilder<A, T> metricsConfig(MetricsConfig newMetricsConfig) {
    appenderBuilder.metricsConfig(newMetricsConfig);
    return this;
  }

  /** Overwrite the file if it already exists. */
  public WriteBuilder<A, T> overwrite() {
    return overwrite(true);
  }

  /** Sets the overwrite flag. */
  public WriteBuilder<A, T> overwrite(boolean enabled) {
    appenderBuilder.overwrite(enabled);
    return this;
  }

  /** Sets the encryption key used for writing the file. */
  public WriteBuilder<A, T> fileEncryptionKey(ByteBuffer encryptionKey) {
    appenderBuilder.fileEncryptionKey(encryptionKey);
    return this;
  }

  /** Sets the additional authentication data prefix used for writing the file. */
  public WriteBuilder<A, T> aADPrefix(ByteBuffer aadPrefix) {
    appenderBuilder.aADPrefix(aadPrefix);
    return this;
  }

  /** Sets the row schema for the delete writers. */
  public WriteBuilder<A, T> withRowSchema(Schema newSchema) {
    this.rowSchema = newSchema;
    return this;
  }

  /** Sets the equality field ids for the equality delete writer. */
  public WriteBuilder<A, T> withEqualityFieldIds(List<Integer> fieldIds) {
    this.equalityFieldIds = ArrayUtil.toIntArray(fieldIds);
    return this;
  }

  /** Sets the equality field ids for the equality delete writer. */
  public WriteBuilder<A, T> withEqualityFieldIds(int... fieldIds) {
    this.equalityFieldIds = fieldIds;
    return this;
  }

  /** Sets the partition specification for the content file metadata. */
  public WriteBuilder<A, T> withSpec(PartitionSpec newSpec) {
    this.spec = newSpec;
    return this;
  }

  /** Sets the partition value for the content file metadata. */
  public WriteBuilder<A, T> withPartition(StructLike newPartition) {
    this.partition = newPartition;
    return this;
  }

  /** Sets the encryption key metadata for the content file. */
  public WriteBuilder<A, T> withKeyMetadata(EncryptionKeyMetadata metadata) {
    this.keyMetadata = metadata;
    return this;
  }

  /** Sets the sort order for the content file metadata. */
  public WriteBuilder<A, T> withSortOrder(SortOrder newSortOrder) {
    this.sortOrder = newSortOrder;
    return this;
  }

  /**
   * Sets the engine specific type of the input. Provided for the {@link
   * AppenderBuilder.Initializer} methods to configure the engine specific converters.
   */
  public WriteBuilder<A, T> withNativeType(T newNativeType) {
    this.nativeType = newNativeType;
    return this;
  }

  /** Creates a {@link FileAppender} based on the configurations set. */
  public <D> FileAppender<D> appender() throws IOException {
    initializer
        .<A, T>buildInitializer(AppenderBuilder.WriteMode.APPENDER)
        .accept(appenderBuilder, nativeType);
    return appenderBuilder.build();
  }

  /**
   * Creates a writer which generates {@link org.apache.iceberg.DataFile}s based on the
   * configurations set.
   */
  public <D> DataWriter<D> dataWriter() throws IOException {
    Preconditions.checkArgument(spec != null, "Cannot create data writer without spec");
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null when creating data writer for partitioned spec");
    Preconditions.checkState(
        initializer != null, "Cannot create data file unless appenderInitializer is set");

    initializer
        .<A, T>buildInitializer(AppenderBuilder.WriteMode.DATA_WRITER)
        .accept(appenderBuilder, nativeType);
    return new org.apache.iceberg.io.DataWriter<>(
        appenderBuilder.build(), format, location, spec, partition, keyMetadata, sortOrder);
  }

  /**
   * Creates a writer which generates equality {@link DeleteFile}s based on the configurations set.
   */
  public <D> EqualityDeleteWriter<D> equalityDeleteWriter() throws IOException {
    Preconditions.checkState(
        rowSchema != null, "Cannot create equality delete file without a schema");
    Preconditions.checkState(
        equalityFieldIds != null, "Cannot create equality delete file without delete field ids");
    Preconditions.checkState(
        initializer != null, "Cannot create data file unless appenderInitializer is set");
    Preconditions.checkArgument(
        spec != null, "Spec must not be null when creating equality delete writer");
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null for partitioned writes");

    initializer
        .<A, T>buildInitializer(AppenderBuilder.WriteMode.EQUALITY_DELETE_WRITER)
        .accept(
            appenderBuilder
                .schema(rowSchema)
                .meta("delete-type", "equality")
                .meta(
                    "delete-field-ids",
                    IntStream.of(equalityFieldIds)
                        .mapToObj(Objects::toString)
                        .collect(Collectors.joining(", "))),
            nativeType);
    return new EqualityDeleteWriter<>(
        appenderBuilder.build(),
        format,
        location,
        spec,
        partition,
        keyMetadata,
        sortOrder,
        equalityFieldIds);
  }

  /**
   * Creates a writer which generates position {@link DeleteFile}s based on the configurations set.
   */
  public <D> PositionDeleteWriter<D> positionDeleteWriter() throws IOException {
    Preconditions.checkState(
        equalityFieldIds == null, "Cannot create position delete file using delete field ids");
    Preconditions.checkArgument(
        spec != null, "Spec must not be null when creating position delete writer");
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null for partitioned writes");
    Preconditions.checkState(
        initializer != null, "Cannot create data file unless appenderInitializer is set");

    if (rowSchema != null) {
      appenderBuilder.schema(DeleteSchemaUtil.posDeleteSchema(rowSchema));
      initializer
          .<A, T>buildInitializer(AppenderBuilder.WriteMode.POSITION_DELETE_WITH_ROW_WRITER)
          .accept(appenderBuilder, nativeType);
    } else {
      appenderBuilder.schema(DeleteSchemaUtil.pathPosSchema());
      initializer
          .<A, T>buildInitializer(AppenderBuilder.WriteMode.POSITION_DELETE_WRITER)
          .accept(appenderBuilder, nativeType);
    }

    return new PositionDeleteWriter<>(
        appenderBuilder.meta("delete-type", "position").build(),
        format,
        location,
        spec,
        partition,
        keyMetadata);
  }
}
