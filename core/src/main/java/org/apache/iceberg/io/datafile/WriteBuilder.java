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

public class WriteBuilder<A extends AppenderBuilder<A>, T> {
  private final AppenderBuilder<A> appenderBuilder;
  private final DataFileServiceRegistry.InitBuilder initBuilder;
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
      DataFileServiceRegistry.InitBuilder initBuilder,
      String location,
      FileFormat format) {
    this.appenderBuilder = appenderBuilder;
    this.initBuilder = initBuilder;
    this.location = location;
    this.format = format;
  }

  public WriteBuilder<A, T> forTable(Table table) {
    appenderBuilder.forTable(table);
    return this;
  }

  public WriteBuilder<A, T> schema(Schema newSchema) {
    appenderBuilder.schema(newSchema);
    return this;
  }

  public WriteBuilder<A, T> named(String newName) {
    appenderBuilder.named(newName);
    return this;
  }

  public WriteBuilder<A, T> set(String property, String value) {
    appenderBuilder.set(property, value);
    return this;
  }

  public WriteBuilder<A, T> setAll(Map<String, String> properties) {
    appenderBuilder.setAll(properties);
    return this;
  }

  public WriteBuilder<A, T> meta(String property, String value) {
    appenderBuilder.meta(property, value);
    return this;
  }

  public WriteBuilder<A, T> metricsConfig(MetricsConfig newMetricsConfig) {
    appenderBuilder.metricsConfig(newMetricsConfig);
    return this;
  }

  public WriteBuilder<A, T> overwrite() {
    appenderBuilder.overwrite();
    return this;
  }

  public WriteBuilder<A, T> overwrite(boolean enabled) {
    appenderBuilder.overwrite(enabled);
    return this;
  }

  public WriteBuilder<A, T> fileEncryptionKey(ByteBuffer encryptionKey) {
    appenderBuilder.fileEncryptionKey(encryptionKey);
    return this;
  }

  public WriteBuilder<A, T> aADPrefix(ByteBuffer aadPrefix) {
    appenderBuilder.aADPrefix(aadPrefix);
    return this;
  }

  public WriteBuilder<A, T> withRowSchema(Schema newSchema) {
    this.rowSchema = newSchema;
    return this;
  }

  public WriteBuilder<A, T> withEqualityFieldIds(List<Integer> fieldIds) {
    this.equalityFieldIds = ArrayUtil.toIntArray(fieldIds);
    return this;
  }

  public WriteBuilder<A, T> withEqualityFieldIds(int... fieldIds) {
    this.equalityFieldIds = fieldIds;
    return this;
  }

  public WriteBuilder<A, T> withSpec(PartitionSpec newSpec) {
    this.spec = newSpec;
    return this;
  }

  public WriteBuilder<A, T> withPartition(StructLike newPartition) {
    this.partition = newPartition;
    return this;
  }

  public WriteBuilder<A, T> withKeyMetadata(EncryptionKeyMetadata metadata) {
    this.keyMetadata = metadata;
    return this;
  }

  public WriteBuilder<A, T> withSortOrder(SortOrder newSortOrder) {
    this.sortOrder = newSortOrder;
    return this;
  }

  public WriteBuilder<A, T> withNativeType(T newNativeType) {
    this.nativeType = newNativeType;
    return this;
  }

  public <D> FileAppender<D> appender() throws IOException {
    initBuilder
        .<A, T>build(DataFileServiceRegistry.WriteMode.APPENDER)
        .accept(appenderBuilder, nativeType);
    return appenderBuilder.build();
  }

  public <D> DataWriter<D> dataWriter() throws IOException {
    Preconditions.checkArgument(spec != null, "Cannot create data writer without spec");
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null when creating data writer for partitioned spec");
    Preconditions.checkState(
        initBuilder != null, "Cannot create data file unless initBuilder is set");

    initBuilder
        .<A, T>build(DataFileServiceRegistry.WriteMode.DATA_WRITER)
        .accept(appenderBuilder, nativeType);
    return new org.apache.iceberg.io.DataWriter<>(
        appenderBuilder.build(), format, location, spec, partition, keyMetadata, sortOrder);
  }

  public <D> EqualityDeleteWriter<D> equalityDeleteWriter() throws IOException {
    Preconditions.checkState(
        rowSchema != null, "Cannot create equality delete file without a schema");
    Preconditions.checkState(
        equalityFieldIds != null, "Cannot create equality delete file without delete field ids");
    Preconditions.checkState(
        initBuilder != null, "Cannot create data file unless initBuilder is set");
    Preconditions.checkArgument(
        spec != null, "Spec must not be null when creating equality delete writer");
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null for partitioned writes");

    initBuilder
        .<A, T>build(DataFileServiceRegistry.WriteMode.EQUALITY_DELETE_WRITER)
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

  public <D> PositionDeleteWriter<D> positionDeleteWriter() throws IOException {
    Preconditions.checkState(
        equalityFieldIds == null, "Cannot create position delete file using delete field ids");
    Preconditions.checkArgument(
        spec != null, "Spec must not be null when creating position delete writer");
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null for partitioned writes");
    Preconditions.checkState(
        initBuilder != null, "Cannot create data file unless initBuilder is set");

    if (rowSchema != null) {
      appenderBuilder.schema(DeleteSchemaUtil.posDeleteSchema(rowSchema));
      initBuilder
          .<A, T>build(DataFileServiceRegistry.WriteMode.POSITION_DELETE_WITH_ROW_WRITER)
          .accept(appenderBuilder, nativeType);
    } else {
      appenderBuilder.schema(DeleteSchemaUtil.pathPosSchema());
      initBuilder
          .<A, T>build(DataFileServiceRegistry.WriteMode.POSITION_DELETE_WRITER)
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
