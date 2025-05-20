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
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.WriteBuilder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ArrayUtil;

/**
 * An internal implementation that handles all {@link ContentFileWriteBuilder} interface variants.
 *
 * <p>This unified implementation serves as a backend for multiple specialized content writers:
 *
 * <ul>
 *   <li>{@link DataWriteBuilder} for creating data files
 *   <li>{@link EqualityDeleteWriteBuilder} for creating equality delete files
 *   <li>{@link PositionDeleteWriteBuilder} for creating position delete files
 * </ul>
 *
 * <p>The implementation delegates to a format-specific {@link WriteBuilder} while enriching it with
 * content-specific functionality. When building a writer, the implementation configures the
 * underlying builder and calls its {@link WriteBuilder#build()} method to create the appropriate
 * specialized writer for the requested content type.
 *
 * @param <C> the concrete builder type for method chaining
 * @param <W> the type of the wrapped format-specific writer builder
 * @param <E> the engine-specific schema type required by the writer
 */
@SuppressWarnings("unchecked")
class ContentFileWriteBuilderImpl<
        C extends ContentFileWriteBuilderImpl<C, W, E>, W extends WriteBuilder<W, E>, E>
    implements DataWriteBuilder<C, E>,
        EqualityDeleteWriteBuilder<C, E>,
        PositionDeleteWriteBuilder<C, E> {
  private final org.apache.iceberg.io.WriteBuilder<W, E> writeBuilder;
  private final String location;
  private final FileFormat format;
  private PartitionSpec spec = null;
  private StructLike partition = null;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder = null;
  private Schema rowSchema = null;
  private int[] equalityFieldIds = null;

  ContentFileWriteBuilderImpl(
      org.apache.iceberg.io.WriteBuilder<W, E> writeBuilder, String location, FileFormat format) {
    this.writeBuilder = writeBuilder;
    this.location = location;
    this.format = format;
  }

  @Override
  public C schema(Schema newSchema) {
    writeBuilder.schema(newSchema);
    return (C) this;
  }

  @Override
  public C dataSchema(E engineSchema) {
    writeBuilder.dataSchema(engineSchema);
    return (C) this;
  }

  @Override
  public C set(String property, String value) {
    writeBuilder.set(property, value);
    return (C) this;
  }

  @Override
  public C set(Map<String, String> properties) {
    properties.forEach(writeBuilder::set);
    return (C) this;
  }

  @Override
  public C meta(String property, String value) {
    writeBuilder.meta(property, value);
    return (C) this;
  }

  @Override
  public C meta(Map<String, String> properties) {
    properties.forEach(writeBuilder::meta);
    return (C) this;
  }

  @Override
  public C metricsConfig(MetricsConfig newMetricsConfig) {
    writeBuilder.metricsConfig(newMetricsConfig);
    return (C) this;
  }

  @Override
  public C overwrite() {
    writeBuilder.overwrite();
    return (C) this;
  }

  @Override
  public C fileEncryptionKey(ByteBuffer encryptionKey) {
    writeBuilder.fileEncryptionKey(encryptionKey);
    return (C) this;
  }

  @Override
  public C aadPrefix(ByteBuffer aadPrefix) {
    writeBuilder.aadPrefix(aadPrefix);
    return (C) this;
  }

  @Override
  public C rowSchema(Schema newSchema) {
    this.rowSchema = newSchema;
    return (C) this;
  }

  @Override
  public C equalityFieldIds(List<Integer> fieldIds) {
    this.equalityFieldIds = ArrayUtil.toIntArray(fieldIds);
    return (C) this;
  }

  @Override
  public C equalityFieldIds(int... fieldIds) {
    this.equalityFieldIds = fieldIds;
    return (C) this;
  }

  @Override
  public C spec(PartitionSpec newSpec) {
    this.spec = newSpec;
    return (C) this;
  }

  @Override
  public C partition(StructLike newPartition) {
    this.partition = newPartition;
    return (C) this;
  }

  @Override
  public C keyMetadata(EncryptionKeyMetadata metadata) {
    this.keyMetadata = metadata;
    return (C) this;
  }

  @Override
  public C sortOrder(SortOrder newSortOrder) {
    this.sortOrder = newSortOrder;
    return (C) this;
  }

  @Override
  public <D> DataWriter<D> dataWriter() throws IOException {
    Preconditions.checkArgument(spec != null, "Cannot create data writer without spec");
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null when creating data writer for partitioned spec");

    return new DataWriter<>(
        writeBuilder.build(), format, location, spec, partition, keyMetadata, sortOrder);
  }

  @Override
  public <D> EqualityDeleteWriter<D> equalityDeleteWriter() throws IOException {
    Preconditions.checkState(
        rowSchema != null, "Cannot create equality delete file without a schema");
    Preconditions.checkState(
        equalityFieldIds != null, "Cannot create equality delete file without delete field ids");
    Preconditions.checkArgument(
        spec != null, "Spec must not be null when creating equality delete writer");
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null for partitioned writes");

    return new EqualityDeleteWriter<>(
        writeBuilder
            .schema(rowSchema)
            .meta("delete-type", "equality")
            .meta(
                "delete-field-ids",
                IntStream.of(equalityFieldIds)
                    .mapToObj(Objects::toString)
                    .collect(Collectors.joining(", ")))
            .build(),
        format,
        location,
        spec,
        partition,
        keyMetadata,
        sortOrder,
        equalityFieldIds);
  }

  @Override
  public <D> PositionDeleteWriter<D> positionDeleteWriter() throws IOException {
    Preconditions.checkState(
        equalityFieldIds == null, "Cannot create position delete file using delete field ids");
    Preconditions.checkArgument(
        spec != null, "Spec must not be null when creating position delete writer");
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null for partitioned writes");

    return new PositionDeleteWriter<>(
        writeBuilder
            .meta("delete-type", "position")
            .schema(DeleteSchemaUtil.posDeleteSchema(rowSchema))
            .build(),
        format,
        location,
        spec,
        partition,
        keyMetadata);
  }
}
