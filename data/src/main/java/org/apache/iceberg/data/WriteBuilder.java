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
import org.apache.iceberg.io.AppenderBuilder;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ArrayUtil;

/**
 * Implementation for the different Write Builder interfaces. The builder is an internal class and
 * could change without notice. Use one of the following specific interfaces instead:
 *
 * <ul>
 *   <li>{@link DataWriteBuilder}
 *   <li>{@link EqualityDeleteWriteBuilder}
 *   <li>{@link PositionDeleteWriteBuilder}
 * </ul>
 *
 * The builder wraps the file format specific {@link AppenderBuilder}. To allow further engine and
 * file format specific configuration changes for the given writer, the {@link
 * AppenderBuilder#build()} method is called to create the appender which is used by the {@link
 * WriteBuilder} to provide the required functionality.
 *
 * @param <A> type of the appender
 * @param <E> engine specific schema of the input records used for appender initialization
 */
@SuppressWarnings("unchecked")
class WriteBuilder<B extends WriteBuilder<B, A, E>, A extends AppenderBuilder<A, E>, E>
    implements DataWriteBuilder<B, E>,
        EqualityDeleteWriteBuilder<B, E>,
        PositionDeleteWriteBuilder<B, E> {
  private final AppenderBuilder<A, E> appenderBuilder;
  private final String location;
  private final FileFormat format;
  private PartitionSpec spec = null;
  private StructLike partition = null;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder = null;
  private Schema rowSchema = null;
  private int[] equalityFieldIds = null;

  WriteBuilder(AppenderBuilder<A, E> appenderBuilder, String location, FileFormat format) {
    this.appenderBuilder = appenderBuilder;
    this.location = location;
    this.format = format;
  }

  @Override
  public B schema(Schema newSchema) {
    appenderBuilder.schema(newSchema);
    return (B) this;
  }

  @Override
  public B engineSchema(E engineSchema) {
    appenderBuilder.engineSchema(engineSchema);
    return (B) this;
  }

  @Override
  public B set(String property, String value) {
    appenderBuilder.set(property, value);
    return (B) this;
  }

  @Override
  public B set(Map<String, String> properties) {
    properties.forEach(appenderBuilder::set);
    return (B) this;
  }

  @Override
  public B meta(String property, String value) {
    appenderBuilder.meta(property, value);
    return (B) this;
  }

  @Override
  public B meta(Map<String, String> properties) {
    properties.forEach(appenderBuilder::meta);
    return (B) this;
  }

  @Override
  public B metricsConfig(MetricsConfig newMetricsConfig) {
    appenderBuilder.metricsConfig(newMetricsConfig);
    return (B) this;
  }

  @Override
  public B overwrite() {
    appenderBuilder.overwrite();
    return (B) this;
  }

  @Override
  public B fileEncryptionKey(ByteBuffer encryptionKey) {
    appenderBuilder.fileEncryptionKey(encryptionKey);
    return (B) this;
  }

  @Override
  public B aadPrefix(ByteBuffer aadPrefix) {
    appenderBuilder.aadPrefix(aadPrefix);
    return (B) this;
  }

  @Override
  public B withRowSchema(Schema newSchema) {
    this.rowSchema = newSchema;
    return (B) this;
  }

  @Override
  public B withEqualityFieldIds(List<Integer> fieldIds) {
    this.equalityFieldIds = ArrayUtil.toIntArray(fieldIds);
    return (B) this;
  }

  @Override
  public B withEqualityFieldIds(int... fieldIds) {
    this.equalityFieldIds = fieldIds;
    return (B) this;
  }

  @Override
  public B withSpec(PartitionSpec newSpec) {
    this.spec = newSpec;
    return (B) this;
  }

  @Override
  public B withPartition(StructLike newPartition) {
    this.partition = newPartition;
    return (B) this;
  }

  @Override
  public B withKeyMetadata(EncryptionKeyMetadata metadata) {
    this.keyMetadata = metadata;
    return (B) this;
  }

  @Override
  public B withSortOrder(SortOrder newSortOrder) {
    this.sortOrder = newSortOrder;
    return (B) this;
  }

  @Override
  public <D> DataWriter<D> dataWriter() throws IOException {
    Preconditions.checkArgument(spec != null, "Cannot create data writer without spec");
    Preconditions.checkArgument(
        spec.isUnpartitioned() || partition != null,
        "Partition must not be null when creating data writer for partitioned spec");

    return new DataWriter<>(
        appenderBuilder.build(), format, location, spec, partition, keyMetadata, sortOrder);
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
        appenderBuilder
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
        appenderBuilder
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
