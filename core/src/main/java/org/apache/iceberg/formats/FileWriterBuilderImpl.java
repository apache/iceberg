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
package org.apache.iceberg.formats;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class FileWriterBuilderImpl<W extends FileWriter<D, ?>, D, S> implements FileWriterBuilder<W, S> {
  private final ModelWriteBuilder<D, S> modelWriteBuilder;
  private final String location;
  private final FileFormat format;
  private final BuilderFunction<W, D, S> builderMethod;
  private Schema schema = null;
  private PartitionSpec spec = null;
  private StructLike partition = null;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder = null;
  private int[] equalityFieldIds = null;

  static <D, S> FileWriterBuilder<DataWriter<D>, S> forDataFile(
      ModelWriteBuilder<D, S> modelWriteBuilder, String location, FileFormat format) {
    return new FileWriterBuilderImpl<>(
        modelWriteBuilder.content(FileContent.DATA),
        location,
        format,
        builder -> {
          Preconditions.checkState(builder.schema != null, "Invalid schema for data writer: null");
          Preconditions.checkArgument(
              builder.spec != null, "Invalid partition spec for data writer: null");
          Preconditions.checkArgument(
              builder.spec.isUnpartitioned() || builder.partition != null,
              "Invalid partition, does not match spec: %s",
              builder.spec);

          return new DataWriter<>(
              builder.modelWriteBuilder.build(),
              builder.format,
              builder.location,
              builder.spec,
              builder.partition,
              builder.keyMetadata,
              builder.sortOrder);
        });
  }

  static <D, S> FileWriterBuilder<EqualityDeleteWriter<D>, S> forEqualityDelete(
      ModelWriteBuilder<D, S> modelWriteBuilder, String location, FileFormat format) {
    return new FileWriterBuilderImpl<>(
        modelWriteBuilder.content(FileContent.EQUALITY_DELETES),
        location,
        format,
        builder -> {
          Preconditions.checkState(
              builder.schema != null, "Invalid schema for equality delete writer: null");
          Preconditions.checkState(
              builder.equalityFieldIds != null,
              "Invalid delete field ids for equality delete writer: null");
          Preconditions.checkArgument(
              builder.spec != null, "Invalid partition spec for equality delete writer: null");
          Preconditions.checkArgument(
              builder.spec.isUnpartitioned() || builder.partition != null,
              "Invalid partition, does not match spec: %s",
              builder.spec);

          return new EqualityDeleteWriter<>(
              builder
                  .modelWriteBuilder
                  .schema(builder.schema)
                  .meta("delete-type", "equality")
                  .meta(
                      "delete-field-ids",
                      IntStream.of(builder.equalityFieldIds)
                          .mapToObj(Objects::toString)
                          .collect(Collectors.joining(", ")))
                  .build(),
              builder.format,
              builder.location,
              builder.spec,
              builder.partition,
              builder.keyMetadata,
              builder.sortOrder,
              builder.equalityFieldIds);
        });
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static <D, S> FileWriterBuilder<PositionDeleteWriter<D>, S> forPositionDelete(
      ModelWriteBuilder<PositionDelete<D>, S> modelWriteBuilder,
      String location,
      FileFormat format) {
    return new FileWriterBuilderImpl<>(
        modelWriteBuilder.content(FileContent.POSITION_DELETES),
        location,
        format,
        builder -> {
          Preconditions.checkArgument(
              builder.spec != null, "Invalid partition spec for position delete writer: null");
          Preconditions.checkArgument(
              builder.spec.isUnpartitioned() || builder.partition != null,
              "Invalid partition, does not match spec: %s");

          return new PositionDeleteWriter<>(
              new PositionDeleteFileAppender<>(
                  builder.modelWriteBuilder.meta("delete-type", "position").build()),
              builder.format,
              builder.location,
              builder.spec,
              builder.partition,
              builder.keyMetadata);
        });
  }

  @FunctionalInterface
  interface BuilderFunction<B extends FileWriter<D, ?>, D, S> {
    B apply(FileWriterBuilderImpl<B, D, S> builder) throws IOException;
  }

  FileWriterBuilderImpl(
      ModelWriteBuilder<D, S> modelWriteBuilder,
      String location,
      FileFormat format,
      BuilderFunction<W, D, S> builderMethod) {
    this.modelWriteBuilder = modelWriteBuilder;
    this.location = location;
    this.format = format;
    this.builderMethod = builderMethod;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> set(String property, String value) {
    modelWriteBuilder.set(property, value);
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> meta(String property, String value) {
    modelWriteBuilder.meta(property, value);
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> metricsConfig(MetricsConfig metricsConfig) {
    modelWriteBuilder.metricsConfig(metricsConfig);
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> overwrite() {
    modelWriteBuilder.overwrite();
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> withFileEncryptionKey(ByteBuffer encryptionKey) {
    modelWriteBuilder.withFileEncryptionKey(encryptionKey);
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> withAADPrefix(ByteBuffer aadPrefix) {
    modelWriteBuilder.withAADPrefix(aadPrefix);
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> schema(Schema newSchema) {
    modelWriteBuilder.schema(newSchema);
    this.schema = newSchema;
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> engineSchema(S newSchema) {
    modelWriteBuilder.engineSchema(newSchema);
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> spec(PartitionSpec newSpec) {
    this.spec = newSpec;
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> partition(StructLike newPartition) {
    this.partition = newPartition;
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> keyMetadata(EncryptionKeyMetadata newKeyMetadata) {
    this.keyMetadata = newKeyMetadata;
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> sortOrder(SortOrder newSortOrder) {
    this.sortOrder = newSortOrder;
    return this;
  }

  @Override
  public FileWriterBuilderImpl<W, D, S> equalityFieldIds(int... fieldIds) {
    this.equalityFieldIds = fieldIds;
    return this;
  }

  @Override
  public W build() throws IOException {
    return builderMethod.apply(this);
  }

  private static class PositionDeleteFileAppender<T> implements FileAppender<StructLike> {
    private final FileAppender<PositionDelete<T>> appender;

    PositionDeleteFileAppender(FileAppender<PositionDelete<T>> appender) {
      this.appender = appender;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(StructLike positionDelete) {
      appender.add((PositionDelete<T>) positionDelete);
    }

    @Override
    public Metrics metrics() {
      return appender.metrics();
    }

    @Override
    public long length() {
      return appender.length();
    }

    @Override
    public void close() throws IOException {
      appender.close();
    }

    @Override
    public List<Long> splitOffsets() {
      return appender.splitOffsets();
    }
  }
}
