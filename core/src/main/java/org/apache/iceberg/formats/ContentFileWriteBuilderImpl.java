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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

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
 * @param <B> the concrete builder type for method chaining
 */
abstract class ContentFileWriteBuilderImpl<B extends ContentFileWriteBuilder<B>>
    implements ContentFileWriteBuilder<B> {
  private final WriteBuilder writeBuilder;
  private final String location;
  private final FileFormat format;
  private PartitionSpec spec = null;
  private StructLike partition = null;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder = null;

  static <D, S> DataWriteBuilder<D, S> forDataFile(
      WriteBuilder writeBuilder, String location, FileFormat format) {
    return new DataFileWriteBuilder<>(writeBuilder, location, format);
  }

  static <D, S> EqualityDeleteWriteBuilder<D, S> forEqualityDelete(
      WriteBuilder writeBuilder, String location, FileFormat format) {
    return new EqualityDeleteFileWriteBuilder<>(writeBuilder, location, format);
  }

  static PositionDeleteWriteBuilder forPositionDelete(
      WriteBuilder writeBuilder, String location, FileFormat format) {
    return new PositionDeleteFileWriteBuilder(writeBuilder, location, format);
  }

  private ContentFileWriteBuilderImpl(
      WriteBuilder writeBuilder, String location, FileFormat format) {
    this.writeBuilder = writeBuilder;
    this.location = location;
    this.format = format;
  }

  @Override
  public B set(String property, String value) {
    writeBuilder.set(property, value);
    return self();
  }

  @Override
  public B meta(String property, String value) {
    writeBuilder.meta(property, value);
    return self();
  }

  @Override
  public B metricsConfig(MetricsConfig metricsConfig) {
    writeBuilder.metricsConfig(metricsConfig);
    return self();
  }

  @Override
  public B overwrite() {
    writeBuilder.overwrite();
    return self();
  }

  @Override
  public B withFileEncryptionKey(ByteBuffer encryptionKey) {
    writeBuilder.withFileEncryptionKey(encryptionKey);
    return self();
  }

  @Override
  public B withAADPrefix(ByteBuffer aadPrefix) {
    writeBuilder.withAADPrefix(aadPrefix);
    return self();
  }

  @Override
  public B spec(PartitionSpec newSpec) {
    this.spec = newSpec;
    return self();
  }

  @Override
  public B partition(StructLike newPartition) {
    this.partition = newPartition;
    return self();
  }

  @Override
  public B keyMetadata(EncryptionKeyMetadata newKeyMetadata) {
    this.keyMetadata = newKeyMetadata;
    return self();
  }

  @Override
  public B sortOrder(SortOrder newSortOrder) {
    this.sortOrder = newSortOrder;
    return self();
  }

  private static class DataFileWriteBuilder<D, S>
      extends ContentFileWriteBuilderImpl<DataWriteBuilder<D, S>>
      implements DataWriteBuilder<D, S> {
    private DataFileWriteBuilder(WriteBuilder writeBuilder, String location, FileFormat format) {
      super(writeBuilder, location, format);
    }

    @Override
    public DataFileWriteBuilder<D, S> schema(Schema schema) {
      super.writeBuilder.schema(schema);
      return this;
    }

    @Override
    public DataFileWriteBuilder<D, S> inputSchema(S schema) {
      super.writeBuilder.inputSchema(schema);
      return this;
    }

    @Override
    public DataFileWriteBuilder<D, S> self() {
      return this;
    }

    @Override
    public DataWriter<D> build() throws IOException {
      Preconditions.checkArgument(super.spec != null, "Cannot create data writer without spec");
      Preconditions.checkArgument(
          super.spec.isUnpartitioned() || super.partition != null,
          "Partition must not be null when creating data writer for partitioned spec");

      return new DataWriter<>(
          super.writeBuilder.build(),
          super.format,
          super.location,
          super.spec,
          super.partition,
          super.keyMetadata,
          super.sortOrder);
    }
  }

  private static class EqualityDeleteFileWriteBuilder<D, S>
      extends ContentFileWriteBuilderImpl<EqualityDeleteWriteBuilder<D, S>>
      implements EqualityDeleteWriteBuilder<D, S> {
    private Schema rowSchema = null;
    private int[] equalityFieldIds = null;

    private EqualityDeleteFileWriteBuilder(
        WriteBuilder writeBuilder, String location, FileFormat format) {
      super(writeBuilder, location, format);
    }

    @Override
    public EqualityDeleteFileWriteBuilder<D, S> inputSchema(S schema) {
      super.writeBuilder.inputSchema(schema);
      return this;
    }

    @Override
    public EqualityDeleteFileWriteBuilder<D, S> self() {
      return this;
    }

    @Override
    public EqualityDeleteFileWriteBuilder<D, S> rowSchema(Schema schema) {
      this.rowSchema = schema;
      return this;
    }

    @Override
    public EqualityDeleteFileWriteBuilder<D, S> equalityFieldIds(int... fieldIds) {
      this.equalityFieldIds = fieldIds;
      return this;
    }

    @Override
    public EqualityDeleteWriter<D> build() throws IOException {
      Preconditions.checkState(
          rowSchema != null, "Cannot create equality delete file without a schema");
      Preconditions.checkState(
          equalityFieldIds != null, "Cannot create equality delete file without delete field ids");
      Preconditions.checkArgument(
          super.spec != null, "Spec must not be null when creating equality delete writer");
      Preconditions.checkArgument(
          super.spec.isUnpartitioned() || super.partition != null,
          "Partition must not be null for partitioned writes");

      return new EqualityDeleteWriter<>(
          super.writeBuilder
              .schema(rowSchema)
              .meta("delete-type", "equality")
              .meta(
                  "delete-field-ids",
                  IntStream.of(equalityFieldIds)
                      .mapToObj(Objects::toString)
                      .collect(Collectors.joining(", ")))
              .build(),
          super.format,
          super.location,
          super.spec,
          super.partition,
          super.keyMetadata,
          super.sortOrder,
          equalityFieldIds);
    }
  }

  private static class PositionDeleteFileWriteBuilder
      extends ContentFileWriteBuilderImpl<PositionDeleteWriteBuilder>
      implements PositionDeleteWriteBuilder {

    private PositionDeleteFileWriteBuilder(
        WriteBuilder writeBuilder, String location, FileFormat format) {
      super(writeBuilder, location, format);
    }

    @Override
    public PositionDeleteFileWriteBuilder self() {
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PositionDeleteWriter<?> build() throws IOException {
      Preconditions.checkArgument(
          super.spec != null, "Spec must not be null when creating position delete writer");
      Preconditions.checkArgument(
          super.spec.isUnpartitioned() || super.partition != null,
          "Partition must not be null for partitioned writes");

      return new PositionDeleteWriter<>(
          new PositionDeleteFileAppender(
              super.writeBuilder.meta("delete-type", "position").build()),
          super.format,
          super.location,
          super.spec,
          super.partition,
          super.keyMetadata);
    }
  }

  private static class PositionDeleteFileAppender implements FileAppender<StructLike> {
    private final FileAppender<PositionDelete<?>> appender;

    PositionDeleteFileAppender(FileAppender<PositionDelete<?>> appender) {
      this.appender = appender;
    }

    @Override
    public void add(StructLike positionDelete) {
      appender.add((PositionDelete<?>) positionDelete);
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
