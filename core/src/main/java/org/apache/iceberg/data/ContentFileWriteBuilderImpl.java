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
import java.util.Objects;
import java.util.function.Function;
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
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.WriteBuilder;
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
 * @param <D> the type of data records the writer will accept
 */
@SuppressWarnings("unchecked")
abstract class ContentFileWriteBuilderImpl<B extends ContentFileWriteBuilder<B, S>, D, S>
    implements ContentFileWriteBuilder<B, S> {
  private final WriteBuilder<D, S> writeBuilder;
  private final String location;
  private final FileFormat format;
  private PartitionSpec spec = null;
  private StructLike partition = null;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder = null;

  static <D, S> DataWriteBuilder<D, S> forDataFile(
      WriteBuilder<D, S> writeBuilder, String location, FileFormat format) {
    return new DataFileWriteBuilder<>(writeBuilder, location, format);
  }

  static <D, S> EqualityDeleteWriteBuilder<D, S> forEqualityDelete(
      WriteBuilder<D, S> writeBuilder, String location, FileFormat format) {
    return new EqualityDeleteFileWriteBuilder<>(writeBuilder, location, format);
  }

  static <D, S> PositionDeleteWriteBuilder<D, S> forPositionDelete(
      WriteBuilder<D, S> writeBuilder,
      String location,
      FileFormat format,
      Function<Schema, Function<PositionDelete<D>, D>> positionDeleteConverter) {
    return new PositionDeleteFileWriteBuilder<>(
        writeBuilder, location, format, positionDeleteConverter);
  }

  private ContentFileWriteBuilderImpl(
      WriteBuilder<D, S> writeBuilder, String location, FileFormat format) {
    this.writeBuilder = writeBuilder;
    this.location = location;
    this.format = format;
  }

  @Override
  public B schema(Schema schema) {
    writeBuilder.schema(schema);
    return self();
  }

  @Override
  public B inputSchema(S schema) {
    writeBuilder.inputSchema(schema);
    return self();
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
  public B fileEncryptionKey(ByteBuffer encryptionKey) {
    writeBuilder.fileEncryptionKey(encryptionKey);
    return self();
  }

  @Override
  public B fileAADPrefix(ByteBuffer aadPrefix) {
    writeBuilder.fileAADPrefix(aadPrefix);
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
      extends ContentFileWriteBuilderImpl<DataWriteBuilder<D, S>, D, S>
      implements DataWriteBuilder<D, S> {
    private DataFileWriteBuilder(
        WriteBuilder<D, S> writeBuilder, String location, FileFormat format) {
      super(writeBuilder, location, format);
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
      extends ContentFileWriteBuilderImpl<EqualityDeleteWriteBuilder<D, S>, D, S>
      implements EqualityDeleteWriteBuilder<D, S> {
    private Schema rowSchema = null;
    private int[] equalityFieldIds = null;

    private EqualityDeleteFileWriteBuilder(
        WriteBuilder<D, S> writeBuilder, String location, FileFormat format) {
      super(writeBuilder, location, format);
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

  private static class PositionDeleteFileWriteBuilder<D, S>
      extends ContentFileWriteBuilderImpl<PositionDeleteWriteBuilder<D, S>, D, S>
      implements PositionDeleteWriteBuilder<D, S> {
    private final Function<Schema, Function<PositionDelete<D>, D>> positionDeleteConverter;
    private Schema rowSchema = null;

    private PositionDeleteFileWriteBuilder(
        WriteBuilder<D, S> writeBuilder,
        String location,
        FileFormat format,
        Function<Schema, Function<PositionDelete<D>, D>> positionDeleteConverter) {
      super(writeBuilder, location, format);
      this.positionDeleteConverter = positionDeleteConverter;
    }

    @Override
    public PositionDeleteFileWriteBuilder<D, S> self() {
      return this;
    }

    @Override
    public PositionDeleteFileWriteBuilder<D, S> rowSchema(Schema schema) {
      this.rowSchema = schema;
      return this;
    }

    @Override
    public PositionDeleteWriter<D> build() throws IOException {
      Preconditions.checkArgument(
          positionDeleteConverter != null,
          "Positional delete converter must not be null when creating position delete writer");
      Preconditions.checkArgument(
          super.spec != null, "Spec must not be null when creating position delete writer");
      Preconditions.checkArgument(
          super.spec.isUnpartitioned() || super.partition != null,
          "Partition must not be null for partitioned writes");

      return new PositionDeleteWriter<D>(
          (FileAppender)
              new ConvertingFileAppender<>(
                  super.writeBuilder
                      .meta("delete-type", "position")
                      .schema(DeleteSchemaUtil.posDeleteSchema(rowSchema))
                      .build(),
                  positionDeleteConverter.apply(rowSchema),
                  rowSchema),
          super.format,
          super.location,
          super.spec,
          super.partition,
          super.keyMetadata);
    }
  }

  private static class ConvertingFileAppender<D> implements FileAppender<PositionDelete<D>> {
    private final FileAppender<D> appender;
    private final Function<PositionDelete<D>, D> converter;
    private final Schema rowSchema;

    ConvertingFileAppender(
        FileAppender<D> appender, Function<PositionDelete<D>, D> converter, Schema rowSchema) {
      this.appender = appender;
      this.converter = converter;
      this.rowSchema = rowSchema;
    }

    @Override
    public void add(PositionDelete<D> positionDelete) {
      Preconditions.checkArgument(
          rowSchema == null || positionDelete.row() != null,
          "The row in PositionDelete must not be null because it was set row schema in position delete.");
      appender.add(converter.apply(positionDelete));
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
