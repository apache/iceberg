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
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
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
 * @param <E> output schema type required by the writer for data conversion
 * @param <D> the type of data records the writer will accept
 */
@SuppressWarnings("unchecked")
class ContentFileWriteBuilderImpl<
        C extends ContentFileWriteBuilder<C, E>, W extends WriteBuilder<W, E, D>, E, D>
    implements ContentFileWriteBuilder<C, E> {
  private final WriteBuilder<W, E, D> writeBuilder;
  private final String location;
  private final FileFormat format;
  private PartitionSpec spec = null;
  private StructLike partition = null;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder = null;

  static <C extends DataWriteBuilder<C, E, D>, W extends WriteBuilder<W, E, D>, E, D>
      DataWriteBuilder<C, E, D> forDataFile(
          WriteBuilder<W, E, D> writeBuilder, String location, FileFormat format) {
    return (DataWriteBuilder<C, E, D>) new DataFileWriteBuilder<>(writeBuilder, location, format);
  }

  static <C extends EqualityDeleteWriteBuilder<C, E, D>, W extends WriteBuilder<W, E, D>, E, D>
      EqualityDeleteWriteBuilder<C, E, D> forEqualityDelete(
          WriteBuilder<W, E, D> writeBuilder, String location, FileFormat format) {
    return (EqualityDeleteWriteBuilder<C, E, D>)
        new EqualityDeleteFileWriteBuilder<>(writeBuilder, location, format);
  }

  static <
          C extends PositionDeleteWriteBuilder<C, E, D>,
          W extends WriteBuilder<W, E, PositionDelete<D>>,
          E,
          D>
      PositionDeleteWriteBuilder<C, E, D> forPositionDelete(
          WriteBuilder<W, E, PositionDelete<D>> writeBuilder, String location, FileFormat format) {
    return (PositionDeleteWriteBuilder<C, E, D>)
        new PositionDeleteFileWriteBuilder<>(writeBuilder, location, format);
  }

  private ContentFileWriteBuilderImpl(
      org.apache.iceberg.io.WriteBuilder<W, E, D> writeBuilder,
      String location,
      FileFormat format) {
    this.writeBuilder = writeBuilder;
    this.location = location;
    this.format = format;
  }

  @Override
  public C fileSchema(Schema newSchema) {
    writeBuilder.fileSchema(newSchema);
    return (C) this;
  }

  @Override
  public C dataSchema(E dataSchema) {
    writeBuilder.dataSchema(dataSchema);
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
  public C fileAADPrefix(ByteBuffer aadPrefix) {
    writeBuilder.fileAADPrefix(aadPrefix);
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

  private static class DataFileWriteBuilder<
          C extends DataFileWriteBuilder<C, W, E, D>, W extends WriteBuilder<W, E, D>, E, D>
      extends ContentFileWriteBuilderImpl<C, W, E, D> implements DataWriteBuilder<C, E, D> {
    private DataFileWriteBuilder(
        WriteBuilder<W, E, D> writeBuilder, String location, FileFormat format) {
      super(writeBuilder, location, format);
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

  private static class EqualityDeleteFileWriteBuilder<
          C extends EqualityDeleteFileWriteBuilder<C, B, E, D>,
          B extends WriteBuilder<B, E, D>,
          E,
          D>
      extends ContentFileWriteBuilderImpl<C, B, E, D>
      implements EqualityDeleteWriteBuilder<C, E, D> {
    private Schema rowSchema = null;
    private int[] equalityFieldIds = null;

    private EqualityDeleteFileWriteBuilder(
        WriteBuilder<B, E, D> writeBuilder, String location, FileFormat format) {
      super(writeBuilder, location, format);
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
              .fileSchema(rowSchema)
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

  private static class PositionDeleteFileWriteBuilder<
          C extends PositionDeleteFileWriteBuilder<C, B, E, D>,
          B extends WriteBuilder<B, E, PositionDelete<D>>,
          E,
          D>
      extends ContentFileWriteBuilderImpl<C, B, E, PositionDelete<D>>
      implements PositionDeleteWriteBuilder<C, E, D> {
    private Schema rowSchema = null;

    private PositionDeleteFileWriteBuilder(
        WriteBuilder<B, E, PositionDelete<D>> writeBuilder, String location, FileFormat format) {
      super(writeBuilder, location, format);
    }

    @Override
    public C rowSchema(Schema newSchema) {
      this.rowSchema = newSchema;
      return (C) this;
    }

    @Override
    public PositionDeleteWriter<D> build() throws IOException {
      Preconditions.checkArgument(
          super.spec != null, "Spec must not be null when creating position delete writer");
      Preconditions.checkArgument(
          super.spec.isUnpartitioned() || super.partition != null,
          "Partition must not be null for partitioned writes");

      return new PositionDeleteWriter<D>(
          (FileAppender)
              super.writeBuilder
                  .meta("delete-type", "position")
                  .fileSchema(DeleteSchemaUtil.posDeleteSchema(rowSchema))
                  .build(),
          super.format,
          super.location,
          super.spec,
          super.partition,
          super.keyMetadata);
    }
  }
}
