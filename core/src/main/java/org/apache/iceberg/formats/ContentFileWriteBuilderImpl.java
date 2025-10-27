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
import java.util.Map;
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
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

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
 * @param <S> the type of the schema for the input data
 * @param <D> the type of data records the writer will accept
 */
abstract class ContentFileWriteBuilderImpl<B extends ContentFileWriteBuilder<B>, D, S>
    implements ContentFileWriteBuilder<B> {
  private final FormatModel<D, S> formatModel;
  private final EncryptedOutputFile outputFile;
  private final FileFormat format;
  private PartitionSpec spec = null;
  private StructLike partition = null;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder = null;
  private Map<String, String> properties = Maps.newHashMap();
  private Map<String, String> metadata = Maps.newHashMap();
  private MetricsConfig metricsConfig = null;
  private boolean overwrite = false;
  private ByteBuffer encryptionKey = null;
  private ByteBuffer aadPrefix = null;

  static <D, S> DataWriteBuilder<D, S> forDataFile(
      FormatModel<D, S> formatModel, EncryptedOutputFile outputFile, FileFormat format) {
    return new DataFileWriteBuilder<>(formatModel, outputFile, format);
  }

  static <D, S> EqualityDeleteWriteBuilder<D, S> forEqualityDelete(
      FormatModel<D, S> formatModel, EncryptedOutputFile outputFile, FileFormat format) {
    return new EqualityDeleteFileWriteBuilder<>(formatModel, outputFile, format);
  }

  static PositionDeleteWriteBuilder forPositionDelete(
      FormatModel<PositionDelete<?>, Object> formatModel,
      EncryptedOutputFile outputFile,
      FileFormat format) {
    return new PositionDeleteFileWriteBuilder(formatModel, outputFile, format);
  }

  private ContentFileWriteBuilderImpl(
      FormatModel<D, S> formatModel, EncryptedOutputFile outputFile, FileFormat format) {
    this.formatModel = formatModel;
    this.outputFile = outputFile;
    this.format = format;
  }

  @Override
  public B set(String property, String value) {
    properties.put(property, value);
    return self();
  }

  @Override
  public B meta(String property, String value) {
    metadata.put(property, value);
    return self();
  }

  @Override
  public B metricsConfig(MetricsConfig newMetricsConfig) {
    this.metricsConfig = newMetricsConfig;
    return self();
  }

  @Override
  public B overwrite() {
    this.overwrite = true;
    return self();
  }

  @Override
  public B withFileEncryptionKey(ByteBuffer newEncryptionKey) {
    this.encryptionKey = newEncryptionKey;
    return self();
  }

  @Override
  public B withAADPrefix(ByteBuffer newAadPrefix) {
    this.aadPrefix = newAadPrefix;
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

  private void init(WriteBuilder writeBuilder) {
    if (metricsConfig != null) {
      writeBuilder.metricsConfig(metricsConfig);
    }
    if (overwrite) {
      writeBuilder.overwrite();
    }
    if (encryptionKey != null) {
      writeBuilder.withFileEncryptionKey(encryptionKey);
    }
    if (aadPrefix != null) {
      writeBuilder.withAADPrefix(aadPrefix);
    }

    writeBuilder.setAll(properties).meta(metadata);
  }

  private static class DataFileWriteBuilder<D, S>
      extends ContentFileWriteBuilderImpl<DataWriteBuilder<D, S>, D, S>
      implements DataWriteBuilder<D, S> {
    private Schema schema = null;
    private S inputSchema = null;

    private DataFileWriteBuilder(
        FormatModel<D, S> formatModel, EncryptedOutputFile outputFile, FileFormat format) {
      super(formatModel, outputFile, format);
    }

    @Override
    public DataFileWriteBuilder<D, S> schema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    @Override
    public DataFileWriteBuilder<D, S> inputSchema(S newInputSchema) {
      this.inputSchema = newInputSchema;
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

      WriteBuilder writeBuilder =
          super.formatModel
              .writeBuilder(super.outputFile.encryptingOutputFile(), schema, inputSchema)
              .schema(schema)
              .content(FileContent.DATA);
      super.init(writeBuilder);
      return new DataWriter<>(
          writeBuilder.build(),
          super.format,
          super.outputFile.encryptingOutputFile().location(),
          super.spec,
          super.partition,
          super.keyMetadata,
          super.sortOrder);
    }
  }

  private static class EqualityDeleteFileWriteBuilder<D, S>
      extends ContentFileWriteBuilderImpl<EqualityDeleteWriteBuilder<D, S>, D, S>
      implements EqualityDeleteWriteBuilder<D, S> {
    private S inputSchema = null;
    private Schema rowSchema = null;
    private int[] equalityFieldIds = null;

    private EqualityDeleteFileWriteBuilder(
        FormatModel<D, S> formatModel, EncryptedOutputFile outputFile, FileFormat format) {
      super(formatModel, outputFile, format);
    }

    @Override
    public EqualityDeleteFileWriteBuilder<D, S> inputSchema(S schema) {
      this.inputSchema = schema;
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

      WriteBuilder writeBuilder =
          super.formatModel
              .writeBuilder(super.outputFile.encryptingOutputFile(), rowSchema, inputSchema)
              .schema(rowSchema)
              .content(FileContent.EQUALITY_DELETES);
      super.init(writeBuilder);

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
          super.format,
          super.outputFile.encryptingOutputFile().location(),
          super.spec,
          super.partition,
          super.keyMetadata,
          super.sortOrder,
          equalityFieldIds);
    }
  }

  private static class PositionDeleteFileWriteBuilder
      extends ContentFileWriteBuilderImpl<PositionDeleteWriteBuilder, PositionDelete<?>, Object>
      implements PositionDeleteWriteBuilder {

    private PositionDeleteFileWriteBuilder(
        FormatModel<PositionDelete<?>, Object> formatModel,
        EncryptedOutputFile outputFile,
        FileFormat format) {
      super(formatModel, outputFile, format);
    }

    @Override
    public PositionDeleteFileWriteBuilder self() {
      return this;
    }

    @Override
    public PositionDeleteWriter<?> build() throws IOException {
      Preconditions.checkArgument(
          super.spec != null, "Spec must not be null when creating position delete writer");
      Preconditions.checkArgument(
          super.spec.isUnpartitioned() || super.partition != null,
          "Partition must not be null for partitioned writes");

      WriteBuilder writeBuilder =
          super.formatModel
              .writeBuilder(super.outputFile.encryptingOutputFile(), null, null)
              .content(FileContent.POSITION_DELETES);
      super.init(writeBuilder);

      return new PositionDeleteWriter<>(
          new PositionDeleteFileAppender(writeBuilder.meta("delete-type", "position").build()),
          super.format,
          super.outputFile.encryptingOutputFile().location(),
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
