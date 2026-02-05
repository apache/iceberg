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
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
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
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

abstract class FileWriterBuilderImpl<W extends FileWriter<D, ?>, D, S>
    implements FileWriterBuilder<W, S> {
  private final ModelWriteBuilder<D, S> modelWriteBuilder;
  private final String location;
  private final FileFormat format;
  private Schema schema = null;
  private PartitionSpec spec = null;
  private StructLike partition = null;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder = null;

  /** Creates a builder for {@link DataWriter} instances for writing data files. */
  static <D, S> FileWriterBuilder<DataWriter<D>, S> forDataFile(
      FormatModel<D, S> model, EncryptedOutputFile outputFile) {
    return new DataFileWriterBuilder<>(model, outputFile);
  }

  /**
   * Creates a builder for {@link EqualityDeleteWriter} instances for writing equality delete files.
   */
  static <D, S> FileWriterBuilder<EqualityDeleteWriter<D>, S> forEqualityDelete(
      FormatModel<D, S> model, EncryptedOutputFile outputFile) {
    return new EqualityDeleteWriterBuilder<>(model, outputFile);
  }

  /**
   * Creates a builder for {@link PositionDeleteWriter} instances for writing position delete files.
   */
  static <D, S> FileWriterBuilder<PositionDeleteWriter<D>, S> forPositionDelete(
      FormatModel<PositionDelete<D>, S> model, EncryptedOutputFile outputFile) {
    return new PositionDeleteWriterBuilder<>(model, outputFile);
  }

  private FileWriterBuilderImpl(
      FormatModel<D, S> model, EncryptedOutputFile outputFile, FileContent content) {
    this.modelWriteBuilder = model.writeBuilder(outputFile).content(content);
    this.location = outputFile.encryptingOutputFile().location();
    this.format = model.format();
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
    throw new UnsupportedOperationException(
        "Equality field ids not supported for this writer type");
  }

  ModelWriteBuilder<D, S> modelWriteBuilder() {
    return modelWriteBuilder;
  }

  String location() {
    return location;
  }

  FileFormat format() {
    return format;
  }

  Schema schema() {
    return schema;
  }

  PartitionSpec spec() {
    return spec;
  }

  StructLike partition() {
    return partition;
  }

  EncryptionKeyMetadata keyMetadata() {
    return keyMetadata;
  }

  SortOrder sortOrder() {
    return sortOrder;
  }

  /** Builder for creating {@link DataWriter} instances for writing data files. */
  private static class DataFileWriterBuilder<D, S>
      extends FileWriterBuilderImpl<DataWriter<D>, D, S> {

    private DataFileWriterBuilder(FormatModel<D, S> model, EncryptedOutputFile outputFile) {
      super(model, outputFile, FileContent.DATA);
    }

    @Override
    public DataWriter<D> build() throws IOException {
      Preconditions.checkState(schema() != null, "Invalid schema for data writer: null");
      Preconditions.checkArgument(spec() != null, "Invalid partition spec for data writer: null");
      Preconditions.checkArgument(
          spec().isUnpartitioned() || partition() != null,
          "Invalid partition, does not match spec: %s",
          spec());

      return new DataWriter<>(
          modelWriteBuilder().build(),
          format(),
          location(),
          spec(),
          partition(),
          keyMetadata(),
          sortOrder());
    }
  }

  /**
   * Builder for creating {@link EqualityDeleteWriter} instances for writing equality delete files.
   */
  private static class EqualityDeleteWriterBuilder<D, S>
      extends FileWriterBuilderImpl<EqualityDeleteWriter<D>, D, S> {

    private int[] equalityFieldIds = null;

    private EqualityDeleteWriterBuilder(FormatModel<D, S> model, EncryptedOutputFile outputFile) {
      super(model, outputFile, FileContent.EQUALITY_DELETES);
    }

    @Override
    public EqualityDeleteWriterBuilder<D, S> equalityFieldIds(int... fieldIds) {
      this.equalityFieldIds = fieldIds;
      return this;
    }

    @Override
    public EqualityDeleteWriter<D> build() throws IOException {
      Preconditions.checkState(schema() != null, "Invalid schema for equality delete writer: null");
      Preconditions.checkState(
          equalityFieldIds != null, "Invalid delete field ids for equality delete writer: null");
      Preconditions.checkArgument(
          spec() != null, "Invalid partition spec for equality delete writer: null");
      Preconditions.checkArgument(
          spec().isUnpartitioned() || partition() != null,
          "Invalid partition, does not match spec: %s",
          spec());

      return new EqualityDeleteWriter<>(
          modelWriteBuilder()
              .schema(schema())
              .meta("delete-type", "equality")
              .meta(
                  "delete-field-ids",
                  IntStream.of(equalityFieldIds)
                      .mapToObj(Objects::toString)
                      .collect(Collectors.joining(", ")))
              .build(),
          format(),
          location(),
          spec(),
          partition(),
          keyMetadata(),
          sortOrder(),
          equalityFieldIds);
    }
  }

  /**
   * Builder for creating {@link PositionDeleteWriter} instances for writing position delete files.
   */
  private static class PositionDeleteWriterBuilder<D, S>
      extends FileWriterBuilderImpl<PositionDeleteWriter<D>, PositionDelete<D>, S> {

    private PositionDeleteWriterBuilder(
        FormatModel<PositionDelete<D>, S> model, EncryptedOutputFile outputFile) {
      super(model, outputFile, FileContent.POSITION_DELETES);
    }

    @Override
    public PositionDeleteWriter<D> build() throws IOException {
      Preconditions.checkArgument(
          spec() != null, "Invalid partition spec for position delete writer: null");
      Preconditions.checkArgument(
          spec().isUnpartitioned() || partition() != null,
          "Invalid partition, does not match spec: %s",
          spec());

      return new PositionDeleteWriter<>(
          modelWriteBuilder().meta("delete-type", "position").build(),
          format(),
          location(),
          spec(),
          partition(),
          keyMetadata());
    }
  }
}
