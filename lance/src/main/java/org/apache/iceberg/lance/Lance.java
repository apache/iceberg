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
package org.apache.iceberg.lance;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Main entry point for Lance format support in Iceberg.
 *
 * <p>Provides static factory methods and Builders for creating Lance file readers and writers,
 * following the same patterns as {@code Parquet} and {@code ORC} entry classes.
 */
public class Lance {

  private Lance() {}

  /**
   * Create a new write builder for a Lance file.
   *
   * @param file the output file to write to
   * @return a new WriteBuilder
   */
  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  /**
   * Create a new read builder for a Lance file.
   *
   * @param file the input file to read from
   * @return a new ReadBuilder
   */
  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  /**
   * Create a new DataWriteBuilder for writing data files in Lance format.
   *
   * @param file the output file to write to
   * @return a new DataWriteBuilder
   */
  public static DataWriteBuilder writeData(OutputFile file) {
    return new DataWriteBuilder(file);
  }

  /** Builder for creating Lance file writers. */
  public static class WriteBuilder {
    private final OutputFile file;
    private Schema schema = null;
    private final Map<String, String> config = Maps.newLinkedHashMap();
    private final Map<String, String> metadata = Maps.newLinkedHashMap();

    private WriteBuilder(OutputFile file) {
      this.file = file;
    }

    public WriteBuilder forTable(Table table) {
      schema(table.schema());
      setAll(table.properties());
      return this;
    }

    public WriteBuilder schema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public WriteBuilder set(String property, String value) {
      config.put(property, value);
      return this;
    }

    public WriteBuilder setAll(Map<String, String> properties) {
      config.putAll(properties);
      return this;
    }

    public WriteBuilder meta(String property, String value) {
      metadata.put(property, value);
      return this;
    }

    public WriteBuilder overwrite() {
      return this;
    }

    @SuppressWarnings("unchecked")
    public <D> FileAppender<D> build() throws IOException {
      Preconditions.checkNotNull(schema, "Schema is required");
      return (FileAppender<D>) new LanceFileAppender<>(file, schema);
    }
  }

  /** Builder for creating Lance file readers. */
  @SuppressWarnings("UnusedVariable")
  public static class ReadBuilder {
    private final InputFile file;
    private Schema schema = null;
    private Schema projectedSchema = null;
    private Expression filter = null;
    private boolean caseSensitive = true;

    private ReadBuilder(InputFile file) {
      this.file = file;
    }

    public ReadBuilder project(Schema newSchema) {
      this.projectedSchema = newSchema;
      return this;
    }

    public ReadBuilder schema(Schema fileSchema) {
      this.schema = fileSchema;
      return this;
    }

    public ReadBuilder filter(Expression newFilter) {
      this.filter = newFilter;
      return this;
    }

    public ReadBuilder caseSensitive(boolean sensitive) {
      this.caseSensitive = sensitive;
      return this;
    }

    public ReadBuilder caseInsensitive() {
      return caseSensitive(false);
    }

    public ReadBuilder split(long start, long length) {
      // Split support will be implemented with Lance Fragment-based splitting
      return this;
    }

    public ReadBuilder reuseContainers() {
      return this;
    }

    @SuppressWarnings("unchecked")
    public <D> CloseableIterable<D> build() {
      Schema readSchema = schema != null ? schema : projectedSchema;
      Preconditions.checkNotNull(readSchema, "Schema is required for reading");
      Schema projected = projectedSchema != null ? projectedSchema : readSchema;
      return (CloseableIterable<D>) new LanceIterable<>(file, readSchema, projected);
    }
  }

  /** Builder for creating Lance data file writers that produce DataWriter instances. */
  public static class DataWriteBuilder {
    private final WriteBuilder appenderBuilder;
    private final String location;
    private PartitionSpec spec = null;
    private StructLike partition = null;
    private EncryptionKeyMetadata keyMetadata = null;
    private SortOrder sortOrder = null;

    private DataWriteBuilder(OutputFile file) {
      this.appenderBuilder = write(file);
      this.location = file.location();
    }

    public DataWriteBuilder forTable(Table table) {
      schema(table.schema());
      withSpec(table.spec());
      setAll(table.properties());
      return this;
    }

    public DataWriteBuilder schema(Schema newSchema) {
      appenderBuilder.schema(newSchema);
      return this;
    }

    public DataWriteBuilder set(String property, String value) {
      appenderBuilder.set(property, value);
      return this;
    }

    public DataWriteBuilder setAll(Map<String, String> properties) {
      appenderBuilder.setAll(properties);
      return this;
    }

    public DataWriteBuilder meta(String property, String value) {
      appenderBuilder.meta(property, value);
      return this;
    }

    public DataWriteBuilder overwrite() {
      appenderBuilder.overwrite();
      return this;
    }

    public DataWriteBuilder withSpec(PartitionSpec newSpec) {
      this.spec = newSpec;
      return this;
    }

    public DataWriteBuilder withPartition(StructLike newPartition) {
      this.partition = newPartition;
      return this;
    }

    public DataWriteBuilder withKeyMetadata(EncryptionKeyMetadata metadata) {
      this.keyMetadata = metadata;
      return this;
    }

    public DataWriteBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder;
      return this;
    }

    public <T> DataWriter<T> build() throws IOException {
      Preconditions.checkArgument(spec != null, "Cannot create data writer without spec");
      Preconditions.checkArgument(
          spec.isUnpartitioned() || partition != null,
          "Partition must not be null when creating data writer for partitioned spec");

      FileAppender<T> fileAppender = appenderBuilder.build();
      return new DataWriter<>(
          fileAppender, FileFormat.LANCE, location, spec, partition, keyMetadata, sortOrder);
    }
  }
}
