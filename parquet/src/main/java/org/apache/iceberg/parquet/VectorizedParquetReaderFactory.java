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
package org.apache.iceberg.parquet;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.schema.MessageType;

/**
 * Interface for creating custom vectorized Parquet readers.
 *
 * <p>Implementations of this interface are loaded at runtime via reflection by specifying the fully
 * qualified class name in the {@code read.parquet.vectorized-reader.factory} configuration
 * property.
 *
 * <p>This allows for pluggable vectorized reader implementations (e.g., Comet, Arrow, Velox)
 * without requiring the core parquet module to depend on specific execution engines.
 */
public interface VectorizedParquetReaderFactory {

  /**
   * Returns the unique identifier for this reader factory.
   *
   * <p>This name is used to select the reader factory via configuration. For example, "comet" for
   * the Comet vectorized reader.
   *
   * @return the unique name for this factory
   */
  String name();

  /**
   * Creates a vectorized parquet reader with the given configuration.
   *
   * @param params reader parameters encapsulating all configuration options
   * @param <T> the type of records returned by the reader
   * @return a closeable iterable of records
   */
  <T> CloseableIterable<T> createReader(ReaderParams params);

  /** Parameters for creating a vectorized parquet reader. */
  interface ReaderParams {
    InputFile file();

    Schema schema();

    ParquetReadOptions options();

    Function<MessageType, VectorizedReader<?>> batchedReaderFunc();

    NameMapping mapping();

    Expression filter();

    boolean reuseContainers();

    boolean caseSensitive();

    int maxRecordsPerBatch();

    Map<String, String> properties();

    Long start();

    Long length();

    ByteBuffer fileEncryptionKey();

    ByteBuffer fileAADPrefix();

    static Builder builder(
        InputFile file,
        Schema schema,
        ParquetReadOptions options,
        Function<MessageType, VectorizedReader<?>> batchedReaderFunc) {
      return new Builder(file, schema, options, batchedReaderFunc);
    }

    /** Builder for ReaderParams. */
    class Builder {
      private final InputFile file;
      private final Schema schema;
      private final ParquetReadOptions options;
      private final Function<MessageType, VectorizedReader<?>> batchedReaderFunc;
      private NameMapping mapping = null;
      private Expression filter = null;
      private boolean reuseContainers = false;
      private boolean caseSensitive = true;
      private int maxRecordsPerBatch = 10000;
      private Map<String, String> properties = null;
      private Long start = null;
      private Long length = null;
      private ByteBuffer fileEncryptionKey = null;
      private ByteBuffer fileAADPrefix = null;

      private Builder(
          InputFile file,
          Schema schema,
          ParquetReadOptions options,
          Function<MessageType, VectorizedReader<?>> batchedReaderFunc) {
        this.file = file;
        this.schema = schema;
        this.options = options;
        this.batchedReaderFunc = batchedReaderFunc;
      }

      public Builder nameMapping(NameMapping nameMapping) {
        this.mapping = nameMapping;
        return this;
      }

      public Builder filter(Expression filterExpr) {
        this.filter = filterExpr;
        return this;
      }

      public Builder reuseContainers(boolean reuse) {
        this.reuseContainers = reuse;
        return this;
      }

      public Builder caseSensitive(boolean sensitive) {
        this.caseSensitive = sensitive;
        return this;
      }

      public Builder maxRecordsPerBatch(int maxRecords) {
        this.maxRecordsPerBatch = maxRecords;
        return this;
      }

      public Builder properties(Map<String, String> props) {
        this.properties = props;
        return this;
      }

      public Builder split(Long splitStart, Long splitLength) {
        this.start = splitStart;
        this.length = splitLength;
        return this;
      }

      public Builder encryption(ByteBuffer encryptionKey, ByteBuffer aadPrefix) {
        this.fileEncryptionKey = encryptionKey;
        this.fileAADPrefix = aadPrefix;
        return this;
      }

      public ReaderParams build() {
        return new ReaderParamsImpl(
            file,
            schema,
            options,
            batchedReaderFunc,
            mapping,
            filter,
            reuseContainers,
            caseSensitive,
            maxRecordsPerBatch,
            properties,
            start,
            length,
            fileEncryptionKey,
            fileAADPrefix);
      }
    }
  }

  /** Implementation of ReaderParams. */
  class ReaderParamsImpl implements ReaderParams {
    private final InputFile file;
    private final Schema schema;
    private final ParquetReadOptions options;
    private final Function<MessageType, VectorizedReader<?>> batchedReaderFunc;
    private final NameMapping mapping;
    private final Expression filter;
    private final boolean reuseContainers;
    private final boolean caseSensitive;
    private final int maxRecordsPerBatch;
    private final Map<String, String> properties;
    private final Long start;
    private final Long length;
    private final ByteBuffer fileEncryptionKey;
    private final ByteBuffer fileAADPrefix;

    private ReaderParamsImpl(
        InputFile file,
        Schema schema,
        ParquetReadOptions options,
        Function<MessageType, VectorizedReader<?>> batchedReaderFunc,
        NameMapping mapping,
        Expression filter,
        boolean reuseContainers,
        boolean caseSensitive,
        int maxRecordsPerBatch,
        Map<String, String> properties,
        Long start,
        Long length,
        ByteBuffer fileEncryptionKey,
        ByteBuffer fileAADPrefix) {
      this.file = file;
      this.schema = schema;
      this.options = options;
      this.batchedReaderFunc = batchedReaderFunc;
      this.mapping = mapping;
      this.filter = filter;
      this.reuseContainers = reuseContainers;
      this.caseSensitive = caseSensitive;
      this.maxRecordsPerBatch = maxRecordsPerBatch;
      this.properties = properties;
      this.start = start;
      this.length = length;
      this.fileEncryptionKey = fileEncryptionKey;
      this.fileAADPrefix = fileAADPrefix;
    }

    @Override
    public InputFile file() {
      return file;
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public ParquetReadOptions options() {
      return options;
    }

    @Override
    public Function<MessageType, VectorizedReader<?>> batchedReaderFunc() {
      return batchedReaderFunc;
    }

    @Override
    public NameMapping mapping() {
      return mapping;
    }

    @Override
    public Expression filter() {
      return filter;
    }

    @Override
    public boolean reuseContainers() {
      return reuseContainers;
    }

    @Override
    public boolean caseSensitive() {
      return caseSensitive;
    }

    @Override
    public int maxRecordsPerBatch() {
      return maxRecordsPerBatch;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public Long start() {
      return start;
    }

    @Override
    public Long length() {
      return length;
    }

    @Override
    public ByteBuffer fileEncryptionKey() {
      return fileEncryptionKey;
    }

    @Override
    public ByteBuffer fileAADPrefix() {
      return fileAADPrefix;
    }
  }
}
