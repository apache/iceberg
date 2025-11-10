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
   * @param file the input file to read
   * @param schema the expected schema for the data
   * @param options parquet read options
   * @param batchedReaderFunc function to create a VectorizedReader from a MessageType
   * @param mapping name mapping for schema evolution
   * @param filter filter expression to apply during reading
   * @param reuseContainers whether to reuse containers for records
   * @param caseSensitive whether column name matching should be case-sensitive
   * @param maxRecordsPerBatch maximum number of records per batch
   * @param properties additional properties for reader configuration
   * @param start optional start position for reading
   * @param length optional length to read
   * @param fileEncryptionKey optional encryption key for the file
   * @param fileAADPrefix optional AAD prefix for encryption
   * @param <T> the type of records returned by the reader
   * @return a closeable iterable of records
   */
  <T> CloseableIterable<T> createReader(
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
      ByteBuffer fileAADPrefix);
}
