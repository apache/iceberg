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
package org.apache.iceberg.io;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;

/**
 * Interface that provides a unified abstraction for converting between data file formats and
 * input/output data representations.
 *
 * <p>FileAccessFactory serves as a bridge between storage formats ({@link FileFormat}) and expected
 * input/output data structures, optimizing performance through direct conversion without
 * intermediate representations. File format implementations handle the low-level parsing details
 * while the object model determines the in-memory representation used for the parsed data.
 * Together, these provide a consistent API for consuming data files while optimizing for specific
 * processing engines.
 *
 * <p>The interface provides:
 *
 * <ul>
 *   <li>{@link ReadBuilder} - creates readers for converting file formats to output objects
 *   <li>{@link WriteBuilder} - creates writers for converting input objects to file formats
 * </ul>
 *
 * <p>Iceberg provides these built-in object models:
 *
 * <ul>
 *   <li><strong>generic</strong> - for {@link Record} objects (engine-agnostic)
 *   <li><strong>spark</strong> - for Apache Spark InternalRow objects
 *   <li><strong>spark-vectorized</strong> - for columnar batch processing in Spark (not supported
 *       with {@link FileFormat#AVRO})
 *   <li><strong>flink</strong> - for Apache Flink RowData objects
 *   <li><strong>arrow</strong> - for Apache Arrow columnar format (only supported with {@link
 *       FileFormat#PARQUET})
 * </ul>
 *
 * <p>Processing engines can implement custom object models to integrate with Iceberg's file reading
 * and writing capabilities.
 *
 * @param <E> input schema type used for writing data
 */
public interface FileAccessFactory<E, D> {
  /** The file format which is read/written by the object model. */
  FileFormat format();

  /**
   * Returns the unique identifier for the object model implementation processed by this factory.
   *
   * <p>Object model names (such as "generic", "spark", "spark-vectorized", "flink", "arrow")
   * identify the input/output data representations that this implementation can process. These
   * identifiers allow users/engines to explicitly select which data representation to use.
   *
   * <p>The object model name acts as a contract specifying the expected data structures for both
   * reading (converting file formats into output objects) and writing (converting input objects
   * into file formats). This ensures proper integration between Iceberg's storage layer and
   * processing engines.
   *
   * @return string identifier for this object model implementation
   */
  String objectModeName();

  /**
   * Creates a writer builder for standard data files.
   *
   * <p>The returned {@link WriteBuilder} configures and creates a writer that converts input
   * objects into the file format supported by this factory for regular data content.
   *
   * <p>The builder follows the fluent pattern for configuring writer properties like compression,
   * encryption, row group size, and other format-specific options.
   *
   * @param outputFile destination for the written data
   * @return configured writer builder for standard data files
   * @param <B> the concrete builder type for method chaining
   */
  <B extends WriteBuilder<B, E, D>> B dataWriteBuilder(OutputFile outputFile);

  /**
   * Creates a writer builder for equality delete files.
   *
   * <p>The returned {@link WriteBuilder} configures and creates a writer that converts input
   * objects into the file format supported by this factory for equality delete content.
   *
   * <p>Equality delete files contain records that identify rows to be deleted based on equality
   * conditions.
   *
   * <p>The builder follows the fluent pattern for configuring writer properties like compression,
   * encryption, row group size, and other format-specific options.
   *
   * @param outputFile destination for the written equality delete data
   * @return configured writer builder for equality delete files
   * @param <B> the concrete builder type for method chaining
   */
  <B extends WriteBuilder<B, E, D>> B equalityDeleteWriteBuilder(OutputFile outputFile);

  /**
   * Creates a writer builder for position delete files.
   *
   * <p>The returned {@link WriteBuilder} configures and creates a writer that converts {@link
   * PositionDelete} objects into the file format supported by this factory for position delete
   * content.
   *
   * <p>Position delete files contain records that identify rows to be deleted by file path and
   * position. Each PositionDelete object could contain the writer's output type in its row field.
   *
   * <p>The builder follows the fluent pattern for configuring writer properties like compression,
   * encryption, row group size, and other format-specific options.
   *
   * @param outputFile destination for the written position delete data
   * @return configured writer builder for position delete files
   * @param <B> the concrete builder type for method chaining
   */
  <B extends WriteBuilder<B, E, StructLike>> B positionDeleteWriteBuilder(OutputFile outputFile);

  /**
   * Creates a file reader builder for the specified input file.
   *
   * <p>The returned {@link ReadBuilder} configures and creates a reader that converts data from the
   * file format into output objects supported by this factory. The builder allows for configuration
   * of various reading aspects like schema projection, predicate pushdown, row/batch size,
   * container reuse, encryption settings, and other format-specific options.
   *
   * <p>The builder follows the fluent pattern for configuring reader properties and ultimately
   * creates a {@link CloseableIterable} for consuming the file data.
   *
   * @param inputFile source file to read from
   * @return configured reader builder for the specified input
   * @param <B> the concrete builder type for method chaining
   */
  <B extends ReadBuilder<B, D>> B readBuilder(InputFile inputFile);
}
