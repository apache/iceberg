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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * Interface that provides a unified abstraction for converting between data file formats and
 * input/output data representations.
 *
 * <p>{@link FormatModel} serves as a bridge between storage formats ({@link FileFormat}) and
 * expected input/output data structures, optimizing performance through direct conversion without
 * intermediate representations. File format implementations handle the low-level parsing details
 * while the object model determines the in-memory representation used for the parsed data.
 * Together, these provide a consistent API for consuming data files while optimizing for specific
 * processing engines.
 *
 * <p>Iceberg provides some built-in object models and processing engines can implement custom
 * object models to integrate with Iceberg's file reading and writing capabilities.
 *
 * @param <D> output type used for reading data, and input type for writing data and deletes
 * @param <S> the type of the schema for the input/output data
 */
public interface FormatModel<D, S> {
  /** The file format which is read/written by the object model. */
  FileFormat format();

  /**
   * Returns the unique identifier for the object model implementation processed by this factory.
   *
   * <p>The model types act as a contract specifying the expected data structures for both reading
   * (converting file formats into output objects) and writing (converting input objects into file
   * formats). This ensures proper integration between Iceberg's storage layer and processing
   * engines.
   *
   * <p>Processing engines can define their own object models by implementing this interface and
   * using their own model name. They can register these models with Iceberg by using the {@link
   * FormatModelRegistry}. This allows custom data representations to be seamlessly integrated with
   * Iceberg's file format handlers.
   *
   * @return the type of the data structures handled by this model implementation
   */
  Class<D> type();

  Class<S> schemaType();

  /**
   * Creates a writer builder for data files.
   *
   * <p>The returned {@link WriteBuilder} configures and creates a writer that converts input
   * objects into the file format supported by this factory.
   *
   * @param outputFile destination for the written data
   * @return configured writer builder
   */
  WriteBuilder writeBuilder(OutputFile outputFile);

  /**
   * Creates a file reader builder for the specified input file.
   *
   * <p>The returned {@link ReadBuilder} configures and creates a reader that converts data from the
   * file format into output objects supported by this factory.
   *
   * @param inputFile source file to read from
   * @return configured reader builder for the specified input
   */
  ReadBuilder readBuilder(InputFile inputFile);
}
