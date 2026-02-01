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

import java.util.Map;
import org.apache.iceberg.Schema;

/**
 * Base implementation of {@link FormatModel} that provides common functionality for format models.
 *
 * <p>This abstract class serves as a foundation for creating format-specific models that handle
 * reading and writing data in various file formats.
 *
 * @param <D> output type used for reading data, and input type for writing data and deletes
 * @param <S> the type of the schema for the input/output data
 * @param <W> the writer type produced by the writer function
 * @param <R> the reader type produced by the reader function
 * @param <F> the file schema type used by the underlying file format
 */
public abstract class BaseFormatModel<D, S, W, R, F> implements FormatModel<D, S> {
  private final Class<? extends D> type;
  private final Class<S> schemaType;
  private final WriterFunction<W, S, F> writerFunction;
  private final ReaderFunction<R, S, F> readerFunction;

  /**
   * Constructs a new BaseFormatModel with the specified configuration.
   *
   * @param type the row type class for the object model implementation processed by this factory.
   * @param schemaType the schema type class for the object model implementation processed by this
   *     factory.
   * @param writerFunction the function used to create writers for this format
   * @param readerFunction the function used to create readers for this format
   */
  protected BaseFormatModel(
      Class<? extends D> type,
      Class<S> schemaType,
      WriterFunction<W, S, F> writerFunction,
      ReaderFunction<R, S, F> readerFunction) {
    this.type = type;
    this.schemaType = schemaType;
    this.writerFunction = writerFunction;
    this.readerFunction = readerFunction;
  }

  @Override
  public Class<? extends D> type() {
    return type;
  }

  @Override
  public Class<S> schemaType() {
    return schemaType;
  }

  /**
   * Returns the writer function used to create writers for this format.
   *
   * @return the writer function
   */
  protected WriterFunction<W, S, F> writerFunction() {
    return writerFunction;
  }

  /**
   * Returns the reader function used to create readers for this format.
   *
   * @return the reader function
   */
  protected ReaderFunction<R, S, F> readerFunction() {
    return readerFunction;
  }

  /**
   * A functional interface for creating writers that can write data in a specific format.
   *
   * @param <W> the writer type to be created
   * @param <S> the type of the schema for the input data
   * @param <F> the file schema type used by the underlying file format
   */
  @FunctionalInterface
  public interface WriterFunction<W, S, F> {
    /**
     * Creates a writer for the given schemas.
     *
     * @param icebergSchema the Iceberg schema defining the table structure
     * @param fileSchema the file format specific target schema for the output files
     * @param engineSchema the engine specific schema for the input data
     * @return a writer configured for the given schemas
     */
    W write(Schema icebergSchema, F fileSchema, S engineSchema);
  }

  /**
   * A functional interface for creating readers that can read data from a specific format.
   *
   * @param <R> the reader type to be created
   * @param <S> the type of the schema for the output data
   * @param <F> the file schema type used by the underlying file format
   */
  @FunctionalInterface
  public interface ReaderFunction<R, S, F> {
    /**
     * Creates a reader for the given schemas.
     *
     * @param icebergSchema the Iceberg schema defining the table structure
     * @param fileSchema the file format specific source schema for the input files
     * @param engineSchema the engine specific schema for the output data
     * @param idToConstant a map of field IDs to constant values for partition columns and other
     *     fields not stored in data files
     * @return a reader configured for the given schemas
     */
    R read(Schema icebergSchema, F fileSchema, S engineSchema, Map<Integer, ?> idToConstant);
  }
}
