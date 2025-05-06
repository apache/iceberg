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

/**
 * Direct conversion is used between file formats and engine internal formats for performance
 * reasons. Object models encapsulate these conversions.
 *
 * <p>{@link ReadBuilder} is provided for reading data files stored in a given {@link FileFormat}
 * into the engine specific object model.
 *
 * <p>{@link AppenderBuilder} is provided for writing engine specific object model to data/delete
 * files stored in a given {@link FileFormat}.
 *
 * <p>Iceberg supports the following object models natively:
 *
 * <ul>
 *   <li>generic - reads and writes Iceberg {@link org.apache.iceberg.data.Record}s
 *   <li>spark - reads and writes Spark InternalRow records
 *   <li>spark-vectorized - vectorized reads for Spark columnar batches. Not supported for {@link
 *       FileFormat#AVRO}
 *   <li>flink - reads and writes Flink RowData records
 *   <li>arrow - vectorized reads for into Arrow columnar format. Only supported for {@link
 *       FileFormat#PARQUET}
 * </ul>
 *
 * <p>Engines could implement their own object models to leverage Iceberg data file reading and
 * writing capabilities.
 *
 * @param <E> the engine specific schema of the input data for the appender
 */
public interface ObjectModel<E> {
  /** The file format which is read/written by the object model. */
  FileFormat format();

  /**
   * The name of the object model. Allows users to specify the object model to map the data file for
   * reading and writing.
   */
  String name();

  /**
   * The appender builder for the output file which writes the data in the specified file format and
   * accepts the records defined by this object model. The 'mode' parameter defines the input type
   * for the specific writer use-cases. The appender should handle the following input in the
   * specific modes:
   *
   * <ul>
   *   <li>The appender's engine specific input type
   *       <ul>
   *         <li>{@link WriteMode#DATA_WRITER}
   *         <li>{@link WriteMode#EQUALITY_DELETE_WRITER}
   *       </ul>
   *   <li>{@link org.apache.iceberg.deletes.PositionDelete} where the type of the row is the
   *       appender's engine specific input type when the 'mode' is {@link
   *       WriteMode#POSITION_DELETE_WRITER}
   * </ul>
   *
   * @param outputFile to write to
   * @param mode for the appender
   * @return the appender builder
   * @param <B> The type of the appender builder
   */
  <B extends AppenderBuilder<B, E>> B appenderBuilder(OutputFile outputFile, WriteMode mode);

  /**
   * The reader builder for the input file which reads the data from the specified file format and
   * returns the records in this object model.
   *
   * @param inputFile to read from
   * @return the reader builder
   * @param <B> The type of the reader builder
   */
  <B extends ReadBuilder<B>> B readBuilder(InputFile inputFile);

  /**
   * Writer modes. Based on the mode the object model could alter the appender configuration when
   * creating the {@link FileAppender}.
   */
  enum WriteMode {
    /** Mode for writing data files. */
    DATA_WRITER,
    /** Mode for writing equality delete files. */
    EQUALITY_DELETE_WRITER,
    /** Mode for writing position delete files. */
    POSITION_DELETE_WRITER,
  }
}
