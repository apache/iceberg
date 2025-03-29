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
 * An interface for object models that can be used to read and write Iceberg data files.
 *
 * @param <E> the native type of the object model
 */
public interface ObjectModel<E> {
  /** The file format which is read by the object model. */
  FileFormat format();

  /**
   * The name of the object model. Allows users to specify the object model to map the data file for
   * reading and writing. The currently supported object models are:
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
   */
  String name();

  /**
   * The appender builder for the output file which writes the data in the specified file format and
   * accepts the specified object model.
   *
   * @param outputFile to write to
   * @return the appender builder
   * @param <B> The type of the appender builder
   */
  <B extends AppenderBuilder<B, E>> B appenderBuilder(OutputFile outputFile);

  /**
   * The reader builder for the input file which reads the data from the specified file format and
   * returns the specified object model.
   *
   * @param inputFile to read from
   * @return the reader builder
   * @param <B> The type of the reader builder
   */
  <B extends ReadBuilder<B>> B readBuilder(InputFile inputFile);
}
