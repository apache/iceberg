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
package org.apache.iceberg.data.lance;

import java.io.IOException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.lance.Lance;
import org.apache.iceberg.lance.LanceFileAppender;

/**
 * A generic writer for writing Iceberg GenericRecord instances to Lance format files.
 *
 * <p>This class provides a convenient way to write GenericRecord data to Lance files using the
 * Iceberg data module's representation.
 */
public class GenericLanceWriter {

  private GenericLanceWriter() {}

  /**
   * Open a Lance file for writing.
   *
   * @param outputFile the output file to write to
   * @param schema the schema of the data
   * @return a FileAppender for GenericRecord
   */
  public static FileAppender<GenericRecord> open(OutputFile outputFile, Schema schema) {
    return new LanceFileAppender<>(outputFile, schema);
  }

  /**
   * Create a write builder using the Lance entry class.
   *
   * @param outputFile the output file to write to
   * @return a Lance.WriteBuilder
   */
  public static Lance.WriteBuilder builder(OutputFile outputFile) {
    return Lance.write(outputFile);
  }

  /**
   * Create a data write builder using the Lance entry class.
   *
   * @param outputFile the output file to write to
   * @return a Lance.DataWriteBuilder
   */
  public static Lance.DataWriteBuilder dataBuilder(OutputFile outputFile) {
    return Lance.writeData(outputFile);
  }
}
