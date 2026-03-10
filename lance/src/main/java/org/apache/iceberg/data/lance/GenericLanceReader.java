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

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.lance.Lance;
import org.apache.iceberg.lance.LanceIterable;

/**
 * A generic reader for reading data from Lance format files into Iceberg GenericRecord instances.
 *
 * <p>This class provides a convenient way to read Lance files using the Iceberg data module's
 * GenericRecord representation.
 */
public class GenericLanceReader {

  private GenericLanceReader() {}

  /**
   * Open a Lance file for reading.
   *
   * @param inputFile the input file to read from
   * @param schema the schema of the file
   * @return a CloseableIterable of GenericRecord
   */
  public static CloseableIterable<GenericRecord> open(InputFile inputFile, Schema schema) {
    return open(inputFile, schema, schema);
  }

  /**
   * Open a Lance file for reading with a projected schema.
   *
   * @param inputFile the input file to read from
   * @param fileSchema the full schema of the file
   * @param projectedSchema the projected schema to read
   * @return a CloseableIterable of GenericRecord
   */
  public static CloseableIterable<GenericRecord> open(
      InputFile inputFile, Schema fileSchema, Schema projectedSchema) {
    return new LanceIterable<>(inputFile, fileSchema, projectedSchema);
  }

  /**
   * Create a read builder using the Lance entry class.
   *
   * @param inputFile the input file to read from
   * @return a Lance.ReadBuilder
   */
  public static Lance.ReadBuilder builder(InputFile inputFile) {
    return Lance.read(inputFile);
  }
}
