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

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.exceptions.AlreadyExistsException;

/**
 * An interface used to create output files using {@link PositionOutputStream} instances.
 *
 * <p>This class is based on Parquet's InputFile.
 */
public interface OutputFile {

  /**
   * Create a new file and return a {@link PositionOutputStream} to it.
   *
   * <p>If the file already exists, this will throw an exception.
   *
   * @return an output stream that can report its position
   * @throws AlreadyExistsException If the path already exists
   * @throws UncheckedIOException If the implementation throws an {@link IOException}
   */
  PositionOutputStream create();

  /**
   * Create a new file and return a {@link PositionOutputStream} to it.
   *
   * <p>If the file already exists, this will not throw an exception and will replace the file.
   *
   * @return an output stream that can report its position
   * @throws UncheckedIOException If the implementation throws an {@link IOException}
   * @throws SecurityException If staging directory creation fails due to missing JVM level
   *     permission
   */
  PositionOutputStream createOrOverwrite();

  /**
   * Return the location this output file will create.
   *
   * @return the location of this output file
   */
  String location();

  /**
   * Return an {@link InputFile} for the location of this output file.
   *
   * @return an input file for the location of this output file
   */
  InputFile toInputFile();
}
