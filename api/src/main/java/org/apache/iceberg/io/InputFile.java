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
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;

/**
 * An interface used to read input files using {@link SeekableInputStream} instances.
 *
 * <p>This class is based on Parquet's InputFile.
 */
public interface InputFile {
  /**
   * Returns the total length of the file, in bytes
   *
   * @return the total length of the file, in bytes
   * @throws RuntimeIOException If the implementation throws an {@link IOException}
   */
  long getLength();

  /**
   * Opens a new {@link SeekableInputStream} for the underlying data file
   *
   * @return a seekable stream for reading the file
   * @throws NotFoundException If the file does not exist
   * @throws RuntimeIOException If the implementation throws an {@link IOException}
   */
  SeekableInputStream newStream();

  /**
   * The fully-qualified location of the input file as a String.
   *
   * @return the input file location
   */
  String location();

  /**
   * Checks whether the file exists.
   *
   * @return true if the file exists, false otherwise
   */
  boolean exists();
}
