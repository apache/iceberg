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
package org.apache.iceberg;

import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileFormatAppenderBuilder;
import org.apache.iceberg.io.FileFormatDataWriterBuilder;
import org.apache.iceberg.io.FileFormatEqualityDeleteWriterBuilder;
import org.apache.iceberg.io.FileFormatPositionDeleteWriterBuilder;

/**
 * Service building readers. Implementations should be registered through the {@link
 * java.util.ServiceLoader}. {@link DataFileReaderServiceRegistry} is used to collect and serve the
 * reader implementations.
 */
public interface DataFileWriterService<S> {
  /**
   * Returns the file format which is read by the readers.
   *
   * @return the input format of the reader
   */
  FileFormat format();

  /**
   * Returns the return type which is generated by the readers.
   *
   * @return the return type of the reader
   */
  Class<?> returnType();

  /** Provides a reader for the given input file which returns objects with a given returnType. */
  FileFormatAppenderBuilder<?> appenderBuilder(EncryptedOutputFile outputFile, S rowType);

  FileFormatDataWriterBuilder<?> dataWriterBuilder(EncryptedOutputFile outputFile, S rowType);

  FileFormatEqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
      EncryptedOutputFile outputFile, S rowType);

  FileFormatPositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
      EncryptedOutputFile outputFile, S rowType);
}
