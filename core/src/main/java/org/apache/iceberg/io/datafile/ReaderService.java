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
package org.apache.iceberg.io.datafile;

import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;

/**
 * Service building readers. Implementations should be registered through the {@link
 * java.util.ServiceLoader}. {@link DataFileServiceRegistry} is used to collect and serve the reader
 * implementations.
 */
public interface ReaderService extends DataFileServiceRegistry.TypedService {
  /**
   * Provides a reader for the given input file which returns objects with a given type.
   *
   * @param inputFile to read
   * @param task to provide the values for metadata columns (_file_path, _spec_id, _partition)
   * @param readSchema to use when reading the data file
   * @param table to provide old partition specifications. Used for calculating values for
   *     _partition column after specification changes
   * @param deleteFilter is used when the delete record filtering is pushed down to the reader
   * @return {@link ReaderBuilder} for building the actual reader
   */
  ReaderBuilder<?> builder(
      InputFile inputFile,
      ContentScanTask<?> task,
      Schema readSchema,
      Table table,
      DeleteFilter<?> deleteFilter);
}
