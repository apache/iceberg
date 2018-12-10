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

package com.netflix.iceberg;

import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;

/**
 * SPI interface to abstract table metadata access and updates.
 */
public interface TableOperations {

  /**
   * Return the currently loaded table metadata, without checking for updates.
   *
   * @return table metadata
   */
  TableMetadata current();

  /**
   * Return the current table metadata after checking for updates.
   *
   * @return table metadata
   */
  TableMetadata refresh();

  /**
   * Replace the base table metadata with a new version.
   * <p>
   * This method should implement and document atomicity guarantees.
   * <p>
   * Implementations must check that the base metadata is current to avoid overwriting updates.
   * Once the atomic commit operation succeeds, implementations must not perform any operations that
   * may fail because failure in this method cannot be distinguished from commit failure.
   *
   * @param base     table metadata on which changes were based
   * @param metadata new table metadata with updates
   */
  void commit(TableMetadata base, TableMetadata metadata);

  /**
   * Create a new {@link InputFile} for a path.
   *
   * @param path a string file path
   * @return an InputFile instance for the path
   */
  InputFile newInputFile(String path);

  /**
   * Create a new {@link OutputFile} in the table's metadata store.
   *
   * @param filename a string file name, not a full path
   * @return an OutputFile instance for the path
   */
  OutputFile newMetadataFile(String filename);

  /**
   * Delete a file.
   *
   * @param path path to the file
   */
  void deleteFile(String path);

  /**
   * Create a new ID for a Snapshot
   *
   * @return a long snapshot ID
   */
  long newSnapshotId();

}
