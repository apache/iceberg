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

package com.netflix.iceberg.io;

import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.StructLike;
import java.io.Serializable;

/**
 * Pluggable module for reading, writing, and deleting files.
 * <p>
 * Both table metadata files and data files can be written and read by this module. Implementations
 * must be serializable because various clients of Spark tables may initialize this once and pass
 * it off to a separate module that would then interact with the streams.
 */
public interface FileIO extends Serializable {

  /**
   * Get a {@link InputFile} instance to read bytes from the file at the given path.
   */
  InputFile newInputFile(String path);

  /**
   * Get a {@link OutputFile} instance to write bytes to the file at the given path.
   */
  OutputFile newOutputFile(String path);

  /**
   * Delete the file at the given path.
   */
  void deleteFile(String path);

  /**
   * Get an {@link InputFile} to get the bytes for this table's metadata file with the given name.
   */
  InputFile readMetadataFile(String fileName);

  /**
   * Get an {@link OutputFile} to write bytes for a new table metadata file with the given name.
   */
  OutputFile newMetadataFile(String fileName);

  /**
   * Get an {@link OutputFile} for writing bytes to a new data file for this table.
   * <p>
   * The partition values of the rows in this file may be used to derive the final location of
   * the file.
   */
  OutputFile newPartitionedDataFile(
      PartitionSpec partitionSpec, StructLike partitionData, String fileName);

  /**
   * Get an {@link OutputFile} for writing bytes to a new data file for this table.
   * <p>
   * The table is not partitioned in this case.
   */
  OutputFile newUnpartitionedDataFile(String fileName);
}
