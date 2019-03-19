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

import com.netflix.iceberg.encryption.EncryptionManager;
import com.netflix.iceberg.encryption.PlaintextEncryptionManager;
import com.netflix.iceberg.io.FileIO;
import com.netflix.iceberg.io.LocationProvider;
import java.util.UUID;

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
   * @return a {@link FileIO} to read and write table data and metadata files
   */
  FileIO io();

  /**
   * @return a {@link com.netflix.iceberg.encryption.EncryptionManager} to encrypt and decrypt
   * data files.
   */
  default EncryptionManager encryption() {
    return new PlaintextEncryptionManager();
  }

  /**
   * Given the name of a metadata file, obtain the full path of that file using an appropriate base
   * location of the implementation's choosing.
   * <p>
   * The file may not exist yet, in which case the path should be returned as if it were to be created
   * by e.g. {@link FileIO#newOutputFile(String)}.
   */
  String metadataFileLocation(String fileName);

  /**
   * Returns a {@link LocationProvider} that supplies locations for new new data files.
   *
   * @return a location provider configured for the current table state
   */
  LocationProvider locationProvider();

  /**
   * Create a new ID for a Snapshot
   *
   * @return a long snapshot ID
   */
  default long newSnapshotId() {
    UUID uuid = UUID.randomUUID();
    long mostSignificantBits = uuid.getMostSignificantBits();
    long leastSignificantBits = uuid.getLeastSignificantBits();
    return Math.abs(mostSignificantBits ^ leastSignificantBits);
  }

}
