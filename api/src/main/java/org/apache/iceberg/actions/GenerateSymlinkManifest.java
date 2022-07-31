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
package org.apache.iceberg.actions;

/**
 * An action that generates a symlink manifest table for an Iceberg table.
 *
 * <p>This action will generate symlink manifests for a table. A symlink manifest is a file with
 * links to the data files in the table. Systems which do not integrate with Iceberg can read the
 * underlying data in the Iceberg tables as an external table. The symlink manifest table will
 * expose all the hidden partitions as the union of historical partition specs.
 */
public interface GenerateSymlinkManifest
    extends Action<GenerateSymlinkManifest, GenerateSymlinkManifest.Result> {

  /**
   * Set a root location for the symlink manifest table. If a root location is not provided the
   * default location will be at table_root/_symlink_format_manifest/snapshot_id/
   *
   * @param rootLocation root location for symlink manifest table
   * @return this for method chaining
   */
  GenerateSymlinkManifest rootLocation(String rootLocation);

  /** The action result that contains a summary of the execution. */
  interface Result {

    /** Returns the number of data files in the generated symlink file */
    long dataFilesCount();

    /** Returns the snapshot id used when generating the symlink file */
    long snapshotId();
  }
}
