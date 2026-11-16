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

import org.apache.iceberg.ManifestFile;

/**
 * An action that repairs a table.
 *
 * <p>The repairs to perform are selected through the configuration methods of this action.
 * Implementations rewrite the affected metadata, producing a new set of manifests in which the
 * repaired entries replace the corrupt ones.
 */
public interface RepairTable extends SnapshotUpdate<RepairTable, RepairTable.Result> {

  /**
   * Repairs incorrect metrics of manifest entries, such as record counts, file sizes and column
   * level statistics, by comparing them against the underlying data and delete files.
   *
   * @return this for method chaining
   */
  RepairTable repairFileMetrics();

  /**
   * Determines the repairs that would be performed without actually committing the operation to the
   * table.
   *
   * @return this for method chaining
   */
  RepairTable dryRun();

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Returns the repaired manifests. */
    Iterable<ManifestFile> repairedManifests();

    /** Returns the number of manifest entries that were repaired. */
    long repairedEntryCount();
  }
}
