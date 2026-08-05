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
 * An action that repairs incorrect statistics in the manifests of a table.
 *
 * <p>Implementations rewrite the manifests of the table, producing a new set of manifests in which
 * the statistics of the entries are corrected to match the underlying data and delete files.
 */
public interface RepairManifests extends SnapshotUpdate<RepairManifests, RepairManifests.Result> {

  /**
   * Repairs incorrect statistics of manifest entries, such as record counts, file sizes and column
   * level statistics.
   *
   * @return this for method chaining
   */
  RepairManifests repairEntryStats();

  /**
   * Determines the repairs that would be performed without actually committing the operation to the
   * table.
   *
   * @return this for method chaining
   */
  RepairManifests dryRun();

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Returns the repaired manifests. */
    Iterable<ManifestFile> repairedManifests();

    /** Returns the number of manifest entries whose stats were repaired. */
    long repairedEntryCount();
  }
}
