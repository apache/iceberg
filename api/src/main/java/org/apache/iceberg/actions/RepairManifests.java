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

/** An action that will repair manifests. Implementations should produce a new set of manifests. */
public interface RepairManifests extends SnapshotUpdate<RepairManifests, RepairManifests.Result> {

  /** Configuration method for repairing manifest entry statistics */
  RepairManifests repairEntryStats();

  /**
   * Configuration method for removing duplicate file entries and removing files which no longer
   * exist in storage
   */
  RepairManifests repairFileEntries();

  /**
   * Configuration option for determining the rewritten and added manifests without actually
   * committing the operation to the table
   *
   * @return this for method chaining
   */
  RepairManifests dryRun();

  interface Result {
    /** Returns rewritten manifests. */
    Iterable<ManifestFile> rewrittenManifests();

    /** Returns the duplicate file paths removed */
    Iterable<String> duplicateFilesRemoved();

    /** Returns the paths of the missing files which were removed */
    Iterable<String> missingFilesRemoved();

    /** Returns the paths of the missing files which were recovered */
    Iterable<String> missingFilesRecovered();

    /** Returns the number of manifest entries for which stats were incorrect */
    long entryStatsIncorrectCount();

    /** Returns the number of manifest entries for which stats were corrected */
    long entryStatsRepairedCount();
  }
}
