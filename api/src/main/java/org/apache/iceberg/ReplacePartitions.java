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


/**
 * Not recommended: API for overwriting files in a table by partition.
 * <p>
 * This is provided to implement SQL compatible with Hive table operations but is not recommended.
 * Instead, use the {@link OverwriteFiles overwrite API} to explicitly overwrite data.
 * <p>
 * This API accumulates file additions and produces a new {@link Snapshot} of the table by replacing
 * all files in partitions with new data with the new additions. This operation is used to implement
 * dynamic partition replacement.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 * This has no requirements for the latest snapshot and will not fail based on other snapshot
 * changes.
 */
public interface ReplacePartitions extends SnapshotUpdate<ReplacePartitions> {
  /**
   * Add a {@link DataFile} to the table.
   *
   * @param file a data file
   * @return this for method chaining
   */
  ReplacePartitions addFile(DataFile file);

  /**
   * Validate that no partitions will be replaced and the operation is append-only.
   *
   * @return this for method chaining
   */
  ReplacePartitions validateAppendOnly();

  /**
   * Set the snapshot ID used in any reads for this operation.
   * <p>
   * Validations will check changes after this snapshot ID. If the from snapshot is not set, all ancestor snapshots
   * through the table's initial snapshot are validated.
   *
   * @param snapshotId a snapshot ID
   * @return this for method chaining
   */
  ReplacePartitions validateFromSnapshot(long snapshotId);

  /**
   * Enables validation that data and delete files added concurrently do not conflict with this commit's operation.
   * <p>
   * This method should be called when the table is first queried to determine which files to overwrite.
   * If a concurrent operation commits a new file or row delta after the data was read that might
   * contain rows matching a partition marked for deletion, the overwrite operation will detect this and fail.
   * <p>
   * Validation applies to files added to the table since the snapshot passed to {@link #validateFromSnapshot(long)}.
   *
   * @return this for method chaining
   */
  ReplacePartitions validateNoConflicts();
}
