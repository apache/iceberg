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
 * API for overwriting files in a table by partition.
 * <p>
 * This is provided to implement SQL compatible with Hive table operations but is not recommended.
 * Instead, use the {@link OverwriteFiles overwrite API} to explicitly overwrite data.
 * <p>
 * The default validation mode is idempotent, meaning the overwrite is
 * correct and should be committed out regardless of other concurrent changes to the table.
 * Alternatively, this API can be configured to validate that no new data or deletes
 * have been applied since a snapshot id associated when this operation began.
 * </p>
 * <p>
 * This API accumulates file additions and produces a new {@link Snapshot} of the table by replacing
 * all files in partitions with new data with the new additions. This operation is used to implement
 * dynamic partition replacement.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
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
   * Set the snapshot ID used in validations for this operation.
   * <p>
   * All validations will check changes after this snapshot ID. If the from snapshot is not set, all ancestor snapshots
   * through the table's initial snapshot are validated.
   *
   * @param snapshotId a snapshot ID, it should be set to when this operation started to read the table.
   * @return this for method chaining
   */
  ReplacePartitions validateFromSnapshot(long snapshotId);

  /**
   * Enables validation that data and delete files added concurrently do not conflict with this operation.
   * <p>
   * This method should be called before this operation is committed.
   * If a concurrent operation committed a data file or row delta after the snapshot id set in
   * {@link #validateFromSnapshot} that might contain rows matching a partition marked for deletion,
   * validation will detect this and fail.
   * <p>
   *
   * @return this for method chaining
   */
  ReplacePartitions validateNoConflicts();
}
