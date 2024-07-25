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

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;

/**
 * API for appending sequential updates to a table
 *
 * <p>This API accumulates batches of file additions and deletions by order, produces a new {@link
 * Snapshot} of the changes where each batch is added to a new data sequence number, and commits
 * that snapshot as the current.
 *
 * <p>When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 * If any of the deleted files are no longer in the latest snapshot when reattempting, the commit
 * will throw a {@link ValidationException}.
 */
public interface StreamingUpdate extends SnapshotUpdate<StreamingUpdate> {

  /**
   * Start a new batch of changes. The changes in this batch will have a sequence number larger than
   * the changes in the previous batches.
   *
   * @return this for method chaining
   */
  default StreamingUpdate newBatch() {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement newBatch");
  }

  /**
   * Add a new data file to the current batch. All files in this batch will receive the same data
   * sequence number.
   *
   * @param dataFile a new data file
   * @return this for method chaining
   */
  default StreamingUpdate addFile(DataFile dataFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement addFile");
  }

  /**
   * Add a new delete file to the current batch. All files in this batch will receive the same data
   * sequence number.
   *
   * @param deleteFile a new delete file
   * @return this for method chaining
   */
  default StreamingUpdate addFile(DeleteFile deleteFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement addFile");
  }

  /**
   * Set the snapshot ID used in any reads for this operation.
   *
   * <p>Validations will check changes after this snapshot ID. If the from snapshot is not set, all
   * ancestor snapshots through the table's initial snapshot are validated.
   *
   * @param snapshotId a snapshot ID
   * @return this for method chaining
   */
  StreamingUpdate validateFromSnapshot(long snapshotId);

  /**
   * Sets a conflict detection filter used to validate concurrently added data and delete files.
   *
   * <p>If not called, a true literal will be used as the conflict detection filter.
   *
   * @param newConflictDetectionFilter an expression on rows in the table
   * @return this for method chaining
   */
  StreamingUpdate conflictDetectionFilter(Expression newConflictDetectionFilter);

  /**
   * Enables validation that data files added concurrently do not conflict with this commit's
   * operation.
   *
   * <p>This method should be called when the table is queried to determine which files to
   * delete/append. If a concurrent operation commits a new file after the data was read and that
   * file might contain rows matching the specified conflict detection filter, this operation will
   * detect this during retries and fail.
   *
   * <p>Calling this method is required to maintain serializable isolation for update/delete
   * operations. Otherwise, the isolation level will be snapshot isolation.
   *
   * <p>Validation uses the conflict detection filter passed to {@link
   * #conflictDetectionFilter(Expression)} and applies to operations that happened after the
   * snapshot passed to {@link #validateFromSnapshot(long)}.
   *
   * @return this for method chaining
   */
  StreamingUpdate validateNoConflictingDataFiles();

  /**
   * Enables validation that delete files added concurrently do not conflict with this commit's
   * operation.
   *
   * <p>This method must be called when the table is queried to produce a row delta for UPDATE and
   * MERGE operations independently of the isolation level. Calling this method isn't required for
   * DELETE operations as it is OK to delete a record that is also deleted concurrently.
   *
   * <p>Validation uses the conflict detection filter passed to {@link
   * #conflictDetectionFilter(Expression)} and applies to operations that happened after the
   * snapshot passed to {@link #validateFromSnapshot(long)}.
   *
   * @return this for method chaining
   */
  StreamingUpdate validateNoConflictingDeleteFiles();
}
