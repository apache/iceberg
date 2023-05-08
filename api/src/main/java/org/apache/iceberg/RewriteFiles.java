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

import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * API for replacing files in a table.
 *
 * <p>This API accumulates file additions and deletions, produces a new {@link Snapshot} of the
 * changes, and commits that snapshot as the current.
 *
 * <p>When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 * If any of the deleted files are no longer in the latest snapshot when reattempting, the commit
 * will throw a {@link ValidationException}.
 *
 * <p>Note that the new state of the table after each rewrite must be logically equivalent to the
 * original table state.
 */
public interface RewriteFiles extends SnapshotUpdate<RewriteFiles> {
  /**
   * Remove a data file from the current table state.
   *
   * <p>This rewrite operation may change the size or layout of the data files. When applicable, it
   * is also recommended to discard already deleted records while rewriting data files. However, the
   * set of live data records must never change.
   *
   * @param dataFile a rewritten data file
   * @return this for method chaining
   */
  default RewriteFiles deleteFile(DataFile dataFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement deleteFile");
  }

  /**
   * Remove a delete file from the table state.
   *
   * <p>This rewrite operation may change the size or layout of the delete files. When applicable,
   * it is also recommended to discard delete records for files that are no longer part of the table
   * state. However, the set of applicable delete records must never change.
   *
   * @param deleteFile a rewritten delete file
   * @return this for method chaining
   */
  default RewriteFiles deleteFile(DeleteFile deleteFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement deleteFile");
  }

  /**
   * Add a new data file.
   *
   * <p>This rewrite operation may change the size or layout of the data files. When applicable, it
   * is also recommended to discard already deleted records while rewriting data files. However, the
   * set of live data records must never change.
   *
   * @param dataFile a new data file
   * @return this for method chaining
   */
  default RewriteFiles addFile(DataFile dataFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement addFile");
  }

  /**
   * Add a new delete file.
   *
   * <p>This rewrite operation may change the size or layout of the delete files. When applicable,
   * it is also recommended to discard delete records for files that are no longer part of the table
   * state. However, the set of applicable delete records must never change.
   *
   * @param deleteFile a new delete file
   * @return this for method chaining
   */
  default RewriteFiles addFile(DeleteFile deleteFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement addFile");
  }

  /**
   * Configure the data sequence number for this rewrite operation. This data sequence number will
   * be used for all new data files that are added in this rewrite. This method is helpful to avoid
   * commit conflicts between data compaction and adding equality deletes.
   *
   * @param sequenceNumber a data sequence number
   * @return this for method chaining
   */
  default RewriteFiles dataSequenceNumber(long sequenceNumber) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement dataSequenceNumber");
  }

  /**
   * Add a rewrite that replaces one set of data files with another set that contains the same data.
   *
   * @param filesToDelete files that will be replaced (deleted), cannot be null or empty.
   * @param filesToAdd files that will be added, cannot be null or empty.
   * @return this for method chaining
   * @deprecated since 1.3.0, will be removed in 2.0.0
   */
  @Deprecated
  default RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd) {
    return rewriteFiles(filesToDelete, ImmutableSet.of(), filesToAdd, ImmutableSet.of());
  }

  /**
   * Add a rewrite that replaces one set of data files with another set that contains the same data.
   * The sequence number provided will be used for all the data files added.
   *
   * @param filesToDelete files that will be replaced (deleted), cannot be null or empty.
   * @param filesToAdd files that will be added, cannot be null or empty.
   * @param sequenceNumber sequence number to use for all data files added
   * @return this for method chaining
   * @deprecated since 1.3.0, will be removed in 2.0.0
   */
  @Deprecated
  RewriteFiles rewriteFiles(
      Set<DataFile> filesToDelete, Set<DataFile> filesToAdd, long sequenceNumber);

  /**
   * Add a rewrite that replaces one set of files with another set that contains the same data.
   *
   * @param dataFilesToReplace data files that will be replaced (deleted).
   * @param deleteFilesToReplace delete files that will be replaced (deleted).
   * @param dataFilesToAdd data files that will be added.
   * @param deleteFilesToAdd delete files that will be added.
   * @return this for method chaining.
   * @deprecated since 1.3.0, will be removed in 2.0.0
   */
  @Deprecated
  RewriteFiles rewriteFiles(
      Set<DataFile> dataFilesToReplace,
      Set<DeleteFile> deleteFilesToReplace,
      Set<DataFile> dataFilesToAdd,
      Set<DeleteFile> deleteFilesToAdd);

  /**
   * Set the snapshot ID used in any reads for this operation.
   *
   * <p>Validations will check changes after this snapshot ID. If this is not called, all ancestor
   * snapshots through the table's initial snapshot are validated.
   *
   * @param snapshotId a snapshot ID
   * @return this for method chaining
   */
  RewriteFiles validateFromSnapshot(long snapshotId);
}
