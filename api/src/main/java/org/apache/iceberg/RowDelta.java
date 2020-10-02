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
 * API for encoding row-level changes to a table.
 * <p>
 * This API accumulates data and delete file changes, produces a new {@link Snapshot} of the table, and commits
 * that snapshot as the current.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 */
public interface RowDelta extends SnapshotUpdate<RowDelta> {
  /**
   * Add a {@link DataFile} to the table.
   *
   * @param inserts a data file of rows to insert
   * @return this for method chaining
   */
  RowDelta addRows(DataFile inserts);

  /**
   * Add a {@link DeleteFile} to the table.
   *
   * @param deletes a delete file of rows to delete
   * @return this for method chaining
   */
  RowDelta addDeletes(DeleteFile deletes);

  /**
   * Set the snapshot ID used to produce delete files.
   * <p>
   * Validations will check changes after this snapshot ID.
   *
   * @param snapshotId a snapshot ID
   * @return this for method chaining
   */
  RowDelta validateFromSnapshot(long snapshotId);

  /**
   * Add data file paths that must not be deleted for this RowDelta to succeed.
   * <p>
   * If any path has been removed from the table since the snapshot passed to {@link #validateFromSnapshot(long)}, the
   * operation will fail with a {@link org.apache.iceberg.exceptions.ValidationException}.
   *
   * @param referencedFiles file paths that are referenced by a position delete file
   * @return this for method chaining
   */
  RowDelta validateDataFilesExist(Iterable<? extends CharSequence> referencedFiles);

  /**
   * Enable validation that referenced data files passed to {@link #validateDataFilesExist(Iterable)} have not been
   * removed by a delete operation.
   *
   * @return this for method chaining
   */
  RowDelta validateDeletedFiles();
}
