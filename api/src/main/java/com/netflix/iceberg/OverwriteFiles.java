/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Projections;

/**
 * API for overwriting files in a table by filter expression.
 * <p>
 * This API accumulates file additions and produces a new {@link Snapshot} of the table by replacing
 * all the files that match the filter expression with the set of additions. This operation is used
 * to implement idempotent writes that always replace a section of a table with new data.
 * <p>
 * Overwrites can be validated
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 * This has no requirements for the latest snapshot and will not fail based on other snapshot
 * changes.
 */
public interface OverwriteFiles extends PendingUpdate<Snapshot> {
  /**
   * Delete files that match an {@link Expression} on data rows from the table.
   * <p>
   * A file is selected to be deleted by the expression if it could contain any rows that match the
   * expression (candidate files are selected using an
   * {@link Projections#inclusive(PartitionSpec) inclusive projection}). These candidate files are
   * deleted if all of the rows in the file must match the expression (the partition data matches
   * the expression's {@link Projections#strict(PartitionSpec)} strict projection}). This guarantees
   * that files are deleted if and only if all rows in the file must match the expression.
   * <p>
   * Files that may contain some rows that match the expression and some rows that do not will
   * result in a {@link ValidationException}.
   *
   * @param expr an expression on rows in the table
   * @return this for method chaining
   * @throws ValidationException If a file can contain both rows that match and rows that do not
   */
  OverwriteFiles overwriteByRowFilter(Expression expr);

  /**
   * Add a {@link DataFile} to the table.
   *
   * @param file a data file
   * @return this for method chaining
   */
  OverwriteFiles addFile(DataFile file);

  /**
   * Signal that each file added to the table must match the overwrite expression.
   * <p>
   * If this method is called, each added file is validated on commit to ensure that it matches the
   * overwrite row filter. This is used to ensure that writes are idempotent: that files cannot
   * be added during a commit that would not be removed if the operation were run a second time.
   *
   * @return this for method chaining
   */
  OverwriteFiles validateAddedFiles();
}
