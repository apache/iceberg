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
import org.apache.iceberg.expressions.Projections;

/**
 * API for deleting files from a table.
 *
 * <p>This API accumulates file deletions, produces a new {@link Snapshot} of the table, and commits
 * that snapshot as the current.
 *
 * <p>When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 */
public interface DeleteFiles extends SnapshotUpdate<DeleteFiles> {
  /**
   * Delete a file path from the underlying table.
   *
   * <p>To remove a file from the table, this path must equal a path in the table's metadata. Paths
   * that are different but equivalent will not be removed. For example, file:/path/file.avro is
   * equivalent to file:///path/file.avro, but would not remove the latter path from the table.
   *
   * @param path a fully-qualified file path to remove from the table
   * @return this for method chaining
   */
  DeleteFiles deleteFile(CharSequence path);

  /**
   * Delete a file tracked by a {@link DataFile} from the underlying table.
   *
   * @param file a DataFile to remove from the table
   * @return this for method chaining
   */
  default DeleteFiles deleteFile(DataFile file) {
    deleteFile(file.path());
    return this;
  }

  /**
   * Delete files that match an {@link Expression} on data rows from the table.
   *
   * <p>A file is selected to be deleted by the expression if it could contain any rows that match
   * the expression (candidate files are selected using an {@link
   * Projections#inclusive(PartitionSpec) inclusive projection}). These candidate files are deleted
   * if all of the rows in the file must match the expression (the partition data matches the
   * expression's {@link Projections#strict(PartitionSpec)} strict projection}). This guarantees
   * that files are deleted if and only if all rows in the file must match the expression.
   *
   * <p>Files that may contain some rows that match the expression and some rows that do not will
   * result in a {@link ValidationException}.
   *
   * @param expr an expression on rows in the table
   * @return this for method chaining
   * @throws ValidationException If a file can contain both rows that match and rows that do not
   */
  DeleteFiles deleteFromRowFilter(Expression expr);

  /**
   * Enables or disables case sensitive expression binding for methods that accept expressions.
   *
   * @param caseSensitive whether expression binding should be case sensitive
   * @return this for method chaining
   */
  DeleteFiles caseSensitive(boolean caseSensitive);

  /**
   * Enables validation that any files that are part of the deletion still exist when committing the
   * operation.
   *
   * @return this for method chaining
   */
  default DeleteFiles validateFilesExist() {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement validateFilesExist");
  }
}
