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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.SnapshotUtil;

public class BaseRowDelta extends MergingSnapshotProducer<RowDelta> implements RowDelta {
  private Long startingSnapshotId = null; // check all versions by default
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
  private final DataFileSet removedDataFiles = DataFileSet.create();
  private boolean validateDeletes = false;
  private Expression conflictDetectionFilter = Expressions.alwaysTrue();
  private boolean validateNewDataFiles = false;
  private boolean validateNewDeleteFiles = false;

  protected BaseRowDelta(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected BaseRowDelta self() {
    return this;
  }

  @Override
  protected String operation() {
    if (addsDeleteFiles() && !addsDataFiles()) {
      return DataOperations.DELETE;
    }

    return DataOperations.OVERWRITE;
  }

  @Override
  public RowDelta addRows(DataFile inserts) {
    add(inserts);
    return this;
  }

  @Override
  public RowDelta addDeletes(DeleteFile deletes) {
    add(deletes);
    return this;
  }

  @Override
  public RowDelta removeRows(DataFile file) {
    removedDataFiles.add(file);
    delete(file);
    return this;
  }

  @Override
  public RowDelta removeDeletes(DeleteFile deletes) {
    delete(deletes);
    return this;
  }

  @Override
  public RowDelta validateFromSnapshot(long snapshotId) {
    this.startingSnapshotId = snapshotId;
    return this;
  }

  @Override
  public RowDelta validateDeletedFiles() {
    this.validateDeletes = true;
    return this;
  }

  @Override
  public RowDelta validateDataFilesExist(Iterable<? extends CharSequence> referencedFiles) {
    referencedFiles.forEach(referencedDataFiles::add);
    return this;
  }

  @Override
  public RowDelta conflictDetectionFilter(Expression newConflictDetectionFilter) {
    Preconditions.checkArgument(
        newConflictDetectionFilter != null, "Conflict detection filter cannot be null");
    this.conflictDetectionFilter = newConflictDetectionFilter;
    return this;
  }

  @Override
  public RowDelta validateNoConflictingDataFiles() {
    this.validateNewDataFiles = true;
    return this;
  }

  @Override
  public RowDelta validateNoConflictingDeleteFiles() {
    this.validateNewDeleteFiles = true;
    return this;
  }

  @Override
  public RowDelta toBranch(String branch) {
    targetBranch(branch);
    return this;
  }

  @Override
  protected void validate(TableMetadata base, Snapshot parent) {
    if (parent != null) {
      if (startingSnapshotId != null) {
        Preconditions.checkArgument(
            SnapshotUtil.isAncestorOf(parent.snapshotId(), startingSnapshotId, base::snapshot),
            "Snapshot %s is not an ancestor of %s",
            startingSnapshotId,
            parent.snapshotId());
      }
      if (!referencedDataFiles.isEmpty()) {
        validateDataFilesExist(
            base,
            startingSnapshotId,
            referencedDataFiles,
            !validateDeletes,
            conflictDetectionFilter,
            parent);
      }

      if (validateDeletes) {
        failMissingDeletePaths();
      }

      if (validateNewDataFiles) {
        validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
      }

      if (validateNewDeleteFiles) {
        // validate that explicitly deleted files have not had added deletes
        if (!removedDataFiles.isEmpty()) {
          validateNoNewDeletesForDataFiles(
              base, startingSnapshotId, conflictDetectionFilter, removedDataFiles, parent);
        }

        // validate that previous deletes do not conflict with added deletes
        validateNoNewDeleteFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
      }

      validateNoConflictingFileAndPositionDeletes();

      validateAddedDVs(base, startingSnapshotId, conflictDetectionFilter, parent);
    }
  }

  /**
   * Validates that the data files removed in this commit do not overlap with data files with delete
   * files added
   */
  @SuppressWarnings("CollectionUndefinedEquality")
  private void validateNoConflictingFileAndPositionDeletes() {
    List<CharSequence> deletedFileWithNewDVs =
        removedDataFiles.stream()
            .map(DataFile::path)
            .filter(referencedDataFiles::contains)
            .collect(Collectors.toList());

    if (!deletedFileWithNewDVs.isEmpty()) {
      throw new ValidationException(
          "Cannot delete data files %s that are referenced by new delete files",
          deletedFileWithNewDVs);
    }
  }
}
