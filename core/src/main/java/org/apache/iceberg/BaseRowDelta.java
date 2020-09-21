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
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.SnapshotUtil;

class BaseRowDelta extends MergingSnapshotProducer<RowDelta> implements RowDelta {
  private final Set<CharSequence> referencedDataFiles = CharSequenceSet.empty();
  private Long validationSnapshotId = null; // check all versions by default

  BaseRowDelta(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected BaseRowDelta self() {
    return this;
  }

  @Override
  protected String operation() {
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
  public RowDelta validateFromSnapshot(long snapshotId) {
    this.validationSnapshotId = snapshotId;
    return this;
  }

  @Override
  public RowDelta validateDataFilesExist(Iterable<? extends CharSequence> referencedFiles) {
    referencedFiles.forEach(referencedDataFiles::add);
    return this;
  }

  @Override
  protected void validate(TableMetadata base) {
    if (referencedDataFiles.isEmpty()) {
      return;
    }

    Set<CharSequence> removedDataFiles = CharSequenceSet.empty();
    removedDataFiles(validationSnapshotId, base.currentSnapshot().snapshotId(), base::snapshot).stream()
        .map(DataFile::path)
        .forEach(removedDataFiles::add);
    Set<CharSequence> missingDataFiles = Sets.intersection(referencedDataFiles, removedDataFiles);

    ValidationException.check(missingDataFiles.isEmpty(),
        "Cannot commit deletes for missing data files: %s", removedDataFiles);
  }

  private static List<DataFile> removedDataFiles(Long baseSnapshotId, long latestSnapshotId,
                                                Function<Long, Snapshot> lookup) {
    List<DataFile> deletedFiles = Lists.newArrayList();

    Long currentSnapshotId = latestSnapshotId;
    while (currentSnapshotId != null && !currentSnapshotId.equals(baseSnapshotId)) {
      Snapshot currentSnapshot = lookup.apply(currentSnapshotId);

      if (currentSnapshot == null) {
        throw new ValidationException(
            "Cannot determine history between read snapshot %s and current %s",
            baseSnapshotId, currentSnapshotId);
      }

      if (!currentSnapshot.operation().equals(DataOperations.DELETE)) {
        Iterables.addAll(deletedFiles, currentSnapshot.deletedFiles());
      }

      currentSnapshotId = currentSnapshot.parentId();
    }

    return deletedFiles;
  }
}
