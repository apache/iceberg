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
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.DataFileSet;

class BaseRewriteFiles extends MergingSnapshotProducer<RewriteFiles> implements RewriteFiles {
  private final DataFileSet replacedDataFiles = DataFileSet.create();
  private final List<DataFile> pendingDataFiles = Lists.newArrayList();
  private Long dataFileSequenceNumber = null;
  private Long startingSnapshotId = null;

  BaseRewriteFiles(String tableName, TableOperations ops) {
    super(tableName, ops);

    // replace files must fail if any of the deleted paths is missing and cannot be deleted
    failMissingDeletePaths();
  }

  @Override
  protected RewriteFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.REPLACE;
  }

  @Override
  public RewriteFiles deleteFile(DataFile dataFile) {
    replacedDataFiles.add(dataFile);
    delete(dataFile);
    return self();
  }

  @Override
  public RewriteFiles deleteFile(DeleteFile deleteFile) {
    delete(deleteFile);
    return self();
  }

  @Override
  public RewriteFiles addFile(DataFile dataFile) {
    pendingDataFiles.add(dataFile);
    return self();
  }

  @Override
  public RewriteFiles addFile(DeleteFile deleteFile) {
    add(deleteFile);
    return self();
  }

  @Override
  public RewriteFiles addFile(DeleteFile deleteFile, long dataSequenceNumber) {
    add(deleteFile, dataSequenceNumber);
    return self();
  }

  @Override
  public RewriteFiles dataSequenceNumber(long sequenceNumber) {
    Preconditions.checkArgument(
        dataFileSequenceNumber == null || sequenceNumber == dataFileSequenceNumber,
        "Cannot change data sequence number for data files: %s != %s",
        dataFileSequenceNumber,
        sequenceNumber);
    this.dataFileSequenceNumber = sequenceNumber;
    return self();
  }

  @Override
  public RewriteFiles rewriteFiles(
      Set<DataFile> filesToDelete, Set<DataFile> filesToAdd, long sequenceNumber) {
    dataSequenceNumber(sequenceNumber);
    return rewriteFiles(filesToDelete, ImmutableSet.of(), filesToAdd, ImmutableSet.of());
  }

  @Override
  public RewriteFiles rewriteFiles(
      Set<DataFile> dataFilesToReplace,
      Set<DeleteFile> deleteFilesToReplace,
      Set<DataFile> dataFilesToAdd,
      Set<DeleteFile> deleteFilesToAdd) {

    Preconditions.checkNotNull(dataFilesToReplace, "Replaced data files can't be null");
    Preconditions.checkNotNull(deleteFilesToReplace, "Replaced delete files can't be null");
    Preconditions.checkNotNull(dataFilesToAdd, "Added data files can't be null");
    Preconditions.checkNotNull(deleteFilesToAdd, "Added delete files can't be null");

    for (DataFile dataFile : dataFilesToReplace) {
      deleteFile(dataFile);
    }

    for (DeleteFile deleteFile : deleteFilesToReplace) {
      deleteFile(deleteFile);
    }

    for (DataFile dataFile : dataFilesToAdd) {
      addFile(dataFile);
    }

    for (DeleteFile deleteFile : deleteFilesToAdd) {
      addFile(deleteFile);
    }

    return this;
  }

  @Override
  public RewriteFiles validateFromSnapshot(long snapshotId) {
    this.startingSnapshotId = snapshotId;
    return this;
  }

  @Override
  public BaseRewriteFiles toBranch(String branch) {
    targetBranch(branch);
    return this;
  }

  @Override
  public Snapshot apply() {
    // using the configured sequence number, add all pending data files
    if (dataFileSequenceNumber != null) {
      pendingDataFiles.forEach(file -> add(file, dataFileSequenceNumber));
    } else {
      pendingDataFiles.forEach(this::add);
    }

    pendingDataFiles.clear();

    // continue with the fully-configured commit
    return super.apply();
  }

  @Override
  protected void validate(TableMetadata base, Snapshot parent) {
    validateReplacedAndAddedFiles();
    if (!replacedDataFiles.isEmpty()) {
      // if there are replaced data files, there cannot be any new row-level deletes for those data
      // files
      validateNoNewDeletesForDataFiles(
          base, startingSnapshotId, replacedDataFiles, dataFileSequenceNumber, parent);
    }
  }

  private void validateReplacedAndAddedFiles() {
    Preconditions.checkArgument(
        deletesDataFiles() || deletesDeleteFiles(), "Files to delete cannot be empty");

    Preconditions.checkArgument(
        deletesDataFiles() || !addsDataFiles(),
        "Data files to add must be empty because there's no data file to be rewritten");

    Preconditions.checkArgument(
        deletesDeleteFiles() || !addsDeleteFiles(),
        "Delete files to add must be empty because there's no delete file to be rewritten");
  }
}
