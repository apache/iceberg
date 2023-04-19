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
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

class BaseDeletedRowsScanTask extends BaseChangelogContentScanTask<DeletedRowsScanTask, DataFile>
    implements DeletedRowsScanTask {

  private final DeleteFile[] addedDeletes;
  private final DeleteFile[] existingDeletes;

  BaseDeletedRowsScanTask(
      int changeOrdinal,
      long commitSnapshotId,
      DataFile file,
      DeleteFile[] addedDeletes,
      DeleteFile[] existingDeletes,
      String schemaString,
      String specString,
      ResidualEvaluator residuals) {
    super(changeOrdinal, commitSnapshotId, file, schemaString, specString, residuals);
    this.addedDeletes = addedDeletes != null ? addedDeletes : new DeleteFile[0];
    this.existingDeletes = existingDeletes != null ? existingDeletes : new DeleteFile[0];
  }

  @Override
  protected DeletedRowsScanTask self() {
    return this;
  }

  @Override
  protected DeletedRowsScanTask newSplitTask(
      DeletedRowsScanTask parentTask, long offset, long length) {
    return new SplitDeletedRowsScanTask(parentTask, offset, length);
  }

  @Override
  public List<DeleteFile> addedDeletes() {
    return ImmutableList.copyOf(addedDeletes);
  }

  @Override
  public List<DeleteFile> existingDeletes() {
    return ImmutableList.copyOf(existingDeletes);
  }

  private static class SplitDeletedRowsScanTask
      extends SplitScanTask<SplitDeletedRowsScanTask, DeletedRowsScanTask, DataFile>
      implements DeletedRowsScanTask {

    SplitDeletedRowsScanTask(DeletedRowsScanTask parentTask, long offset, long length) {
      super(parentTask, offset, length);
    }

    @Override
    protected SplitDeletedRowsScanTask copyWithNewLength(long newLength) {
      return new SplitDeletedRowsScanTask(parentTask(), start(), newLength);
    }

    @Override
    public List<DeleteFile> addedDeletes() {
      return parentTask().addedDeletes();
    }

    @Override
    public List<DeleteFile> existingDeletes() {
      return parentTask().existingDeletes();
    }
  }
}
