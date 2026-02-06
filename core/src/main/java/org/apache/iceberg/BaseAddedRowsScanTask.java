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

class BaseAddedRowsScanTask extends BaseChangelogContentScanTask<AddedRowsScanTask, DataFile>
    implements AddedRowsScanTask {

  private final DeleteFile[] deletes;

  BaseAddedRowsScanTask(
      int changeOrdinal,
      long commitSnapshotId,
      DataFile file,
      DeleteFile[] deletes,
      String schemaString,
      String specString,
      ResidualEvaluator residuals) {
    super(changeOrdinal, commitSnapshotId, file, schemaString, specString, residuals);
    this.deletes = deletes != null ? deletes : new DeleteFile[0];
  }

  @Override
  protected AddedRowsScanTask self() {
    return this;
  }

  @Override
  protected AddedRowsScanTask newSplitTask(AddedRowsScanTask parentTask, long offset, long length) {
    return new SplitAddedRowsScanTask(parentTask, offset, length);
  }

  @Override
  public List<DeleteFile> deletes() {
    return ImmutableList.copyOf(deletes);
  }

  private static class SplitAddedRowsScanTask
      extends SplitScanTask<SplitAddedRowsScanTask, AddedRowsScanTask, DataFile>
      implements AddedRowsScanTask {

    SplitAddedRowsScanTask(AddedRowsScanTask parentTask, long offset, long length) {
      super(parentTask, offset, length);
    }

    @Override
    protected SplitAddedRowsScanTask copyWithNewLength(long newLength) {
      return new SplitAddedRowsScanTask(parentTask(), start(), newLength);
    }

    @Override
    public List<DeleteFile> deletes() {
      return parentTask().deletes();
    }
  }
}
