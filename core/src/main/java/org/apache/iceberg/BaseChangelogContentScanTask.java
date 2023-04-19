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

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

abstract class BaseChangelogContentScanTask<
        ThisT extends ContentScanTask<F> & ChangelogScanTask, F extends ContentFile<F>>
    extends BaseContentScanTask<ThisT, F> implements ChangelogScanTask {

  private final int changeOrdinal;
  private final long commitSnapshotId;

  BaseChangelogContentScanTask(
      int changeOrdinal,
      long commitSnapshotId,
      F file,
      String schemaString,
      String specString,
      ResidualEvaluator residuals) {
    super(file, schemaString, specString, residuals);
    this.changeOrdinal = changeOrdinal;
    this.commitSnapshotId = commitSnapshotId;
  }

  @Override
  public int changeOrdinal() {
    return changeOrdinal;
  }

  @Override
  public long commitSnapshotId() {
    return commitSnapshotId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("change_ordinal", changeOrdinal)
        .add("commit_snapshot_id", commitSnapshotId)
        .add("file", file().path())
        .add("partition_data", file().partition())
        .add("residual", residual())
        .toString();
  }

  abstract static class SplitScanTask<
          ThisT, ParentT extends ContentScanTask<F> & ChangelogScanTask, F extends ContentFile<F>>
      implements ContentScanTask<F>, ChangelogScanTask, MergeableScanTask<ThisT> {

    private final ParentT parentTask;
    private final long offset;
    private final long length;

    protected SplitScanTask(ParentT parentTask, long offset, long length) {
      this.parentTask = parentTask;
      this.offset = offset;
      this.length = length;
    }

    protected abstract ThisT copyWithNewLength(long newLength);

    protected ParentT parentTask() {
      return parentTask;
    }

    @Override
    public int changeOrdinal() {
      return parentTask.changeOrdinal();
    }

    @Override
    public long commitSnapshotId() {
      return parentTask.commitSnapshotId();
    }

    @Override
    public F file() {
      return parentTask.file();
    }

    @Override
    public PartitionSpec spec() {
      return parentTask.spec();
    }

    @Override
    public long start() {
      return offset;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public Expression residual() {
      return parentTask.residual();
    }

    @Override
    public boolean canMerge(ScanTask other) {
      if (getClass().equals(other.getClass())) {
        SplitScanTask<?, ?, ?> that = (SplitScanTask<?, ?, ?>) other;
        return changeOrdinal() == that.changeOrdinal()
            && commitSnapshotId() == that.commitSnapshotId()
            && file().equals(that.file())
            && start() + length() == that.start();

      } else {
        return false;
      }
    }

    @Override
    public ThisT merge(ScanTask other) {
      SplitScanTask<?, ?, ?> that = (SplitScanTask<?, ?, ?>) other;
      return copyWithNewLength(length() + that.length());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("change_ordinal", changeOrdinal())
          .add("commit_snapshot_id", commitSnapshotId())
          .add("file", file().path())
          .add("partition_data", file().partition())
          .add("offset", offset)
          .add("length", length)
          .add("residual", residual())
          .toString();
    }
  }
}
