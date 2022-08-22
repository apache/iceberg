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

import java.util.*;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class BaseFileScanTask implements FileScanTask {
  private final DataFile file;
  private final DeleteFile[] deletes;
  private final String schemaString;
  private final String specString;
  private final ResidualEvaluator residuals;

  private transient PartitionSpec spec = null;

  BaseFileScanTask(DataFile file, DeleteFile[] deletes, String schemaString, String specString,
                   ResidualEvaluator residuals) {
    this.file = file;
    this.deletes = deletes != null ? deletes : new DeleteFile[0];
    this.schemaString = schemaString;
    this.specString = specString;
    this.residuals = residuals;
  }

  @Override
  public DataFile file() {
    return file;
  }

  @Override
  public List<DeleteFile> deletes() {
    return ImmutableList.copyOf(deletes);
  }

  @Override
  public PartitionSpec spec() {
    if (spec == null) {
      this.spec = PartitionSpecParser.fromJson(SchemaParser.fromJson(schemaString), specString);
    }
    return spec;
  }

  @Override
  public long start() {
    return 0;
  }

  @Override
  public long length() {
    return Optional.ofNullable(file).isPresent() ? file.fileSizeInBytes() : deletes[0].fileSizeInBytes();
  }

  @Override
  public Expression residual() {
    return Optional.ofNullable(file).isPresent() ? residuals.residualFor(file.partition()) : null;
  }

  @Override
  public Iterable<FileScanTask> split(long targetSplitSize) {
    if (Optional.ofNullable(file).isPresent() && file.format().isSplittable()) {
      if (file.splitOffsets() != null) {
        return () -> new OffsetsAwareTargetSplitSizeScanTaskIterator(file.splitOffsets(), this);
      } else {
        return () -> new FixedSizeSplitScanTaskIterator(targetSplitSize, this);
      }
    }
    return ImmutableList.of(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("file", file != null ? file.path() : "")
        .add("partition_data",file != null ? file.partition() : "")
        .add("residual", residual())
        .toString();
  }

  /**
   * This iterator returns {@link FileScanTask} using guidance provided by split offsets.
   */
  @VisibleForTesting
  static final class OffsetsAwareTargetSplitSizeScanTaskIterator implements Iterator<FileScanTask> {
    private final List<Long> offsets;
    private final List<Long> splitSizes;
    private final FileScanTask parentScanTask;
    private int sizeIdx = 0;

    OffsetsAwareTargetSplitSizeScanTaskIterator(List<Long> offsetList, FileScanTask parentScanTask) {
      this.offsets = ImmutableList.copyOf(offsetList);
      this.parentScanTask = parentScanTask;
      this.splitSizes = Lists.newArrayListWithCapacity(offsets.size());
      if (offsets.size() > 0) {
        int lastIndex = offsets.size() - 1;
        for (int index = 0; index < lastIndex; index++) {
          splitSizes.add(offsets.get(index + 1) - offsets.get(index));
        }
        splitSizes.add(parentScanTask.length() - offsets.get(lastIndex));
      }
    }

    @Override
    public boolean hasNext() {
      return sizeIdx < splitSizes.size();
    }

    @Override
    public FileScanTask next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      int offsetIdx = sizeIdx;
      long currentSize = splitSizes.get(sizeIdx);
      sizeIdx += 1; // Create 1 split per offset
      FileScanTask combinedTask = new SplitScanTask(offsets.get(offsetIdx), currentSize, parentScanTask);
      return combinedTask;
    }

  }

  @VisibleForTesting
  static final class FixedSizeSplitScanTaskIterator implements Iterator<FileScanTask> {
    private long offset;
    private long remainingLen;
    private long splitSize;
    private final FileScanTask fileScanTask;

    FixedSizeSplitScanTaskIterator(long splitSize, FileScanTask fileScanTask) {
      this.offset = 0;
      this.remainingLen = fileScanTask.length();
      this.splitSize = splitSize;
      this.fileScanTask = fileScanTask;
    }

    @Override
    public boolean hasNext() {
      return remainingLen > 0;
    }

    @Override
    public FileScanTask next() {
      long len = Math.min(splitSize, remainingLen);
      final FileScanTask splitTask = new SplitScanTask(offset, len, fileScanTask);
      offset += len;
      remainingLen -= len;
      return splitTask;
    }
  }

  private static final class SplitScanTask implements FileScanTask {
    private final long len;
    private final long offset;
    private final FileScanTask fileScanTask;

    SplitScanTask(long offset, long len, FileScanTask fileScanTask) {
      this.offset = offset;
      this.len = len;
      this.fileScanTask = fileScanTask;
    }

    @Override
    public DataFile file() {
      return fileScanTask.file();
    }

    @Override
    public List<DeleteFile> deletes() {
      return fileScanTask.deletes();
    }

    @Override
    public PartitionSpec spec() {
      return fileScanTask.spec();
    }

    @Override
    public long start() {
      return offset;
    }

    @Override
    public long length() {
      return len;
    }

    @Override
    public Expression residual() {
      return fileScanTask.residual();
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      throw new UnsupportedOperationException("Cannot split a task which is already split");
    }

    public boolean isAdjacent(SplitScanTask other) {
      return (other != null) &&
          this.file().equals(other.file()) &&
          (this.offset + this.len == other.offset);
    }
  }

  @SuppressWarnings("MixedMutabilityReturnType")
  static List<FileScanTask> combineAdjacentTasks(List<FileScanTask> tasks) {
    if (tasks.isEmpty()) {
      return Collections.emptyList();
    }

    List<FileScanTask> combinedScans = Lists.newArrayList();
    SplitScanTask lastSplit = null;

    for (FileScanTask fileScanTask : tasks) {
      if (!(fileScanTask instanceof SplitScanTask)) {
        // Return any tasks not produced by split un-modified
        combinedScans.add(fileScanTask);
      } else {
        SplitScanTask split = (SplitScanTask) fileScanTask;
        if (lastSplit != null) {
          if (lastSplit.isAdjacent(split)) {
            // Merge with the last split
            lastSplit = new SplitScanTask(
                lastSplit.offset,
                lastSplit.len + split.len,
                lastSplit.fileScanTask);
          } else {
            // Last split is not adjacent, add it to finished adjacent groups
            combinedScans.add(lastSplit);
            lastSplit = split;
          }
        } else {
          // First split
          lastSplit = split;
        }
      }
    }

    if (lastSplit != null) {
      combinedScans.add(lastSplit);
    }

    return combinedScans;
  }
}
