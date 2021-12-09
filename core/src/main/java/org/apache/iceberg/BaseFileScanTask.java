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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableBaseFileScanTask.class)
@JsonDeserialize(as = ImmutableBaseFileScanTask.class)
abstract class BaseFileScanTask implements FileScanTask {

  public static BaseFileScanTask of(
      DataFile file, DeleteFile[] deletes, String schemaString, String specString, ResidualEvaluator residuals) {
    return ImmutableBaseFileScanTask.builder()
        .file(file)
        .deletes(Arrays.asList(deletes != null ? deletes : new DeleteFile[0]))
        .residuals(residuals)
        .spec(PartitionSpecParser.fromJson(SchemaParser.fromJson(schemaString), specString))
        .build();
  }

  @Override
  public abstract DataFile file();

  @Override
  @Value.Default
  public List<DeleteFile> deletes() {
    return Collections.emptyList();
  }

  public abstract ResidualEvaluator residuals();

  @Override
  public abstract PartitionSpec spec();

  @Override
  @Value.Default
  public long start() {
    return 0;
  }

  @Override
  @Value.Derived
  public long length() {
    return file().fileSizeInBytes();
  }

  @Override
  @Value.Derived
  public Expression residual() {
    return residuals().residualFor(file().partition());
  }

  @Override
  public Iterable<FileScanTask> split(long targetSplitSize) {
    if (file().format().isSplittable()) {
      if (file().splitOffsets() != null) {
        return () -> new OffsetsAwareTargetSplitSizeScanTaskIterator(file().splitOffsets(), this);
      } else {
        return () -> new FixedSizeSplitScanTaskIterator(targetSplitSize, this);
      }
    }
    return ImmutableList.of(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("file", file().path())
        .add("partition_data", file().partition())
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
      return SplitScanTask.of(offsets.get(offsetIdx), currentSize, parentScanTask);
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
      final FileScanTask splitTask = SplitScanTask.of(offset, len, fileScanTask);
      offset += len;
      remainingLen -= len;
      return splitTask;
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableSplitScanTask.class)
  @JsonDeserialize(as = ImmutableSplitScanTask.class)
  abstract static class SplitScanTask implements FileScanTask {

    static SplitScanTask of(long offset, long len, FileScanTask fileScanTask) {
      return ImmutableSplitScanTask.builder()
          .fileScanTask(fileScanTask)
          .file(fileScanTask.file())
          .deletes(fileScanTask.deletes())
          .spec(fileScanTask.spec())
          .residual(fileScanTask.residual())
          .length(len)
          .start(offset)
          .build();
    }

    public abstract FileScanTask fileScanTask();

    @Override
    @Nullable
    public abstract DataFile file();

    @Override
    @Nullable
    public abstract List<DeleteFile> deletes();

    @Override
    @Nullable
    public abstract PartitionSpec spec();

    @Override
    @Nullable
    public abstract Expression residual();

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      throw new UnsupportedOperationException("Cannot split a task which is already split");
    }

    public boolean isAdjacent(SplitScanTask other) {
      return (other != null) &&
          Objects.equals(this.file(), other.file()) &&
          (this.start() + this.length() == other.start());
    }
  }

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
            lastSplit = SplitScanTask.of(
                lastSplit.start(),
                lastSplit.length() + split.length(),
                lastSplit.fileScanTask());
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
