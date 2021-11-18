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

package org.apache.iceberg.flink.sink;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.StructLikeWrapper;

class RewriteResult {

  private final Collection<StructLikeWrapper> partitions;
  private final long startingSnapshotSeqNum;
  private final long startingSnapshotId;
  private final Iterable<DataFile> addedDataFiles;
  private final Iterable<DataFile> deletedDataFiles;

  private RewriteResult(Collection<StructLikeWrapper> partitions,
                        long startingSnapshotSeqNum,
                        long startingSnapshotId,
                        Iterable<DataFile> addedDataFiles,
                        Iterable<DataFile> deletedDataFiles) {
    this.partitions = partitions;
    this.startingSnapshotSeqNum = startingSnapshotSeqNum;
    this.startingSnapshotId = startingSnapshotId;
    this.addedDataFiles = addedDataFiles;
    this.deletedDataFiles = deletedDataFiles;
  }

  Collection<StructLikeWrapper> partitions() {
    return partitions;
  }

  long startingSnapshotSeqNum() {
    return startingSnapshotSeqNum;
  }

  long startingSnapshotId() {
    return startingSnapshotId;
  }

  Iterable<DataFile> addedDataFiles() {
    return addedDataFiles;
  }

  Iterable<DataFile> deletedDataFiles() {
    return deletedDataFiles;
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private final Set<StructLikeWrapper> partitions;
    private long startingSnapshotSeqNum;
    private long startingSnapshotId;
    private final List<DataFile> addedFiles;
    private final List<DataFile> deletedFiles;

    private Builder() {
      partitions = Sets.newHashSet();
      startingSnapshotSeqNum = 0;
      startingSnapshotId = 0;
      addedFiles = Lists.newArrayList();
      deletedFiles = Lists.newArrayList();
    }

    Builder partition(StructLikeWrapper newPartition) {
      this.partitions.add(newPartition);
      return this;
    }

    Builder partitions(Collection<StructLikeWrapper> newPartitions) {
      newPartitions.forEach(this::partition);
      return this;
    }

    Builder startingSnapshotSeqNum(long newStartingSnapshotSeqNum) {
      this.startingSnapshotSeqNum = newStartingSnapshotSeqNum;
      return this;
    }

    Builder startingSnapshotId(long newStartingSnapshotId) {
      this.startingSnapshotId = newStartingSnapshotId;
      return this;
    }

    Builder addAddedDataFiles(Iterable<DataFile> dataFiles) {
      Iterables.addAll(addedFiles, dataFiles);
      return this;
    }

    Builder addDeletedDataFiles(Iterable<DataFile> dataFiles) {
      Iterables.addAll(deletedFiles, dataFiles);
      return this;
    }

    Builder add(RewriteResult result) {
      if (startingSnapshotSeqNum == 0 || result.startingSnapshotSeqNum() < startingSnapshotSeqNum) {
        startingSnapshotSeqNum = result.startingSnapshotSeqNum();
        startingSnapshotId = result.startingSnapshotId();
      }
      partitions(result.partitions());
      addAddedDataFiles(result.addedDataFiles());
      addDeletedDataFiles(result.deletedDataFiles());
      return this;
    }

    Builder addAll(Iterable<RewriteResult> results) {
      results.forEach(this::add);
      return this;
    }

    RewriteResult build() {
      return new RewriteResult(partitions, startingSnapshotSeqNum, startingSnapshotId, addedFiles, deletedFiles);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partitions", partitions)
        .add("startingSnapshotSeqNum", startingSnapshotSeqNum)
        .add("startingSnapshotId", startingSnapshotId)
        .add("numRewrittenFiles", Iterables.size(deletedDataFiles))
        .add("numAddedFiles", Iterables.size(addedDataFiles))
        .toString();
  }
}
