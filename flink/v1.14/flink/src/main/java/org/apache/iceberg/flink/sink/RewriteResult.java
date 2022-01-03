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
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

class RewriteResult {

  private final long startingSnapshotId;
  private final Collection<StructLike> partitions;
  private final Set<DataFile> addedDataFiles;
  private final Set<DataFile> rewrittenDataFiles;

  private RewriteResult(long startingSnapshotId,
                        Collection<StructLike> partitions,
                        Set<DataFile> addedDataFiles,
                        Set<DataFile> rewrittenDataFiles) {
    this.startingSnapshotId = startingSnapshotId;
    this.partitions = partitions;
    this.addedDataFiles = addedDataFiles;
    this.rewrittenDataFiles = rewrittenDataFiles;
  }

  long startingSnapshotId() {
    return startingSnapshotId;
  }

  Collection<StructLike> partitions() {
    return partitions;
  }

  Set<DataFile> addedDataFiles() {
    return addedDataFiles;
  }

  Set<DataFile> rewrittenDataFiles() {
    return rewrittenDataFiles;
  }

  static Builder builder(long startingSnapshotId) {
    return new Builder(startingSnapshotId);
  }

  static class Builder {
    private final long startingSnapshotId;
    private final Set<StructLike> partitions;
    private final Set<DataFile> addedFiles;
    private final Set<DataFile> rewrittenDataFiles;

    private Builder(long startingSnapshotId) {
      this.startingSnapshotId = startingSnapshotId;
      this.partitions = Sets.newHashSet();
      this.addedFiles = Sets.newHashSet();
      this.rewrittenDataFiles = Sets.newHashSet();
    }

    Builder partition(StructLike newPartition) {
      this.partitions.add(newPartition);
      return this;
    }

    Builder partitions(Collection<StructLike> newPartitions) {
      this.partitions.addAll(newPartitions);
      return this;
    }

    Builder addAddedDataFiles(Iterable<DataFile> dataFiles) {
      Iterables.addAll(addedFiles, dataFiles);
      return this;
    }

    Builder addRewrittenDataFiles(Iterable<DataFile> dataFiles) {
      Iterables.addAll(rewrittenDataFiles, dataFiles);
      return this;
    }

    RewriteResult build() {
      return new RewriteResult(startingSnapshotId, partitions, addedFiles, rewrittenDataFiles);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partitions", partitions)
        .add("startingSnapshotId", startingSnapshotId)
        .add("numRewrittenFiles", Iterables.size(rewrittenDataFiles))
        .add("numAddedFiles", Iterables.size(addedDataFiles))
        .toString();
  }
}
