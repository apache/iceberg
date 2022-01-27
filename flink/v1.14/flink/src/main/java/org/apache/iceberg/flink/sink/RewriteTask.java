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

import java.io.Serializable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

class RewriteTask implements Serializable {

  private final long snapshotId;
  private final StructLike partition;
  private final CombinedScanTask task;
  private final long totalFilesCount;
  private final long totalSizeInBytes;

  RewriteTask(long snapshotId, StructLike partition, CombinedScanTask task) {
    this.snapshotId = snapshotId;
    this.partition = partition;
    this.task = task;
    this.totalFilesCount = task.files().size();
    this.totalSizeInBytes = task.files().stream().mapToLong(FileScanTask::length).sum();
  }

  long snapshotId() {
    return snapshotId;
  }

  StructLike partition() {
    return partition;
  }

  CombinedScanTask task() {
    return task;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshotId", snapshotId)
        .add("partition", partition)
        .add("totalFilesCount", totalFilesCount)
        .add("totalSizeInBytes", totalSizeInBytes)
        .toString();
  }
}
