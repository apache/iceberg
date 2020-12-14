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

package org.apache.iceberg.flink.source.split;

import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public class IcebergSourceSplit extends FileSourceSplit {

  private final CombinedScanTask task;
  @Nullable
  private final CheckpointedPosition checkpointedPosition;

  /**
   * The splits are frequently serialized into checkpoints.
   * Caching the byte representation makes repeated serialization cheap.
   */
  @Nullable private transient byte[] serializedFormCache;

  public IcebergSourceSplit(CombinedScanTask task, CheckpointedPosition checkpointedPosition) {
    // Supply dummy values so that IcebergSourceSplit extend from FileSourceSplit,
    // as required by using BulkFormat interface in IcebergSource.
    // We are hoping to clean this up after FLINK-20174 is resolved.
    super("", new Path("file:///dummy"), 0L, 0L);
    this.task = task;
    this.checkpointedPosition = checkpointedPosition;
  }

  public static IcebergSourceSplit fromCombinedScanTask(CombinedScanTask combinedScanTask) {
    return new IcebergSourceSplit(combinedScanTask, null);
  }

  public static IcebergSourceSplit fromSplitState(MutableIcebergSourceSplit state) {
    return new IcebergSourceSplit(state.task(), new CheckpointedPosition(
        state.offset(), state.recordsToSkipAfterOffset()));
  }

  public CombinedScanTask task() {
    return task;
  }

  public CheckpointedPosition checkpointedPosition() {
    return checkpointedPosition;
  }

  public byte[] serializedFormCache() {
    return serializedFormCache;
  }

  public void serializedFormCache(byte[] cachedBytes) {
    this.serializedFormCache = cachedBytes;
  }

  @Override
  public String splitId() {
    return MoreObjects.toStringHelper(this)
        .add("files", toString(task.files()))
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IcebergSourceSplit split = (IcebergSourceSplit) o;
    return Objects.equal(splitId(), split.splitId()) &&
        Objects.equal(checkpointedPosition, split.checkpointedPosition());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(splitId());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("files", toString(task.files()))
        .add("checkpointedPosition", checkpointedPosition)
        .toString();
  }

  private String toString(Collection<FileScanTask> files) {
    return Iterables.toString(files.stream().map(fileScanTask ->
        MoreObjects.toStringHelper(fileScanTask)
            .add("file", fileScanTask.file() != null ?
                fileScanTask.file().path().toString() :
                "NoFile")
            .add("start", fileScanTask.start())
            .add("length", fileScanTask.length())
            .toString()).collect(Collectors.toList()));
  }
}
