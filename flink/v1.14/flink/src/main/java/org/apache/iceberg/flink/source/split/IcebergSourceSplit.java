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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

@Internal
public class IcebergSourceSplit implements SourceSplit, Serializable {
  private static final long serialVersionUID = 1L;

  private final CombinedScanTask task;

  private int fileOffset;
  private long recordOffset;

  // The splits are frequently serialized into checkpoints.
  // Caching the byte representation makes repeated serialization cheap.
  @Nullable private transient byte[] serializedBytesCache;

  private IcebergSourceSplit(CombinedScanTask task, int fileOffset, long recordOffset) {
    this.task = task;
    this.fileOffset = fileOffset;
    this.recordOffset = recordOffset;
  }

  public static IcebergSourceSplit fromCombinedScanTask(CombinedScanTask combinedScanTask) {
    return fromCombinedScanTask(combinedScanTask, 0, 0L);
  }

  public static IcebergSourceSplit fromCombinedScanTask(
      CombinedScanTask combinedScanTask, int fileOffset, long recordOffset) {
    return new IcebergSourceSplit(combinedScanTask, fileOffset, recordOffset);
  }

  public CombinedScanTask task() {
    return task;
  }

  public int fileOffset() {
    return fileOffset;
  }

  public long recordOffset() {
    return recordOffset;
  }

  @Override
  public String splitId() {
    return MoreObjects.toStringHelper(this).add("files", toString(task.files())).toString();
  }

  public void updatePosition(int newFileOffset, long newRecordOffset) {
    // invalidate the cache after position change
    serializedBytesCache = null;
    fileOffset = newFileOffset;
    recordOffset = newRecordOffset;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("files", toString(task.files()))
        .add("fileOffset", fileOffset)
        .add("recordOffset", recordOffset)
        .toString();
  }

  private String toString(Collection<FileScanTask> files) {
    return Iterables.toString(
        files.stream()
            .map(
                fileScanTask ->
                    MoreObjects.toStringHelper(fileScanTask)
                        .add("file", fileScanTask.file().path().toString())
                        .add("start", fileScanTask.start())
                        .add("length", fileScanTask.length())
                        .toString())
            .collect(Collectors.toList()));
  }

  byte[] serializeV1() throws IOException {
    if (serializedBytesCache == null) {
      serializedBytesCache = InstantiationUtil.serializeObject(this);
    }
    return serializedBytesCache;
  }

  static IcebergSourceSplit deserializeV1(byte[] serialized) throws IOException {
    try {
      return InstantiationUtil.deserializeObject(
          serialized, IcebergSourceSplit.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize the split.", e);
    }
  }
}
