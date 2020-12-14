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

import org.apache.iceberg.CombinedScanTask;

/**
 * This essentially the mutable version of {@link IcebergSourceSplit}
 */
public class MutableIcebergSourceSplit {

  private final CombinedScanTask task;
  private long offset;
  private long recordsToSkipAfterOffset;

  public MutableIcebergSourceSplit(CombinedScanTask task, long offset, long recordsToSkipAfterOffset) {
    this.task = task;
    this.offset = offset;
    this.recordsToSkipAfterOffset = recordsToSkipAfterOffset;
  }

  public static MutableIcebergSourceSplit fromSplit(IcebergSourceSplit split) {
    if (split.checkpointedPosition() != null) {
      return new MutableIcebergSourceSplit(split.task(),
          split.checkpointedPosition().getOffset(),
          split.checkpointedPosition().getRecordsAfterOffset());
    } else {
      return new MutableIcebergSourceSplit(split.task(), 0L, 0L);
    }
  }

  public CombinedScanTask task() {
    return task;
  }

  public long offset() {
    return offset;
  }

  public long recordsToSkipAfterOffset() {
    return recordsToSkipAfterOffset;
  }

  public void updatePosition(long newOffset, long newRecordsToSkipAfterOffset) {
    this.offset = newOffset;
    this.recordsToSkipAfterOffset = newRecordsToSkipAfterOffset;
  }
}
