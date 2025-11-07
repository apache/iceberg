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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Adapter that wraps a TrackedFile and presents it as a DataFile.
 *
 * <p>This adapter allows TrackedFile instances to be used in contexts that expect DataFile. The
 * adapter returns null for partition data and column-level statistics until ContentStats is
 * implemented.
 *
 * <p>TODO: When ContentStats is implemented, use spec to extract partition from contentStats.
 */
class TrackedDataFileAdapter implements DataFile {
  private final TrackedFile<?> trackedFile;

  @SuppressWarnings("UnusedVariable")
  private final PartitionSpec spec;

  TrackedDataFileAdapter(TrackedFile<?> trackedFile, PartitionSpec spec) {
    if (trackedFile.contentType() != FileContent.DATA) {
      throw new IllegalStateException(
          "Cannot convert TrackedFile with content type "
              + trackedFile.contentType()
              + " to DataFile");
    }
    this.trackedFile = trackedFile;
    this.spec = spec;
  }

  @Override
  public String manifestLocation() {
    return trackedFile.manifestLocation();
  }

  @Override
  public Long pos() {
    return trackedFile.pos();
  }

  @Override
  public int specId() {
    return trackedFile.partitionSpecId();
  }

  @Override
  public FileContent content() {
    return trackedFile.contentType();
  }

  @Override
  public CharSequence path() {
    return trackedFile.location();
  }

  @Override
  public String location() {
    return trackedFile.location();
  }

  @Override
  public FileFormat format() {
    return trackedFile.fileFormat();
  }

  @Override
  public StructLike partition() {
    return null;
  }

  @Override
  public long recordCount() {
    return trackedFile.recordCount();
  }

  @Override
  public long fileSizeInBytes() {
    Long size = trackedFile.fileSizeInBytes();
    return size != null ? size : 0L;
  }

  @Override
  public Map<Integer, Long> columnSizes() {
    return null;
  }

  @Override
  public Map<Integer, Long> valueCounts() {
    return null;
  }

  @Override
  public Map<Integer, Long> nullValueCounts() {
    return null;
  }

  @Override
  public Map<Integer, Long> nanValueCounts() {
    return null;
  }

  @Override
  public Map<Integer, ByteBuffer> lowerBounds() {
    return null;
  }

  @Override
  public Map<Integer, ByteBuffer> upperBounds() {
    return null;
  }

  @Override
  public ByteBuffer keyMetadata() {
    return trackedFile.keyMetadata();
  }

  @Override
  public List<Long> splitOffsets() {
    return trackedFile.splitOffsets();
  }

  @Override
  public List<Integer> equalityFieldIds() {
    return null;
  }

  @Override
  public Integer sortOrderId() {
    return trackedFile.sortOrderId();
  }

  @Override
  public Long dataSequenceNumber() {
    TrackingInfo trackingInfo = trackedFile.trackingInfo();
    return trackingInfo != null ? trackingInfo.sequenceNumber() : null;
  }

  @Override
  public Long fileSequenceNumber() {
    TrackingInfo trackingInfo = trackedFile.trackingInfo();
    return trackingInfo != null ? trackingInfo.fileSequenceNumber() : null;
  }

  @Override
  public Long firstRowId() {
    TrackingInfo trackingInfo = trackedFile.trackingInfo();
    return trackingInfo != null ? trackingInfo.firstRowId() : null;
  }

  @Override
  public DataFile copy() {
    return new TrackedDataFileAdapter((TrackedFile<?>) trackedFile.copy(), spec);
  }

  @Override
  public DataFile copyWithoutStats() {
    return new TrackedDataFileAdapter((TrackedFile<?>) trackedFile.copyWithoutStats(), spec);
  }

  @Override
  public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
    return copyWithoutStats();
  }
}
