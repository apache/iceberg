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
 * Adapter that wraps a TrackedFile and presents it as a DeleteFile.
 *
 * <p>This adapter allows TrackedFile instances to be used in contexts that expect DeleteFile. The
 * adapter returns null for partition data and column-level statistics, as these are not stored in
 * TrackedFile.
 */
class TrackedDeleteFileAdapter implements DeleteFile {
  private final TrackedFile<?> trackedFile;

  TrackedDeleteFileAdapter(TrackedFile<?> trackedFile) {
    FileContent contentType = trackedFile.contentType();
    if (contentType != FileContent.POSITION_DELETES
        && contentType != FileContent.EQUALITY_DELETES) {
      throw new IllegalStateException(
          "Cannot convert TrackedFile with content type " + contentType + " to DeleteFile");
    }
    this.trackedFile = trackedFile;
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
    return trackedFile.equalityIds();
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
  public String referencedDataFile() {
    return trackedFile.referencedFile();
  }

  @Override
  public Long contentOffset() {
    DeletionVector dv = trackedFile.deletionVector();
    return dv != null ? dv.offset() : null;
  }

  @Override
  public Long contentSizeInBytes() {
    DeletionVector dv = trackedFile.deletionVector();
    return dv != null ? dv.sizeInBytes() : null;
  }

  @Override
  public DeleteFile copy() {
    return new TrackedDeleteFileAdapter((TrackedFile<?>) trackedFile.copy());
  }

  @Override
  public DeleteFile copyWithoutStats() {
    return new TrackedDeleteFileAdapter((TrackedFile<?>) trackedFile.copyWithoutStats());
  }

  @Override
  public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
    return copyWithoutStats();
  }
}
