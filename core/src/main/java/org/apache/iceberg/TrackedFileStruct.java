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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.stats.BaseContentStats;
import org.apache.iceberg.stats.ContentStats;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ByteBuffers;

/** Mutable {@link StructLike} implementation of {@link TrackedFile}. */
class TrackedFileStruct implements TrackedFile, StructLike {
  private static final FileContent[] FILE_CONTENT_VALUES = FileContent.values();
  private static final EntryStatus[] STATUS_VALUES = EntryStatus.values();

  private TrackingStruct tracking;
  private FileContent contentType = FileContent.DATA;
  private String location;
  private FileFormat fileFormat;
  private long recordCount;
  private long fileSizeInBytes;
  private Integer specId;
  private ContentStats contentStats;
  private Integer sortOrderId;
  private DeletionVectorStruct deletionVector;
  private ManifestInfoStruct manifestInfo;
  private ByteBuffer keyMetadata;
  private long[] splitOffsets;
  private int[] equalityIds;
  private String manifestLocation;
  private long manifestPos;

  TrackedFileStruct(Types.StructType type) {}

  /** Copy constructor. */
  private TrackedFileStruct(TrackedFileStruct toCopy, boolean withStats, Set<Integer> statsIds) {
    this.tracking = toCopy.tracking != null ? toCopy.tracking.copy() : null;
    this.contentType = toCopy.contentType;
    this.location = toCopy.location;
    this.fileFormat = toCopy.fileFormat;
    this.recordCount = toCopy.recordCount;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
    this.specId = toCopy.specId;
    this.sortOrderId = toCopy.sortOrderId;
    this.deletionVector = toCopy.deletionVector != null ? toCopy.deletionVector.copy() : null;

    if (withStats && toCopy.contentStats != null) {
      ContentStats filtered = BaseContentStats.buildFrom(toCopy.contentStats, statsIds).build();
      this.contentStats = filtered.fieldStats().isEmpty() ? null : filtered;
    } else {
      this.contentStats = null;
    }

    this.manifestInfo = toCopy.manifestInfo != null ? toCopy.manifestInfo.copy() : null;
    this.keyMetadata = toCopy.keyMetadata != null ? ByteBuffers.copy(toCopy.keyMetadata) : null;
    this.splitOffsets =
        toCopy.splitOffsets != null
            ? Arrays.copyOf(toCopy.splitOffsets, toCopy.splitOffsets.length)
            : null;
    this.equalityIds =
        toCopy.equalityIds != null
            ? Arrays.copyOf(toCopy.equalityIds, toCopy.equalityIds.length)
            : null;
    this.manifestLocation = toCopy.manifestLocation;
    this.manifestPos = toCopy.manifestPos;
  }

  @Override
  public Tracking tracking() {
    return tracking;
  }

  TrackingStruct trackingStruct() {
    return tracking;
  }

  @Override
  public FileContent contentType() {
    return contentType;
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public FileFormat fileFormat() {
    return fileFormat;
  }

  @Override
  public long recordCount() {
    return recordCount;
  }

  @Override
  public long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  @Override
  public Integer specId() {
    return specId;
  }

  @Override
  public ContentStats contentStats() {
    return (ContentStats) contentStats;
  }

  @Override
  public Integer sortOrderId() {
    return sortOrderId;
  }

  @Override
  public DeletionVector deletionVector() {
    return deletionVector;
  }

  @Override
  public ManifestInfo manifestInfo() {
    return manifestInfo;
  }

  @Override
  public ByteBuffer keyMetadata() {
    return keyMetadata;
  }

  @Override
  public List<Long> splitOffsets() {
    return splitOffsets != null ? ArrayUtil.toUnmodifiableLongList(splitOffsets) : null;
  }

  @Override
  public List<Integer> equalityIds() {
    return equalityIds != null ? ArrayUtil.toUnmodifiableIntList(equalityIds) : null;
  }

  @Override
  public TrackedFile copy() {
    return new TrackedFileStruct(this, true, null);
  }

  @Override
  public TrackedFile copyWithStats(Set<Integer> requestedColumnIds) {
    return new TrackedFileStruct(this, true, requestedColumnIds);
  }

  @Override
  public String manifestLocation() {
    return manifestLocation;
  }

  @Override
  public long manifestPos() {
    return manifestPos;
  }

  void setManifestLocation(String newManifestLocation) {
    this.manifestLocation = newManifestLocation;
  }

  void setManifestPos(long newManifestPos) {
    this.manifestPos = newManifestPos;
  }

  // StructLike implementation - field ordinals match TrackedFile.schemaWithContentStats() order

  @Override
  public int size() {
    return 14;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int pos, Class<T> javaClass) {
    Object value;
    switch (pos) {
      case 0:
        value = tracking;
        break;
      case 1:
        value = contentType.id();
        break;
      case 2:
        value = location;
        break;
      case 3:
        value = fileFormat != null ? fileFormat.toString() : null;
        break;
      case 4:
        value = recordCount;
        break;
      case 5:
        value = fileSizeInBytes;
        break;
      case 6:
        value = specId;
        break;
      case 7:
        value = contentStats;
        break;
      case 8:
        value = sortOrderId;
        break;
      case 9:
        value = deletionVector;
        break;
      case 10:
        value = manifestInfo;
        break;
      case 11:
        value = keyMetadata;
        break;
      case 12:
        value = splitOffsets();
        break;
      case 13:
        value = equalityIds();
        break;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }

    return (T) value;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void set(int pos, T value) {
    switch (pos) {
      case 0:
        this.tracking = (TrackingStruct) value;
        break;
      case 1:
        this.contentType = FILE_CONTENT_VALUES[(Integer) value];
        break;
      case 2:
        this.location = (String) value;
        break;
      case 3:
        this.fileFormat = FileFormat.fromString(value.toString());
        break;
      case 4:
        this.recordCount = (Long) value;
        break;
      case 5:
        this.fileSizeInBytes = (Long) value;
        break;
      case 6:
        this.specId = (Integer) value;
        break;
      case 7:
        Preconditions.checkArgument(
            value == null || value instanceof ContentStats,
            "Expected ContentStats but found: %s",
            value == null ? "null" : value.getClass().getName());

        this.contentStats = (ContentStats) value;
        break;
      case 8:
        this.sortOrderId = (Integer) value;
        break;
      case 9:
        this.deletionVector = (DeletionVectorStruct) value;
        break;
      case 10:
        this.manifestInfo = (ManifestInfoStruct) value;
        break;
      case 11:
        this.keyMetadata = (ByteBuffer) value;
        break;
      case 12:
        this.splitOffsets = ArrayUtil.toLongArray((List<Long>) value);
        break;
      case 13:
        this.equalityIds = ArrayUtil.toIntArray((List<Integer>) value);
        break;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  /** Mutable {@link StructLike} implementation of {@link Tracking}. */
  static class TrackingStruct implements Tracking, StructLike {
    private EntryStatus status = EntryStatus.EXISTING;
    private Long snapshotId;
    private Long sequenceNumber;
    private Long fileSequenceNumber;
    private Long dvSnapshotId;
    private Long firstRowId;
    private ByteBuffer deletedPositions;
    private ByteBuffer replacedPositions;

    TrackingStruct(Types.StructType type) {}

    private TrackingStruct(TrackingStruct toCopy) {
      this.status = toCopy.status;
      this.snapshotId = toCopy.snapshotId;
      this.sequenceNumber = toCopy.sequenceNumber;
      this.fileSequenceNumber = toCopy.fileSequenceNumber;
      this.dvSnapshotId = toCopy.dvSnapshotId;
      this.firstRowId = toCopy.firstRowId;
      this.deletedPositions =
          toCopy.deletedPositions != null ? ByteBuffers.copy(toCopy.deletedPositions) : null;
      this.replacedPositions =
          toCopy.replacedPositions != null ? ByteBuffers.copy(toCopy.replacedPositions) : null;
    }

    TrackingStruct copy() {
      return new TrackingStruct(this);
    }

    @Override
    public EntryStatus status() {
      return status;
    }

    @Override
    public Long snapshotId() {
      return snapshotId;
    }

    @Override
    public Long dataSequenceNumber() {
      return sequenceNumber;
    }

    @Override
    public Long fileSequenceNumber() {
      return fileSequenceNumber;
    }

    @Override
    public Long dvSnapshotId() {
      return dvSnapshotId;
    }

    @Override
    public Long firstRowId() {
      return firstRowId;
    }

    @Override
    public ByteBuffer deletedPositions() {
      return deletedPositions;
    }

    @Override
    public ByteBuffer replacedPositions() {
      return replacedPositions;
    }

    void setSnapshotId(Long newSnapshotId) {
      this.snapshotId = newSnapshotId;
    }

    void setSequenceNumber(Long newSequenceNumber) {
      this.sequenceNumber = newSequenceNumber;
    }

    void setFirstRowId(Long newFirstRowId) {
      this.firstRowId = newFirstRowId;
    }

    @Override
    public int size() {
      return 8;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      Object value;
      switch (pos) {
        case 0:
          value = status.id();
          break;
        case 1:
          value = snapshotId;
          break;
        case 2:
          value = sequenceNumber;
          break;
        case 3:
          value = fileSequenceNumber;
          break;
        case 4:
          value = dvSnapshotId;
          break;
        case 5:
          value = firstRowId;
          break;
        case 6:
          value = deletedPositions;
          break;
        case 7:
          value = replacedPositions;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }

      return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void set(int pos, T value) {
      switch (pos) {
        case 0:
          this.status = STATUS_VALUES[(Integer) value];
          break;
        case 1:
          this.snapshotId = (Long) value;
          break;
        case 2:
          this.sequenceNumber = (Long) value;
          break;
        case 3:
          this.fileSequenceNumber = (Long) value;
          break;
        case 4:
          this.dvSnapshotId = (Long) value;
          break;
        case 5:
          this.firstRowId = (Long) value;
          break;
        case 6:
          this.deletedPositions = (ByteBuffer) value;
          break;
        case 7:
          this.replacedPositions = (ByteBuffer) value;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }
  }

  /** Mutable {@link StructLike} implementation of {@link DeletionVector}. */
  static class DeletionVectorStruct implements DeletionVector, StructLike {
    private String location;
    private long offset;
    private long sizeInBytes;
    private long cardinality;

    DeletionVectorStruct(Types.StructType type) {}

    private DeletionVectorStruct(DeletionVectorStruct toCopy) {
      this.location = toCopy.location;
      this.offset = toCopy.offset;
      this.sizeInBytes = toCopy.sizeInBytes;
      this.cardinality = toCopy.cardinality;
    }

    DeletionVectorStruct copy() {
      return new DeletionVectorStruct(this);
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public long offset() {
      return offset;
    }

    @Override
    public long sizeInBytes() {
      return sizeInBytes;
    }

    @Override
    public long cardinality() {
      return cardinality;
    }

    @Override
    public int size() {
      return 4;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      Object value;
      switch (pos) {
        case 0:
          value = location;
          break;
        case 1:
          value = offset;
          break;
        case 2:
          value = sizeInBytes;
          break;
        case 3:
          value = cardinality;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }

      return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void set(int pos, T value) {
      switch (pos) {
        case 0:
          this.location = (String) value;
          break;
        case 1:
          this.offset = (Long) value;
          break;
        case 2:
          this.sizeInBytes = (Long) value;
          break;
        case 3:
          this.cardinality = (Long) value;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }
  }

  /** Mutable {@link StructLike} implementation of {@link ManifestInfo}. */
  static class ManifestInfoStruct implements ManifestInfo, StructLike {
    private int addedFilesCount;
    private int existingFilesCount;
    private int deletedFilesCount;
    private int replacedFilesCount;
    private long addedRowsCount;
    private long existingRowsCount;
    private long deletedRowsCount;
    private long replacedRowsCount;
    private long minSequenceNumber;
    private ByteBuffer dv;
    private Long dvCardinality;

    ManifestInfoStruct(Types.StructType type) {}

    private ManifestInfoStruct(ManifestInfoStruct toCopy) {
      this.addedFilesCount = toCopy.addedFilesCount;
      this.existingFilesCount = toCopy.existingFilesCount;
      this.deletedFilesCount = toCopy.deletedFilesCount;
      this.replacedFilesCount = toCopy.replacedFilesCount;
      this.addedRowsCount = toCopy.addedRowsCount;
      this.existingRowsCount = toCopy.existingRowsCount;
      this.deletedRowsCount = toCopy.deletedRowsCount;
      this.replacedRowsCount = toCopy.replacedRowsCount;
      this.minSequenceNumber = toCopy.minSequenceNumber;
      this.dv = toCopy.dv != null ? ByteBuffers.copy(toCopy.dv) : null;
      this.dvCardinality = toCopy.dvCardinality;
    }

    ManifestInfoStruct copy() {
      return new ManifestInfoStruct(this);
    }

    @Override
    public int addedFilesCount() {
      return addedFilesCount;
    }

    @Override
    public int existingFilesCount() {
      return existingFilesCount;
    }

    @Override
    public int deletedFilesCount() {
      return deletedFilesCount;
    }

    @Override
    public int replacedFilesCount() {
      return replacedFilesCount;
    }

    @Override
    public long addedRowsCount() {
      return addedRowsCount;
    }

    @Override
    public long existingRowsCount() {
      return existingRowsCount;
    }

    @Override
    public long deletedRowsCount() {
      return deletedRowsCount;
    }

    @Override
    public long replacedRowsCount() {
      return replacedRowsCount;
    }

    @Override
    public long minSequenceNumber() {
      return minSequenceNumber;
    }

    @Override
    public ByteBuffer dv() {
      return dv;
    }

    @Override
    public Long dvCardinality() {
      return dvCardinality;
    }

    @Override
    public int size() {
      return 11;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      Object value;
      switch (pos) {
        case 0:
          value = addedFilesCount;
          break;
        case 1:
          value = existingFilesCount;
          break;
        case 2:
          value = deletedFilesCount;
          break;
        case 3:
          value = replacedFilesCount;
          break;
        case 4:
          value = addedRowsCount;
          break;
        case 5:
          value = existingRowsCount;
          break;
        case 6:
          value = deletedRowsCount;
          break;
        case 7:
          value = replacedRowsCount;
          break;
        case 8:
          value = minSequenceNumber;
          break;
        case 9:
          value = dv;
          break;
        case 10:
          value = dvCardinality;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }

      return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void set(int pos, T value) {
      switch (pos) {
        case 0:
          this.addedFilesCount = (Integer) value;
          break;
        case 1:
          this.existingFilesCount = (Integer) value;
          break;
        case 2:
          this.deletedFilesCount = (Integer) value;
          break;
        case 3:
          this.replacedFilesCount = (Integer) value;
          break;
        case 4:
          this.addedRowsCount = (Long) value;
          break;
        case 5:
          this.existingRowsCount = (Long) value;
          break;
        case 6:
          this.deletedRowsCount = (Long) value;
          break;
        case 7:
          this.replacedRowsCount = (Long) value;
          break;
        case 8:
          this.minSequenceNumber = (Long) value;
          break;
        case 9:
          this.dv = (ByteBuffer) value;
          break;
        case 10:
          this.dvCardinality = (Long) value;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }
  }
}
