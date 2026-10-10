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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.iceberg.util.StructProjection;

/** Mutable {@link StructLike} implementation of {@link TrackedFile}. */
class TrackedFileStruct extends SupportsIndexProjection implements TrackedFile, Serializable {
  private static final Types.StructType EMPTY_STRUCT_TYPE = Types.StructType.of();

  private static final Types.StructType BASE_TYPE =
      Types.StructType.of(
          TrackedFile.TRACKING,
          TrackedFile.CONTENT_TYPE,
          TrackedFile.LOCATION,
          TrackedFile.FILE_FORMAT,
          TrackedFile.RECORD_COUNT,
          TrackedFile.FILE_SIZE_IN_BYTES,
          TrackedFile.SPEC_ID,
          Types.NestedField.optional(
              TrackedFile.PARTITION_ID,
              TrackedFile.PARTITION_NAME,
              EMPTY_STRUCT_TYPE,
              TrackedFile.PARTITION_DOC),
          Types.NestedField.optional(
              TrackedFile.CONTENT_STATS_ID,
              TrackedFile.CONTENT_STATS_NAME,
              EMPTY_STRUCT_TYPE,
              TrackedFile.CONTENT_STATS_DOC),
          TrackedFile.SORT_ORDER_ID,
          TrackedFile.DELETION_VECTOR,
          TrackedFile.MANIFEST_INFO,
          TrackedFile.KEY_METADATA,
          TrackedFile.SPLIT_OFFSETS,
          TrackedFile.EQUALITY_IDS);

  private FileContent contentType = null;
  private String location = null;
  private FileFormat fileFormat = null;
  private Tracking tracking = null;
  private long recordCount = -1L;
  private long fileSizeInBytes = -1L;
  private StructLike partition = null;

  // optional fields
  private Integer specId = null;
  private ContentStats contentStats = null;
  private Integer sortOrderId = null;
  private DeletionVector deletionVector = null;
  private ManifestInfo manifestInfo = null;
  private byte[] keyMetadata = null;
  private long[] splitOffsets = null;
  private int[] equalityIds = null;

  private transient StructProjection partitionProjection = null;

  /** Used by internal readers to instantiate this class with a projection schema. */
  TrackedFileStruct(Types.StructType projection) {
    super(BASE_TYPE, projection);
  }

  /** No-projection constructor for direct construction. */
  TrackedFileStruct() {
    super(BASE_TYPE.fields().size());
  }

  TrackedFileStruct(
      Tracking tracking,
      FileContent contentType,
      String location,
      FileFormat fileFormat,
      long recordCount,
      long fileSizeInBytes,
      Integer specId,
      PartitionData partition,
      ContentStats contentStats,
      Integer sortOrderId,
      DeletionVector deletionVector,
      ManifestInfo manifestInfo,
      ByteBuffer keyMetadata,
      List<Long> splitOffsets,
      List<Integer> equalityIds) {
    super(BASE_TYPE.fields().size());
    this.tracking = tracking;
    this.contentType = contentType;
    this.location = location;
    this.fileFormat = fileFormat;
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
    this.specId = specId;
    this.partition = partition;
    this.contentStats = contentStats;
    this.sortOrderId = sortOrderId;
    this.deletionVector = deletionVector;
    this.manifestInfo = manifestInfo;
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
    this.splitOffsets = ArrayUtil.toLongArray(splitOffsets);
    this.equalityIds = ArrayUtil.toIntArray(equalityIds);
  }

  /** Copy constructor. */
  private TrackedFileStruct(TrackedFileStruct toCopy, Set<Integer> statsIds) {
    super(toCopy);
    this.contentType = toCopy.contentType;
    this.location = toCopy.location;
    this.fileFormat = toCopy.fileFormat;
    this.recordCount = toCopy.recordCount;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
    this.specId = toCopy.specId;
    this.partition = toCopy.partition == null ? null : StructLikeUtil.copy(toCopy.partition());
    this.tracking = toCopy.tracking != null ? toCopy.tracking.copy() : null;
    this.sortOrderId = toCopy.sortOrderId;
    this.deletionVector = toCopy.deletionVector != null ? toCopy.deletionVector.copy() : null;

    if (toCopy.contentStats != null && (statsIds == null || !statsIds.isEmpty())) {
      this.contentStats =
          statsIds != null ? toCopy.contentStats.copy(statsIds) : toCopy.contentStats.copy();
    } else {
      this.contentStats = null;
    }

    this.manifestInfo = toCopy.manifestInfo != null ? toCopy.manifestInfo.copy() : null;
    this.keyMetadata =
        toCopy.keyMetadata != null
            ? Arrays.copyOf(toCopy.keyMetadata, toCopy.keyMetadata.length)
            : null;
    this.splitOffsets =
        toCopy.splitOffsets != null
            ? Arrays.copyOf(toCopy.splitOffsets, toCopy.splitOffsets.length)
            : null;
    this.equalityIds =
        toCopy.equalityIds != null
            ? Arrays.copyOf(toCopy.equalityIds, toCopy.equalityIds.length)
            : null;
  }

  @Override
  public Tracking tracking() {
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

  // Package-private only so the manifest reader can store the location resolved against the
  // table location; other callers must go through construction.
  void setLocation(String newLocation) {
    this.location = newLocation;
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

  void setPartitionProjection(StructProjection projection) {
    this.partitionProjection = projection;
  }

  void clearPartition() {
    this.partition = null;
    this.partitionProjection = null;
  }

  @Override
  public Integer specId() {
    return specId;
  }

  @Override
  public StructLike partition() {
    if (partition == null) {
      ValidationException.check(specId == null, "Missing partition for spec %s", specId);
      return null;
    }

    return partitionProjection != null ? partitionProjection.wrap(partition) : partition;
  }

  @Override
  public ContentStats contentStats() {
    return contentStats;
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
    return keyMetadata != null ? ByteBuffer.wrap(keyMetadata) : null;
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
    return new TrackedFileStruct(this, null);
  }

  @Override
  public TrackedFile copyWithStats(Set<Integer> requestedColumnIds) {
    return new TrackedFileStruct(this, requestedColumnIds);
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Object getByPos(int pos) {
    return switch (pos) {
      case 0 -> tracking;
      case 1 -> contentType != null ? contentType.id() : null;
      case 2 -> location;
      case 3 -> fileFormat != null ? fileFormat.toString() : null;
      case 4 -> recordCount;
      case 5 -> fileSizeInBytes;
      case 6 -> specId;
      case 7 -> partition;
      case 8 -> contentStats;
      case 9 -> sortOrderId;
      case 10 -> deletionVector;
      case 11 -> manifestInfo;
      case 12 -> keyMetadata();
      case 13 -> splitOffsets();
      case 14 -> equalityIds();
      default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    };
  }

  @Override
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0 -> this.tracking = (Tracking) value;
      case 1 -> this.contentType = FileContent.fromId((Integer) value);
      case 2 ->
          // always coerce to String for Serializable
          this.location = value.toString();
      case 3 -> this.fileFormat = FileFormat.fromString(value.toString());
      case 4 -> this.recordCount = (long) value;
      case 5 -> this.fileSizeInBytes = (long) value;
      case 6 -> this.specId = (Integer) value;
      case 7 -> this.partition = (StructLike) value;
      case 8 -> this.contentStats = (ContentStats) value;
      case 9 -> this.sortOrderId = (Integer) value;
      case 10 -> this.deletionVector = (DeletionVector) value;
      case 11 -> this.manifestInfo = (ManifestInfo) value;
      case 12 -> this.keyMetadata = ByteBuffers.toByteArray((ByteBuffer) value);
      case 13 -> this.splitOffsets = ArrayUtil.toLongArray((List<Long>) value);
      case 14 -> this.equalityIds = ArrayUtil.toIntArray((List<Integer>) value);
      default -> {
        // ignore the object, it must be from a newer version of the format
      }
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("content", contentType != null ? contentType.lowerCaseName() : null)
        .add("location", location)
        .add("file_format", fileFormat)
        .add("record_count", recordCount)
        .add("file_size_in_bytes", fileSizeInBytes)
        .add("spec_id", specId())
        .add("partition", partition)
        .add("tracking", tracking)
        .add("content_stats", contentStats)
        .add("sort_order_id", sortOrderId)
        .add("deletion_vector", deletionVector)
        .add("manifest_info", manifestInfo)
        .add("key_metadata", keyMetadata == null ? "null" : "(redacted)")
        .add("split_offsets", splitOffsets == null ? "null" : splitOffsets())
        .add("equality_ids", equalityIds == null ? "null" : equalityIds())
        .toString();
  }
}
