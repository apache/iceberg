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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.stats.BaseContentStats;
import org.apache.iceberg.stats.ContentStats;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ByteBuffers;

/** Mutable {@link StructLike} implementation of {@link TrackedFile}. */
class TrackedFileStruct extends SupportsIndexProjection implements TrackedFile, Serializable {
  private static final FileContent[] FILE_CONTENT_VALUES = FileContent.values();
  private static final Types.StructType BASE_TYPE =
      Types.StructType.of(
          ImmutableList.<Types.NestedField>builder()
              .addAll(TrackedFile.schemaWithContentStats(Types.StructType.of()).fields())
              .add(MetadataColumns.ROW_POSITION)
              .build());

  private FileContent contentType = FileContent.DATA;
  private String location = null;
  private FileFormat fileFormat = null;
  private long recordCount = 0L;
  private long fileSizeInBytes = 0L;
  private Integer specId = null;

  // optional fields
  private TrackingStruct tracking = null;
  private ContentStats contentStats = null;
  private Integer sortOrderId = null;
  private DeletionVectorStruct deletionVector = null;
  private ManifestInfoStruct manifestInfo = null;
  private byte[] keyMetadata = null;
  private long[] splitOffsets = null;
  private int[] equalityIds = null;

  // not serialized to manifest, set by manifest readers
  private transient TrackedFile manifestContext = null;
  private long manifestPos = 0L;

  /** Used by internal readers to instantiate this class with a projection schema. */
  TrackedFileStruct(Types.StructType projection) {
    super(BASE_TYPE, projection);
  }

  /** No-projection constructor for direct construction. */
  TrackedFileStruct() {
    super(BASE_TYPE.fields().size());
  }

  /** Copy constructor. */
  private TrackedFileStruct(TrackedFileStruct toCopy, boolean withStats, Set<Integer> statsIds) {
    super(toCopy);
    this.contentType = toCopy.contentType;
    this.location = toCopy.location;
    this.fileFormat = toCopy.fileFormat;
    this.recordCount = toCopy.recordCount;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
    this.specId = toCopy.specId;

    this.tracking = toCopy.tracking != null ? toCopy.tracking.copy() : null;
    if (this.tracking != null && this.manifestContext != null) {
      this.tracking.setManifestContext(this.manifestContext);
    }

    this.sortOrderId = toCopy.sortOrderId;
    this.deletionVector = toCopy.deletionVector != null ? toCopy.deletionVector.copy() : null;

    if (withStats && toCopy.contentStats != null) {
      ContentStats filtered = BaseContentStats.buildFrom(toCopy.contentStats, statsIds).build();
      this.contentStats = filtered.fieldStats().isEmpty() ? null : filtered;
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

    this.manifestContext = toCopy.manifestContext;
    this.manifestPos = toCopy.manifestPos;
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
    if (specId != null) {
      return specId;
    }

    return manifestContext != null ? manifestContext.specId() : null;
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
    return new TrackedFileStruct(this, true, null);
  }

  @Override
  public TrackedFile copyWithStats(Set<Integer> requestedColumnIds) {
    return new TrackedFileStruct(this, true, requestedColumnIds);
  }

  @Override
  public String manifestLocation() {
    return manifestContext != null ? manifestContext.location() : null;
  }

  @Override
  public long manifestPos() {
    return manifestPos;
  }

  void setManifestContext(TrackedFile manifest) {
    this.manifestContext = manifest;
    if (this.tracking != null) {
      this.tracking.setManifestContext(manifest);
    }
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Object getByPos(int pos) {
    switch (pos) {
      case 0:
        return tracking;
      case 1:
        return contentType.id();
      case 2:
        return location;
      case 3:
        return fileFormat != null ? fileFormat.toString() : null;
      case 4:
        return recordCount;
      case 5:
        return fileSizeInBytes;
      case 6:
        return specId;
      case 7:
        return contentStats;
      case 8:
        return sortOrderId;
      case 9:
        return deletionVector;
      case 10:
        return manifestInfo;
      case 11:
        return keyMetadata();
      case 12:
        return splitOffsets();
      case 13:
        return equalityIds();
      case 14:
        return manifestPos;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0:
        this.tracking = (TrackingStruct) value;
        this.tracking.setManifestContext(this.manifestContext);
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
        this.keyMetadata = ByteBuffers.toByteArray((ByteBuffer) value);
        break;
      case 12:
        this.splitOffsets = ArrayUtil.toLongArray((List<Long>) value);
        break;
      case 13:
        this.equalityIds = ArrayUtil.toIntArray((List<Integer>) value);
        break;
      case 14:
        this.manifestPos = (long) value;
        break;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }
}
