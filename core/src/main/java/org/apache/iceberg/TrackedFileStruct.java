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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ByteBuffers;

/** Internal struct implementation of {@link TrackedFile} for V4 manifests. */
class TrackedFileStruct extends SupportsIndexProjection
    implements TrackedFile, StructLike, Serializable {

  private static final FileContent[] FILE_CONTENT_VALUES = FileContent.values();

  // Reader-side metadata (not serialized)
  private String manifestLocation = null;
  private Long position = null;

  // Common file metadata
  private FileContent contentType = FileContent.DATA;
  private String location = null;
  private FileFormat fileFormat = null;
  private int partitionSpecId = -1;
  private Integer sortOrderId = null;
  private long recordCount = -1L;
  private Long fileSizeInBytes = null;
  private byte[] keyMetadata = null;
  private long[] splitOffsets = null;
  private int[] equalityIds = null;
  private String referencedFile = null;

  // Nested structs
  private TrackingInfoStruct trackingInfo = null;
  private ContentInfoStruct contentInfo = null;
  private ManifestStatsStruct manifestStats = null;

  // Manifest DV (for manifest entries - marks deleted entries without rewriting)
  private byte[] manifestDV = null;

  // Content stats placeholder (TODO: implement ContentStats)
  private Object contentStats = null;

  // Base type that corresponds to positions for get/set
  // Nested structs (tracking_info, content_info, manifest_stats) are embedded as struct fields
  static final Types.StructType BASE_TYPE =
      Types.StructType.of(
          TrackedFile.CONTENT_TYPE,
          TrackedFile.LOCATION,
          TrackedFile.FILE_FORMAT,
          TrackedFile.PARTITION_SPEC_ID,
          TrackedFile.SORT_ORDER_ID,
          TrackedFile.RECORD_COUNT,
          TrackedFile.FILE_SIZE_IN_BYTES,
          TrackedFile.KEY_METADATA,
          TrackedFile.SPLIT_OFFSETS,
          TrackedFile.EQUALITY_IDS,
          TrackedFile.REFERENCED_FILE,
          TrackedFile.TRACKING_INFO,
          TrackedFile.CONTENT_INFO,
          TrackedFile.MANIFEST_STATS,
          TrackedFile.MANIFEST_DV);

  /** Used by internal readers to instantiate this class with a projection schema. */
  TrackedFileStruct(Types.StructType projection) {
    super(BASE_TYPE, projection);
  }

  TrackedFileStruct() {
    super(BASE_TYPE.fields().size());
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a tracked file to copy
   * @param requestedColumnIds column IDs for which to keep stats, or null for all stats
   */
  private TrackedFileStruct(TrackedFileStruct toCopy, Set<Integer> requestedColumnIds) {
    super(toCopy);

    // Reader-side metadata
    this.manifestLocation = toCopy.manifestLocation;
    this.position = toCopy.position;

    // Common file metadata
    this.contentType = toCopy.contentType;
    this.location = toCopy.location;
    this.fileFormat = toCopy.fileFormat;
    this.partitionSpecId = toCopy.partitionSpecId;
    this.sortOrderId = toCopy.sortOrderId;
    this.recordCount = toCopy.recordCount;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
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
    this.referencedFile = toCopy.referencedFile;

    // Nested structs (always copy)
    this.trackingInfo =
        toCopy.trackingInfo != null ? new TrackingInfoStruct(toCopy.trackingInfo) : null;
    this.contentInfo =
        toCopy.contentInfo != null ? new ContentInfoStruct(toCopy.contentInfo) : null;

    // Manifest DV (always copy - not dependent on stats)
    this.manifestDV =
        toCopy.manifestDV != null
            ? Arrays.copyOf(toCopy.manifestDV, toCopy.manifestDV.length)
            : null;

    if (requestedColumnIds != null) {
      this.manifestStats =
          toCopy.manifestStats != null ? new ManifestStatsStruct(toCopy.manifestStats) : null;
      // TODO: When ContentStats structure is implemented, filter stats to only requestedColumnIds
      this.contentStats = toCopy.contentStats;
    }
  }

  @Override
  public String manifestLocation() {
    return manifestLocation;
  }

  public void setManifestLocation(String manifestLocation) {
    this.manifestLocation = manifestLocation;
  }

  @Override
  public TrackingInfo trackingInfo() {
    return trackingInfo;
  }

  public void setTrackingInfo(TrackingInfo info) {
    this.trackingInfo = info != null ? new TrackingInfoStruct(info) : null;
  }

  @Override
  public Long pos() {
    return position;
  }

  void setPos(Long pos) {
    this.position = pos;
  }

  /**
   * Returns the mutable tracking info, creating one if needed.
   *
   * <p>Use this to modify tracking info fields directly rather than through TrackedFileStruct.
   */
  TrackingInfoStruct ensureTrackingInfo() {
    if (trackingInfo == null) {
      trackingInfo = new TrackingInfoStruct();
    }
    return trackingInfo;
  }

  @Override
  public FileContent contentType() {
    return contentType;
  }

  public void setContentType(FileContent contentType) {
    this.contentType = contentType;
  }

  @Override
  public String location() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @Override
  public FileFormat fileFormat() {
    return fileFormat;
  }

  public void setFileFormat(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  @Override
  public ContentInfo contentInfo() {
    return contentInfo;
  }

  public void setContentInfo(ContentInfo info) {
    this.contentInfo = info != null ? new ContentInfoStruct(info) : null;
  }

  @Override
  public int partitionSpecId() {
    return partitionSpecId;
  }

  public void setPartitionSpecId(int partitionSpecId) {
    this.partitionSpecId = partitionSpecId;
  }

  @Override
  public Integer sortOrderId() {
    return sortOrderId;
  }

  public void setSortOrderId(Integer sortOrderId) {
    this.sortOrderId = sortOrderId;
  }

  @Override
  public long recordCount() {
    return recordCount;
  }

  public void setRecordCount(long recordCount) {
    this.recordCount = recordCount;
  }

  @Override
  public Long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  public void setFileSizeInBytes(Long fileSizeInBytes) {
    this.fileSizeInBytes = fileSizeInBytes;
  }

  @Override
  public Object contentStats() {
    return contentStats;
  }

  @Override
  public ManifestStats manifestStats() {
    return manifestStats;
  }

  public void setManifestStats(ManifestStats stats) {
    this.manifestStats = stats != null ? new ManifestStatsStruct(stats) : null;
  }

  @Override
  public ByteBuffer manifestDV() {
    return manifestDV != null ? ByteBuffer.wrap(manifestDV) : null;
  }

  public void setManifestDV(ByteBuffer dv) {
    this.manifestDV = ByteBuffers.toByteArray(dv);
  }

  @Override
  public ByteBuffer keyMetadata() {
    return keyMetadata != null ? ByteBuffer.wrap(keyMetadata) : null;
  }

  public void setKeyMetadata(ByteBuffer keyMetadata) {
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
  }

  @Override
  public List<Long> splitOffsets() {
    return ArrayUtil.toUnmodifiableLongList(splitOffsets);
  }

  public void setSplitOffsets(List<Long> offsets) {
    this.splitOffsets = ArrayUtil.toLongArray(offsets);
  }

  @Override
  public List<Integer> equalityIds() {
    return ArrayUtil.toUnmodifiableIntList(equalityIds);
  }

  public void setEqualityIds(List<Integer> ids) {
    this.equalityIds = ArrayUtil.toIntArray(ids);
  }

  @Override
  public String referencedFile() {
    return referencedFile;
  }

  public void setReferencedFile(String referencedFile) {
    this.referencedFile = referencedFile;
  }

  @Override
  public TrackedFile copy() {
    return new TrackedFileStruct(this, Collections.emptySet());
  }

  @Override
  public TrackedFile copyWithoutStats() {
    return new TrackedFileStruct(this, null);
  }

  @Override
  public TrackedFile copyWithStats(Set<Integer> requestedColumnIds) {
    return new TrackedFileStruct(this, requestedColumnIds);
  }

  @Override
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0:
        this.contentType = value != null ? FILE_CONTENT_VALUES[(Integer) value] : null;
        return;
      case 1:
        this.location = (String) value;
        return;
      case 2:
        this.fileFormat = value != null ? FileFormat.fromString(value.toString()) : null;
        return;
      case 3:
        this.partitionSpecId = value != null ? (Integer) value : -1;
        return;
      case 4:
        this.sortOrderId = (Integer) value;
        return;
      case 5:
        this.recordCount = value != null ? (Long) value : -1L;
        return;
      case 6:
        this.fileSizeInBytes = (Long) value;
        return;
      case 7:
        this.keyMetadata = ByteBuffers.toByteArray((ByteBuffer) value);
        return;
      case 8:
        this.splitOffsets = ArrayUtil.toLongArray((List<Long>) value);
        return;
      case 9:
        this.equalityIds = ArrayUtil.toIntArray((List<Integer>) value);
        return;
      case 10:
        this.referencedFile = (String) value;
        return;
      case 11:
        this.trackingInfo = TrackingInfoStruct.fromStructLike((StructLike) value);
        return;
      case 12:
        this.contentInfo = ContentInfoStruct.fromStructLike((StructLike) value);
        return;
      case 13:
        this.manifestStats = ManifestStatsStruct.fromStructLike((StructLike) value);
        return;
      case 14:
        this.manifestDV = ByteBuffers.toByteArray((ByteBuffer) value);
        return;
      case 15:
        // MetadataColumns.ROW_POSITION - set the ordinal position in the manifest
        this.position = (Long) value;
        return;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  public Object get(int pos) {
    return getByPos(pos);
  }

  private Object getByPos(int pos) {
    switch (pos) {
      case 0:
        return contentType != null ? contentType.id() : null;
      case 1:
        return location;
      case 2:
        return fileFormat != null ? fileFormat.name().toLowerCase(Locale.ROOT) : null;
      case 3:
        return partitionSpecId;
      case 4:
        return sortOrderId;
      case 5:
        return recordCount;
      case 6:
        return fileSizeInBytes;
      case 7:
        return keyMetadata();
      case 8:
        return splitOffsets();
      case 9:
        return equalityIds();
      case 10:
        return referencedFile;
      case 11:
        return trackingInfo;
      case 12:
        return contentInfo;
      case 13:
        return manifestStats;
      case 14:
        return manifestDV != null ? ByteBuffer.wrap(manifestDV) : null;
      case 15:
        // MetadataColumns.ROW_POSITION
        return position;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public DataFile asDataFile(PartitionSpec spec) {
    if (contentType != FileContent.DATA) {
      throw new IllegalStateException(
          "Cannot convert TrackedFile with content type " + contentType + " to DataFile");
    }
    return new TrackedDataFile(this, spec);
  }

  @Override
  public DeleteFile asDeleteFile(PartitionSpec spec) {
    if (contentType != FileContent.POSITION_DELETES
        && contentType != FileContent.EQUALITY_DELETES) {
      throw new IllegalStateException(
          "Cannot convert TrackedFile with content type " + contentType + " to DeleteFile");
    }
    return new TrackedDeleteFile(this, spec);
  }

  /** Wrapper that presents a TrackedFile as a DataFile. */
  private static class TrackedDataFile implements DataFile {
    private final TrackedFile trackedFile;

    @SuppressWarnings("UnusedVariable")
    private final PartitionSpec spec;

    private TrackedDataFile(TrackedFile trackedFile, PartitionSpec spec) {
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
      // TODO: Implement partition value adaptation from content stats - key for validating v4
      // partition design
      return null;
    }

    @Override
    public long recordCount() {
      return trackedFile.recordCount();
    }

    @Override
    public long fileSizeInBytes() {
      return trackedFile.fileSizeInBytes();
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      // TODO: Adapt from new column stats structure
      return null;
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      // TODO: Adapt from new column stats structure
      return null;
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      // TODO: Adapt from new column stats structure
      return null;
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      // TODO: Adapt from new column stats structure
      return null;
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      // TODO: Adapt from new column stats structure
      return null;
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      // TODO: Adapt from new column stats structure
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
      return trackingInfo != null ? trackingInfo.dataSequenceNumber() : null;
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
      return new TrackedDataFile(trackedFile.copy(), spec);
    }

    @Override
    public DataFile copyWithoutStats() {
      return new TrackedDataFile(trackedFile.copyWithoutStats(), spec);
    }

    @Override
    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedDataFile(trackedFile.copyWithStats(requestedColumnIds), spec);
    }
  }

  /** Wrapper that presents a TrackedFile as a DeleteFile. */
  private static class TrackedDeleteFile implements DeleteFile {
    private final TrackedFile trackedFile;

    @SuppressWarnings("UnusedVariable")
    private final PartitionSpec spec;

    private TrackedDeleteFile(TrackedFile trackedFile, PartitionSpec spec) {
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
      // TODO: Implement partition value adaptation from content stats
      return null;
    }

    @Override
    public long recordCount() {
      return trackedFile.recordCount();
    }

    @Override
    public long fileSizeInBytes() {
      return trackedFile.fileSizeInBytes();
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      // TODO: Adapt from new column stats structure
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
      // TODO: Adapt from new column stats structure
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
      return trackingInfo != null ? trackingInfo.dataSequenceNumber() : null;
    }

    @Override
    public Long fileSequenceNumber() {
      TrackingInfo trackingInfo = trackedFile.trackingInfo();
      return trackingInfo != null ? trackingInfo.fileSequenceNumber() : null;
    }

    @Override
    public DeleteFile copy() {
      return new TrackedDeleteFile(trackedFile.copy(), spec);
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return new TrackedDeleteFile(trackedFile.copyWithoutStats(), spec);
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedDeleteFile(trackedFile.copyWithStats(requestedColumnIds), spec);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("content_type", contentType)
        .add("location", location)
        .add("file_format", fileFormat)
        .add("partition_spec_id", partitionSpecId)
        .add("record_count", recordCount)
        .add("file_size_in_bytes", fileSizeInBytes)
        .add("sort_order_id", sortOrderId)
        .add("key_metadata", keyMetadata == null ? "null" : "(redacted)")
        .add("split_offsets", splitOffsets)
        .add("equality_ids", equalityIds)
        .add("referenced_file", referencedFile)
        .add("tracking_info", trackingInfo)
        .add("content_info", contentInfo)
        .add("manifest_stats", manifestStats)
        .add("manifest_dv", manifestDV == null ? "null" : "(present)")
        .toString();
  }

  /** Mutable struct implementation of TrackingInfo. */
  static class TrackingInfoStruct implements TrackingInfo, StructLike, Serializable {
    // Serialized fields
    private TrackingInfo.Status status = null;
    private Long snapshotId = null;
    private Long sequenceNumber = null;
    private Long fileSequenceNumber = null;
    private Long firstRowId = null;

    // Reader-side metadata (not serialized)
    private String manifestLocation = null;
    private long manifestPos = -1;

    TrackingInfoStruct() {}

    TrackingInfoStruct(TrackingInfo toCopy) {
      this.status = toCopy.status();
      this.snapshotId = toCopy.snapshotId();
      this.sequenceNumber = toCopy.dataSequenceNumber();
      this.fileSequenceNumber = toCopy.fileSequenceNumber();
      this.firstRowId = toCopy.firstRowId();
      this.manifestLocation = toCopy.manifestLocation();
      this.manifestPos = toCopy.manifestPos();
    }

    static TrackingInfoStruct fromStructLike(StructLike struct) {
      if (struct == null) {
        return null;
      }
      if (struct instanceof TrackingInfoStruct) {
        return (TrackingInfoStruct) struct;
      }
      TrackingInfoStruct result = new TrackingInfoStruct();
      Integer statusId = struct.get(0, Integer.class);
      result.status = statusId != null ? TrackingInfo.Status.values()[statusId] : null;
      result.snapshotId = struct.get(1, Long.class);
      result.sequenceNumber = struct.get(2, Long.class);
      result.fileSequenceNumber = struct.get(3, Long.class);
      result.firstRowId = struct.get(4, Long.class);
      return result;
    }

    @Override
    public Status status() {
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
    public Long firstRowId() {
      return firstRowId;
    }

    void setStatus(TrackingInfo.Status status) {
      this.status = status;
    }

    void setSnapshotId(Long snapshotId) {
      this.snapshotId = snapshotId;
    }

    void setSequenceNumber(Long sequenceNumber) {
      this.sequenceNumber = sequenceNumber;
    }

    void setFileSequenceNumber(Long fileSequenceNumber) {
      this.fileSequenceNumber = fileSequenceNumber;
    }

    void setFirstRowId(Long firstRowId) {
      this.firstRowId = firstRowId;
    }

    @Override
    public String manifestLocation() {
      return manifestLocation;
    }

    void setManifestLocation(String manifestLocation) {
      this.manifestLocation = manifestLocation;
    }

    @Override
    public long manifestPos() {
      return manifestPos;
    }

    void setManifestPos(long manifestPos) {
      this.manifestPos = manifestPos;
    }

    @Override
    public int size() {
      return 5;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      Object value;
      switch (pos) {
        case 0:
          value = status != null ? status.id() : null;
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
          value = firstRowId;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
      return javaClass.cast(value);
    }

    @Override
    public <T> void set(int pos, T value) {
      switch (pos) {
        case 0:
          this.status = value != null ? TrackingInfo.Status.values()[(Integer) value] : null;
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
          this.firstRowId = (Long) value;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("status", status)
          .add("snapshot_id", snapshotId)
          .add("sequence_number", sequenceNumber)
          .add("file_sequence_number", fileSequenceNumber)
          .add("first_row_id", firstRowId)
          .add("manifest_location", manifestLocation)
          .add("manifest_pos", manifestPos)
          .toString();
    }
  }

  /** Mutable struct implementation of ContentInfo. */
  static class ContentInfoStruct implements ContentInfo, StructLike, Serializable {
    private long offset;
    private long sizeInBytes;

    ContentInfoStruct() {}

    ContentInfoStruct(ContentInfo toCopy) {
      this.offset = toCopy.offset();
      this.sizeInBytes = toCopy.sizeInBytes();
    }

    static ContentInfoStruct fromStructLike(StructLike struct) {
      if (struct == null) {
        return null;
      }
      if (struct instanceof ContentInfoStruct) {
        return (ContentInfoStruct) struct;
      }
      ContentInfoStruct result = new ContentInfoStruct();
      result.offset = struct.get(0, Long.class);
      result.sizeInBytes = struct.get(1, Long.class);
      return result;
    }

    @Override
    public long offset() {
      return offset;
    }

    @Override
    public long sizeInBytes() {
      return sizeInBytes;
    }

    void setOffset(long offset) {
      this.offset = offset;
    }

    void setSizeInBytes(long sizeInBytes) {
      this.sizeInBytes = sizeInBytes;
    }

    @Override
    public int size() {
      return 2;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      Object value;
      switch (pos) {
        case 0:
          value = offset;
          break;
        case 1:
          value = sizeInBytes;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
      return javaClass.cast(value);
    }

    @Override
    public <T> void set(int pos, T value) {
      switch (pos) {
        case 0:
          this.offset = (Long) value;
          break;
        case 1:
          this.sizeInBytes = (Long) value;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("offset", offset)
          .add("size_in_bytes", sizeInBytes)
          .toString();
    }
  }

  /** Mutable struct implementation of ManifestStats. */
  static class ManifestStatsStruct implements ManifestStats, StructLike, Serializable {
    private int addedFilesCount;
    private int existingFilesCount;
    private int deletedFilesCount;
    private long addedRowsCount;
    private long existingRowsCount;
    private long deletedRowsCount;
    private long minSequenceNumber;

    ManifestStatsStruct() {}

    ManifestStatsStruct(ManifestStats toCopy) {
      this.addedFilesCount = toCopy.addedFilesCount();
      this.existingFilesCount = toCopy.existingFilesCount();
      this.deletedFilesCount = toCopy.deletedFilesCount();
      this.addedRowsCount = toCopy.addedRowsCount();
      this.existingRowsCount = toCopy.existingRowsCount();
      this.deletedRowsCount = toCopy.deletedRowsCount();
      this.minSequenceNumber = toCopy.minSequenceNumber();
    }

    static ManifestStatsStruct fromStructLike(StructLike struct) {
      if (struct == null) {
        return null;
      }
      if (struct instanceof ManifestStatsStruct) {
        return (ManifestStatsStruct) struct;
      }
      ManifestStatsStruct result = new ManifestStatsStruct();
      result.addedFilesCount = struct.get(0, Integer.class);
      result.existingFilesCount = struct.get(1, Integer.class);
      result.deletedFilesCount = struct.get(2, Integer.class);
      result.addedRowsCount = struct.get(3, Long.class);
      result.existingRowsCount = struct.get(4, Long.class);
      result.deletedRowsCount = struct.get(5, Long.class);
      result.minSequenceNumber = struct.get(6, Long.class);
      return result;
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
    public long minSequenceNumber() {
      return minSequenceNumber;
    }

    void setAddedFilesCount(int count) {
      this.addedFilesCount = count;
    }

    void setExistingFilesCount(int count) {
      this.existingFilesCount = count;
    }

    void setDeletedFilesCount(int count) {
      this.deletedFilesCount = count;
    }

    void setAddedRowsCount(long count) {
      this.addedRowsCount = count;
    }

    void setExistingRowsCount(long count) {
      this.existingRowsCount = count;
    }

    void setDeletedRowsCount(long count) {
      this.deletedRowsCount = count;
    }

    void setMinSequenceNumber(long minSeqNum) {
      this.minSequenceNumber = minSeqNum;
    }

    @Override
    public int size() {
      return 7;
    }

    @Override
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
          value = addedRowsCount;
          break;
        case 4:
          value = existingRowsCount;
          break;
        case 5:
          value = deletedRowsCount;
          break;
        case 6:
          value = minSequenceNumber;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
      return javaClass.cast(value);
    }

    @Override
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
          this.addedRowsCount = (Long) value;
          break;
        case 4:
          this.existingRowsCount = (Long) value;
          break;
        case 5:
          this.deletedRowsCount = (Long) value;
          break;
        case 6:
          this.minSequenceNumber = (Long) value;
          break;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("added_files_count", addedFilesCount)
          .add("existing_files_count", existingFilesCount)
          .add("deleted_files_count", deletedFilesCount)
          .add("added_rows_count", addedRowsCount)
          .add("existing_rows_count", existingRowsCount)
          .add("deleted_rows_count", deletedRowsCount)
          .add("min_sequence_number", minSequenceNumber)
          .toString();
    }
  }
}
