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
import java.util.Locale;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ByteBuffers;

/** Generic implementation of {@link TrackedFile} for V4 manifests. */
public class GenericTrackedFile extends SupportsIndexProjection
    implements TrackedFile<GenericTrackedFile>,
        IndexedRecord,
        StructLike,
        SpecificData.SchemaConstructable,
        Serializable {

  private static final FileContent[] FILE_CONTENT_VALUES = FileContent.values();

  // Tracking metadata
  private TrackingInfo.Status status = null;
  private Long snapshotId = null;
  private Long sequenceNumber = null;
  private Long fileSequenceNumber = null;
  private Long firstRowId = null;
  private Long position = null;

  // Common file metadata
  private FileContent contentType = FileContent.DATA;
  private String location = null;
  private FileFormat fileFormat = null;
  private long recordCount = -1L;
  private Long fileSizeInBytes = null;

  // Deletion vector fields
  private Long deletionVectorOffset = null;
  private Long deletionVectorSizeInBytes = null;
  private byte[] deletionVectorInlineContent = null;

  // Partition and ordering
  private int partitionSpecId = -1;
  private Integer sortOrderId = null;

  // Optional metadata
  private byte[] keyMetadata = null;
  private long[] splitOffsets = null;
  private int[] equalityIds = null;
  private String referencedFile = null;

  // Manifest stats (for manifest entries)
  private Integer addedFilesCount = null;
  private Integer existingFilesCount = null;
  private Integer deletedFilesCount = null;
  private Long addedRowsCount = null;
  private Long existingRowsCount = null;
  private Long deletedRowsCount = null;
  private Long minSequenceNumber = null;

  // Content stats placeholder (TODO: implement ContentStats)
  private Object contentStats = null;

  // Cached schema for Avro
  private transient Schema avroSchema = null;

  // Base type that corresponds to positions for get/set
  // All fields are made optional to support different content types
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
          // Tracking info fields
          TrackingInfo.STATUS.asOptional(),
          TrackingInfo.SNAPSHOT_ID,
          TrackingInfo.SEQUENCE_NUMBER,
          TrackingInfo.FILE_SEQUENCE_NUMBER,
          TrackingInfo.FIRST_ROW_ID,
          // Deletion vector fields
          DeletionVector.OFFSET,
          DeletionVector.SIZE_IN_BYTES,
          DeletionVector.INLINE_CONTENT,
          // Manifest stats fields
          ManifestStats.ADDED_FILES_COUNT.asOptional(),
          ManifestStats.EXISTING_FILES_COUNT.asOptional(),
          ManifestStats.DELETED_FILES_COUNT.asOptional(),
          ManifestStats.ADDED_ROWS_COUNT.asOptional(),
          ManifestStats.EXISTING_ROWS_COUNT.asOptional(),
          ManifestStats.DELETED_ROWS_COUNT.asOptional(),
          ManifestStats.MIN_SEQUENCE_NUMBER.asOptional());

  /** Used by Avro reflection to instantiate this class when reading manifest files. */
  public GenericTrackedFile(Schema avroSchema) {
    this(AvroSchemaUtil.convert(avroSchema).asStructType());
    this.avroSchema = avroSchema;
  }

  /** Used by internal readers to instantiate this class with a projection schema. */
  GenericTrackedFile(Types.StructType projection) {
    super(BASE_TYPE, projection);
    this.avroSchema = AvroSchemaUtil.convert(projection, "tracked_file");
  }

  public GenericTrackedFile() {
    super(BASE_TYPE.fields().size());
  }

  /**
   * Full constructor for creating a tracked file.
   *
   * @param contentType the type of content (DATA, POSITION_DELETES, etc.)
   * @param location the file location
   * @param fileFormat the file format
   * @param partitionSpecId the partition spec ID
   * @param recordCount the number of records
   */
  public GenericTrackedFile(
      FileContent contentType,
      String location,
      FileFormat fileFormat,
      int partitionSpecId,
      long recordCount,
      Long fileSizeInBytes) {
    super(BASE_TYPE.fields().size());
    this.contentType = contentType;
    this.location = location;
    this.fileFormat = fileFormat;
    this.partitionSpecId = partitionSpecId;
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a tracked file to copy
   * @param copyStats whether to copy stats
   */
  private GenericTrackedFile(GenericTrackedFile toCopy, boolean copyStats) {
    super(toCopy);

    // Tracking metadata
    this.status = toCopy.status;
    this.snapshotId = toCopy.snapshotId;
    this.sequenceNumber = toCopy.sequenceNumber;
    this.fileSequenceNumber = toCopy.fileSequenceNumber;
    this.firstRowId = toCopy.firstRowId;
    this.position = toCopy.position;

    // Common file metadata
    this.contentType = toCopy.contentType;
    this.location = toCopy.location;
    this.fileFormat = toCopy.fileFormat;
    this.recordCount = toCopy.recordCount;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;

    // Deletion vector
    this.deletionVectorOffset = toCopy.deletionVectorOffset;
    this.deletionVectorSizeInBytes = toCopy.deletionVectorSizeInBytes;
    this.deletionVectorInlineContent =
        toCopy.deletionVectorInlineContent != null
            ? Arrays.copyOf(
                toCopy.deletionVectorInlineContent, toCopy.deletionVectorInlineContent.length)
            : null;

    // Partition and ordering
    this.partitionSpecId = toCopy.partitionSpecId;
    this.sortOrderId = toCopy.sortOrderId;

    // Optional metadata
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

    if (copyStats) {
      // Manifest stats
      this.addedFilesCount = toCopy.addedFilesCount;
      this.existingFilesCount = toCopy.existingFilesCount;
      this.deletedFilesCount = toCopy.deletedFilesCount;
      this.addedRowsCount = toCopy.addedRowsCount;
      this.existingRowsCount = toCopy.existingRowsCount;
      this.deletedRowsCount = toCopy.deletedRowsCount;
      this.minSequenceNumber = toCopy.minSequenceNumber;
      this.contentStats = toCopy.contentStats;
    }
  }

  @Override
  public TrackingInfo trackingInfo() {
    if (status == null
        && snapshotId == null
        && sequenceNumber == null
        && fileSequenceNumber == null
        && firstRowId == null) {
      return null;
    }

    return new TrackingInfo() {
      @Override
      public TrackingInfo.Status status() {
        return status;
      }

      @Override
      public Long snapshotId() {
        return snapshotId;
      }

      @Override
      public Long sequenceNumber() {
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
    };
  }

  @Override
  public Long pos() {
    return position;
  }

  @Override
  public void setStatus(TrackingInfo.Status status) {
    this.status = status;
  }

  @Override
  public void setSnapshotId(Long snapshotId) {
    this.snapshotId = snapshotId;
  }

  @Override
  public void setSequenceNumber(Long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  @Override
  public void setFileSequenceNumber(Long fileSequenceNumber) {
    this.fileSequenceNumber = fileSequenceNumber;
  }

  @Override
  public void setFirstRowId(Long firstRowId) {
    this.firstRowId = firstRowId;
  }

  @Override
  public void setPos(Long pos) {
    this.position = pos;
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
  public DeletionVector deletionVector() {
    if (deletionVectorOffset == null
        && deletionVectorSizeInBytes == null
        && deletionVectorInlineContent == null) {
      return null;
    }

    return new DeletionVector() {
      @Override
      public Long offset() {
        return deletionVectorOffset;
      }

      @Override
      public Long sizeInBytes() {
        return deletionVectorSizeInBytes;
      }

      @Override
      public ByteBuffer inlineContent() {
        return deletionVectorInlineContent != null
            ? ByteBuffer.wrap(deletionVectorInlineContent)
            : null;
      }
    };
  }

  public void setDeletionVectorOffset(Long offset) {
    this.deletionVectorOffset = offset;
  }

  public void setDeletionVectorSizeInBytes(Long sizeInBytes) {
    this.deletionVectorSizeInBytes = sizeInBytes;
  }

  public void setDeletionVectorInlineContent(ByteBuffer inlineContent) {
    this.deletionVectorInlineContent = ByteBuffers.toByteArray(inlineContent);
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
    if (addedFilesCount == null
        && existingFilesCount == null
        && deletedFilesCount == null
        && addedRowsCount == null
        && existingRowsCount == null
        && deletedRowsCount == null
        && minSequenceNumber == null) {
      return null;
    }

    return new ManifestStats() {
      @Override
      public int addedFilesCount() {
        return addedFilesCount != null ? addedFilesCount : 0;
      }

      @Override
      public int existingFilesCount() {
        return existingFilesCount != null ? existingFilesCount : 0;
      }

      @Override
      public int deletedFilesCount() {
        return deletedFilesCount != null ? deletedFilesCount : 0;
      }

      @Override
      public long addedRowsCount() {
        return addedRowsCount != null ? addedRowsCount : 0L;
      }

      @Override
      public long existingRowsCount() {
        return existingRowsCount != null ? existingRowsCount : 0L;
      }

      @Override
      public long deletedRowsCount() {
        return deletedRowsCount != null ? deletedRowsCount : 0L;
      }

      @Override
      public long minSequenceNumber() {
        return minSequenceNumber != null ? minSequenceNumber : 0L;
      }
    };
  }

  public void setAddedFilesCount(Integer count) {
    this.addedFilesCount = count;
  }

  public void setExistingFilesCount(Integer count) {
    this.existingFilesCount = count;
  }

  public void setDeletedFilesCount(Integer count) {
    this.deletedFilesCount = count;
  }

  public void setAddedRowsCount(Long count) {
    this.addedRowsCount = count;
  }

  public void setExistingRowsCount(Long count) {
    this.existingRowsCount = count;
  }

  public void setDeletedRowsCount(Long count) {
    this.deletedRowsCount = count;
  }

  public void setMinSequenceNumber(Long minSeqNum) {
    this.minSequenceNumber = minSeqNum;
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
  public GenericTrackedFile copy() {
    return new GenericTrackedFile(this, true);
  }

  @Override
  public GenericTrackedFile copyWithoutStats() {
    return new GenericTrackedFile(this, false);
  }

  @Override
  public Schema getSchema() {
    if (avroSchema == null) {
      this.avroSchema = getAvroSchema();
    }
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    set(i, v);
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
        this.status = value != null ? TrackingInfo.Status.values()[(Integer) value] : null;
        return;
      case 12:
        this.snapshotId = (Long) value;
        return;
      case 13:
        this.sequenceNumber = (Long) value;
        return;
      case 14:
        this.fileSequenceNumber = (Long) value;
        return;
      case 15:
        this.firstRowId = (Long) value;
        return;
      case 16:
        this.deletionVectorOffset = (Long) value;
        return;
      case 17:
        this.deletionVectorSizeInBytes = (Long) value;
        return;
      case 18:
        this.deletionVectorInlineContent = ByteBuffers.toByteArray((ByteBuffer) value);
        return;
      case 19:
        this.addedFilesCount = (Integer) value;
        return;
      case 20:
        this.existingFilesCount = (Integer) value;
        return;
      case 21:
        this.deletedFilesCount = (Integer) value;
        return;
      case 22:
        this.addedRowsCount = (Long) value;
        return;
      case 23:
        this.existingRowsCount = (Long) value;
        return;
      case 24:
        this.deletedRowsCount = (Long) value;
        return;
      case 25:
        this.minSequenceNumber = (Long) value;
        return;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  @Override
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
        return status != null ? status.id() : null;
      case 12:
        return snapshotId;
      case 13:
        return sequenceNumber;
      case 14:
        return fileSequenceNumber;
      case 15:
        return firstRowId;
      case 16:
        return deletionVectorOffset;
      case 17:
        return deletionVectorSizeInBytes;
      case 18:
        return deletionVectorInlineContent != null
            ? ByteBuffer.wrap(deletionVectorInlineContent)
            : null;
      case 19:
        return addedFilesCount;
      case 20:
        return existingFilesCount;
      case 21:
        return deletedFilesCount;
      case 22:
        return addedRowsCount;
      case 23:
        return existingRowsCount;
      case 24:
        return deletedRowsCount;
      case 25:
        return minSequenceNumber;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  private Schema getAvroSchema() {
    return AvroSchemaUtil.convert(
        BASE_TYPE, ImmutableMap.of(BASE_TYPE, GenericTrackedFile.class.getName()));
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
        .add("status", status)
        .add("snapshot_id", snapshotId)
        .add("sequence_number", sequenceNumber)
        .add("file_sequence_number", fileSequenceNumber)
        .add("first_row_id", firstRowId)
        .add("deletion_vector_offset", deletionVectorOffset)
        .add("deletion_vector_size_in_bytes", deletionVectorSizeInBytes)
        .add(
            "deletion_vector_inline_content",
            deletionVectorInlineContent == null ? "null" : "(inline)")
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
