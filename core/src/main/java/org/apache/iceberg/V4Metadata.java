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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.iceberg.util.StructProjection;

class V4Metadata {
  private V4Metadata() {}

  static final Schema MANIFEST_LIST_SCHEMA =
      new Schema(
          ManifestFile.PATH,
          ManifestFile.LENGTH,
          ManifestFile.SPEC_ID,
          ManifestFile.MANIFEST_CONTENT.asRequired(),
          ManifestFile.SEQUENCE_NUMBER.asRequired(),
          ManifestFile.MIN_SEQUENCE_NUMBER.asRequired(),
          ManifestFile.SNAPSHOT_ID,
          ManifestFile.ADDED_FILES_COUNT.asRequired(),
          ManifestFile.EXISTING_FILES_COUNT.asRequired(),
          ManifestFile.DELETED_FILES_COUNT.asRequired(),
          ManifestFile.ADDED_ROWS_COUNT.asRequired(),
          ManifestFile.EXISTING_ROWS_COUNT.asRequired(),
          ManifestFile.DELETED_ROWS_COUNT.asRequired(),
          ManifestFile.PARTITION_SUMMARIES,
          ManifestFile.KEY_METADATA,
          ManifestFile.FIRST_ROW_ID);

  /**
   * A wrapper class to write any ManifestFile implementation to Avro using the v4 write schema.
   *
   * <p>This is used to maintain compatibility with v4 by writing manifest list files with the old
   * schema, instead of writing a sequence number into metadata files in v4 tables.
   */
  static class ManifestFileWrapper implements ManifestFile, StructLike {
    private final long commitSnapshotId;
    private final long sequenceNumber;
    private ManifestFile wrapped = null;
    private Long wrappedFirstRowId = null;

    ManifestFileWrapper(long commitSnapshotId, long sequenceNumber) {
      this.commitSnapshotId = commitSnapshotId;
      this.sequenceNumber = sequenceNumber;
    }

    public ManifestFile wrap(ManifestFile file, Long firstRowId) {
      this.wrapped = file;
      this.wrappedFirstRowId = firstRowId;
      return this;
    }

    @Override
    public int size() {
      return MANIFEST_LIST_SCHEMA.columns().size();
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify ManifestFileWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      switch (pos) {
        case 0:
          return wrapped.path();
        case 1:
          return wrapped.length();
        case 2:
          return wrapped.partitionSpecId();
        case 3:
          return wrapped.content().id();
        case 4:
          if (wrapped.sequenceNumber() == ManifestWriter.UNASSIGNED_SEQ) {
            // if the sequence number is being assigned here, then the manifest must be created by
            // the current
            // operation. to validate this, check that the snapshot id matches the current commit
            Preconditions.checkState(
                commitSnapshotId == wrapped.snapshotId(),
                "Found unassigned sequence number for a manifest from snapshot: %s",
                wrapped.snapshotId());
            return sequenceNumber;
          } else {
            return wrapped.sequenceNumber();
          }
        case 5:
          if (wrapped.minSequenceNumber() == ManifestWriter.UNASSIGNED_SEQ) {
            // same sanity check as above
            Preconditions.checkState(
                commitSnapshotId == wrapped.snapshotId(),
                "Found unassigned sequence number for a manifest from snapshot: %s",
                wrapped.snapshotId());
            // if the min sequence number is not determined, then there was no assigned sequence
            // number for any file
            // written to the wrapped manifest. replace the unassigned sequence number with the one
            // for this commit
            return sequenceNumber;
          } else {
            return wrapped.minSequenceNumber();
          }
        case 6:
          return wrapped.snapshotId();
        case 7:
          return wrapped.addedFilesCount();
        case 8:
          return wrapped.existingFilesCount();
        case 9:
          return wrapped.deletedFilesCount();
        case 10:
          return wrapped.addedRowsCount();
        case 11:
          return wrapped.existingRowsCount();
        case 12:
          return wrapped.deletedRowsCount();
        case 13:
          return wrapped.partitions();
        case 14:
          return wrapped.keyMetadata();
        case 15:
          if (wrappedFirstRowId != null) {
            // if first-row-id is assigned, ensure that it is valid
            Preconditions.checkState(
                wrapped.content() == ManifestContent.DATA && wrapped.firstRowId() == null,
                "Found invalid first-row-id assignment: %s",
                wrapped);
            return wrappedFirstRowId;
          } else if (wrapped.content() != ManifestContent.DATA) {
            return null;
          } else {
            Preconditions.checkState(
                wrapped.firstRowId() != null,
                "Found unassigned first-row-id for file: " + wrapped.path());
            return wrapped.firstRowId();
          }
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public String path() {
      return wrapped.path();
    }

    @Override
    public long length() {
      return wrapped.length();
    }

    @Override
    public int partitionSpecId() {
      return wrapped.partitionSpecId();
    }

    @Override
    public ManifestContent content() {
      return wrapped.content();
    }

    @Override
    public long sequenceNumber() {
      return wrapped.sequenceNumber();
    }

    @Override
    public long minSequenceNumber() {
      return wrapped.minSequenceNumber();
    }

    @Override
    public Long snapshotId() {
      return wrapped.snapshotId();
    }

    @Override
    public boolean hasAddedFiles() {
      return wrapped.hasAddedFiles();
    }

    @Override
    public Integer addedFilesCount() {
      return wrapped.addedFilesCount();
    }

    @Override
    public Long addedRowsCount() {
      return wrapped.addedRowsCount();
    }

    @Override
    public boolean hasExistingFiles() {
      return wrapped.hasExistingFiles();
    }

    @Override
    public Integer existingFilesCount() {
      return wrapped.existingFilesCount();
    }

    @Override
    public Long existingRowsCount() {
      return wrapped.existingRowsCount();
    }

    @Override
    public boolean hasDeletedFiles() {
      return wrapped.hasDeletedFiles();
    }

    @Override
    public Integer deletedFilesCount() {
      return wrapped.deletedFilesCount();
    }

    @Override
    public Long deletedRowsCount() {
      return wrapped.deletedRowsCount();
    }

    @Override
    public List<PartitionFieldSummary> partitions() {
      return wrapped.partitions();
    }

    @Override
    public ByteBuffer keyMetadata() {
      return wrapped.keyMetadata();
    }

    @Override
    public Long firstRowId() {
      return wrapped.firstRowId();
    }

    @Override
    public ManifestFile copy() {
      return wrapped.copy();
    }
  }

  static Schema entrySchema(Types.StructType partitionType) {
    return wrapFileSchema(fileType(partitionType));
  }

  static Schema wrapFileSchema(Types.StructType fileSchema) {
    // this is used to build projection schemas
    return new Schema(
        ManifestEntry.STATUS,
        ManifestEntry.SNAPSHOT_ID,
        ManifestEntry.SEQUENCE_NUMBER,
        ManifestEntry.FILE_SEQUENCE_NUMBER,
        required(ManifestEntry.DATA_FILE_ID, "data_file", fileSchema));
  }

  static Types.StructType fileType(Types.StructType partitionType) {
    // Parquet cannot represent empty groups, so the partition field is omitted entirely from
    // the file schema for unpartitioned tables. DataFileWrapper adjusts positions to match.
    ImmutableList.Builder<Types.NestedField> fields =
        ImmutableList.builderWithExpectedSize(partitionType.fields().isEmpty() ? 18 : 19);
    fields.add(DataFile.CONTENT.asRequired());
    fields.add(DataFile.FILE_PATH);
    fields.add(DataFile.FILE_FORMAT);
    if (!partitionType.fields().isEmpty()) {
      fields.add(
          required(
              DataFile.PARTITION_ID,
              DataFile.PARTITION_NAME,
              partitionType,
              DataFile.PARTITION_DOC));
    }
    fields.add(DataFile.RECORD_COUNT);
    fields.add(DataFile.FILE_SIZE);
    fields.add(DataFile.COLUMN_SIZES);
    fields.add(DataFile.VALUE_COUNTS);
    fields.add(DataFile.NULL_VALUE_COUNTS);
    fields.add(DataFile.NAN_VALUE_COUNTS);
    fields.add(DataFile.LOWER_BOUNDS);
    fields.add(DataFile.UPPER_BOUNDS);
    fields.add(DataFile.KEY_METADATA);
    fields.add(DataFile.SPLIT_OFFSETS);
    fields.add(DataFile.EQUALITY_IDS);
    fields.add(DataFile.SORT_ORDER_ID);
    fields.add(DataFile.FIRST_ROW_ID);
    fields.add(DataFile.REFERENCED_DATA_FILE);
    fields.add(DataFile.CONTENT_OFFSET);
    fields.add(DataFile.CONTENT_SIZE);
    return Types.StructType.of(fields.build());
  }

  static class ManifestEntryWrapper<F extends ContentFile<F>>
      implements ManifestEntry<F>, StructLike {
    private final int size;
    private final Long commitSnapshotId;
    private final DataFileWrapper<?> fileWrapper;
    private ManifestEntry<F> wrapped = null;

    ManifestEntryWrapper(Long commitSnapshotId, Types.StructType partitionType) {
      this.size = entrySchema(partitionType).columns().size();
      this.commitSnapshotId = commitSnapshotId;
      this.fileWrapper = new DataFileWrapper<>(partitionType);
    }

    public ManifestEntryWrapper<F> wrap(ManifestEntry<F> entry) {
      this.wrapped = entry;
      return this;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify ManifestEntryWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      switch (pos) {
        case 0:
          return wrapped.status().id();
        case 1:
          return wrapped.snapshotId();
        case 2:
          if (wrapped.dataSequenceNumber() == null) {
            // if the entry's data sequence number is null,
            // then it will inherit the sequence number of the current commit.
            // to validate that this is correct, check that the snapshot id is either null (will
            // also be inherited) or that it matches the id of the current commit.
            Preconditions.checkState(
                wrapped.snapshotId() == null || wrapped.snapshotId().equals(commitSnapshotId),
                "Found unassigned sequence number for an entry from snapshot: %s",
                wrapped.snapshotId());

            // inheritance should work only for ADDED entries
            Preconditions.checkState(
                wrapped.status() == Status.ADDED,
                "Only entries with status ADDED can have null sequence number");

            return null;
          }
          return wrapped.dataSequenceNumber();
        case 3:
          return wrapped.fileSequenceNumber();
        case 4:
          return fileWrapper.wrap(wrapped.file());
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public Status status() {
      return wrapped.status();
    }

    @Override
    public Long snapshotId() {
      return wrapped.snapshotId();
    }

    @Override
    public void setSnapshotId(long snapshotId) {
      wrapped.setSnapshotId(snapshotId);
    }

    @Override
    public Long dataSequenceNumber() {
      return wrapped.dataSequenceNumber();
    }

    @Override
    public void setDataSequenceNumber(long dataSequenceNumber) {
      wrapped.setDataSequenceNumber(dataSequenceNumber);
    }

    @Override
    public Long fileSequenceNumber() {
      return wrapped.fileSequenceNumber();
    }

    @Override
    public void setFileSequenceNumber(long fileSequenceNumber) {
      wrapped.setFileSequenceNumber(fileSequenceNumber);
    }

    @Override
    public F file() {
      return wrapped.file();
    }

    @Override
    public ManifestEntry<F> copy() {
      return wrapped.copy();
    }

    @Override
    public ManifestEntry<F> copyWithoutStats() {
      return wrapped.copyWithoutStats();
    }
  }

  /** Wrapper used to write DataFile or DeleteFile to v4 metadata. */
  static class DataFileWrapper<F extends ContentFile<F>> extends Delegates.DelegatingContentFile<F>
      implements ContentFile<F>, StructLike {
    private static final int PARTITION_POSITION = 3;

    private final int size;
    private final boolean hasPartition;

    DataFileWrapper(Types.StructType partitionType) {
      super(null);
      this.hasPartition = !partitionType.fields().isEmpty();
      this.size = fileType(partitionType).fields().size();
    }

    @SuppressWarnings("unchecked")
    DataFileWrapper<F> wrap(ContentFile<?> file) {
      setWrapped((F) file);
      return this;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify DataFileWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      // when the partition field is omitted, positions at or after where it would appear
      // shift down by 1, so adjust back to the canonical field ordering
      int adjusted = hasPartition ? pos : (pos >= PARTITION_POSITION ? pos + 1 : pos);
      switch (adjusted) {
        case 0:
          return wrapped.content().id();
        case 1:
          return wrapped.location();
        case 2:
          return wrapped.format() != null ? wrapped.format().toString() : null;
        case 3:
          return wrapped.partition();
        case 4:
          return wrapped.recordCount();
        case 5:
          return wrapped.fileSizeInBytes();
        case 6:
          return wrapped.columnSizes();
        case 7:
          return wrapped.valueCounts();
        case 8:
          return wrapped.nullValueCounts();
        case 9:
          return wrapped.nanValueCounts();
        case 10:
          return wrapped.lowerBounds();
        case 11:
          return wrapped.upperBounds();
        case 12:
          return wrapped.keyMetadata();
        case 13:
          return wrapped.splitOffsets();
        case 14:
          return wrapped.equalityFieldIds();
        case 15:
          return wrapped.sortOrderId();
        case 16:
          if (wrapped.content() == FileContent.DATA) {
            return wrapped.firstRowId();
          } else {
            return null;
          }
        case 17:
          if (wrapped.content() == FileContent.POSITION_DELETES) {
            return ((DeleteFile) wrapped).referencedDataFile();
          } else {
            return null;
          }
        case 18:
          if (wrapped.content() == FileContent.POSITION_DELETES) {
            return ((DeleteFile) wrapped).contentOffset();
          } else {
            return null;
          }
        case 19:
          if (wrapped.content() == FileContent.POSITION_DELETES) {
            return ((DeleteFile) wrapped).contentSizeInBytes();
          } else {
            return null;
          }
      }
      throw new IllegalArgumentException("Unknown field ordinal: " + pos);
    }

    @Override
    public String manifestLocation() {
      return null;
    }

    @Override
    public Long pos() {
      return null;
    }
  }

  /** Base wrapper used to write a {@link TrackedFile} using the v4 tracked file schema. */
  private static void checkNullSequenceNumbers(TrackedFile file, Tracking tracking) {
    // null sequence numbers are inherited from the commit: ADDED entries inherit both and
    // MODIFIED entries inherit only the data sequence number
    EntryStatus status = tracking.status();
    Preconditions.checkState(
        tracking.dataSequenceNumber() != null
            || status == EntryStatus.ADDED
            || status == EntryStatus.MODIFIED,
        "Cannot write null data sequence number for status %s (must be ADDED or MODIFIED): %s",
        status,
        file.location());
    Preconditions.checkState(
        tracking.fileSequenceNumber() != null || status == EntryStatus.ADDED,
        "Cannot write null file sequence number for status %s (must be ADDED): %s",
        status,
        file.location());
  }

  private abstract static class TrackedFileWrapper implements TrackedFile, StructLike {
    private static final int SIZE =
        TrackedFile.schema(Types.StructType.of(), Types.StructType.of()).columns().size();

    private final Map<Integer, PartitionSpec> specsById;
    private final Map<Integer, StructProjection> partitionProjections;
    private final ContentStatsWrapper contentStats;
    private final DeletionVectorWrapper deletionVector = new DeletionVectorWrapper();

    private TrackedFile wrapped = null;
    private Integer specId = null;
    private TrackingWrapper tracking = null;
    private ManifestInfoWrapper manifestInfo = null;

    private TrackedFileWrapper(
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType) {
      this.specsById = specsById;
      this.partitionProjections = partitionProjections(specsById.values(), partitionType);
      this.contentStats = new ContentStatsWrapper(contentStatsType);
    }

    protected void wrapInternal(
        TrackedFile file, TrackingWrapper newTracking, ManifestInfoWrapper newManifestInfo) {
      Integer fileSpecId = file.specId();
      PartitionSpec spec = fileSpecId != null ? specsById.get(fileSpecId) : null;
      Preconditions.checkArgument(
          fileSpecId == null || spec != null,
          "Cannot find partition spec for spec ID: %s",
          fileSpecId);

      this.wrapped = file;
      // v4 metadata stores a null spec ID and partition for unpartitioned files
      this.specId = spec != null && !spec.isUnpartitioned() ? fileSpecId : null;
      this.tracking = newTracking;
      this.manifestInfo = newManifestInfo;
    }

    @Override
    public int size() {
      return SIZE;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify TrackedFileWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      return switch (pos) {
        case 0 -> tracking;
        case 1 -> wrapped.contentType().id();
        case 2 -> wrapped.location();
        case 3 -> wrapped.fileFormat().toString();
        case 4 -> wrapped.recordCount();
        case 5 -> wrapped.fileSizeInBytes();
        case 6 -> specId;
        case 7 -> unionPartition();
        case 8 -> wrapped.contentStats() != null ? contentStats.wrap(wrapped.contentStats()) : null;
        case 9 -> wrapped.sortOrderId();
        case 10 ->
            wrapped.deletionVector() != null ? deletionVector.wrap(wrapped.deletionVector()) : null;
        case 11 -> manifestInfo;
        case 12 -> wrapped.keyMetadata();
        case 13 -> wrapped.splitOffsets();
        case 14 -> wrapColumnFiles(wrapped.columnFiles());
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
    }

    @Override
    public Tracking tracking() {
      return tracking;
    }

    @Override
    public FileContent contentType() {
      return wrapped.contentType();
    }

    @Override
    public String location() {
      return wrapped.location();
    }

    @Override
    public FileFormat fileFormat() {
      return wrapped.fileFormat();
    }

    @Override
    public long recordCount() {
      return wrapped.recordCount();
    }

    @Override
    public long fileSizeInBytes() {
      return wrapped.fileSizeInBytes();
    }

    @Override
    public Integer specId() {
      return specId;
    }

    @Override
    public StructLike partition() {
      return specId != null ? wrapped.partition() : null;
    }

    @Override
    public ContentStats contentStats() {
      return wrapped.contentStats();
    }

    @Override
    public Integer sortOrderId() {
      return wrapped.sortOrderId();
    }

    @Override
    public DeletionVector deletionVector() {
      return wrapped.deletionVector();
    }

    @Override
    public ManifestInfo manifestInfo() {
      return manifestInfo;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return wrapped.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
      return wrapped.splitOffsets();
    }

    @Override
    public List<ColumnFile> columnFiles() {
      return wrapped.columnFiles();
    }

    @Override
    public TrackedFile copy() {
      return copy(null /* all stats */);
    }

    @Override
    public TrackedFile copyWithStats(Set<Integer> requestedColumnIds) {
      return copy(requestedColumnIds);
    }

    private TrackedFile copy(Set<Integer> statsIds) {
      ContentStats stats = wrapped.contentStats();
      ContentStats statsCopy;
      if (stats == null || (statsIds != null && statsIds.isEmpty())) {
        statsCopy = null;
      } else {
        statsCopy = statsIds != null ? stats.copy(statsIds) : stats.copy();
      }

      DeletionVector dv = wrapped.deletionVector();
      return new TrackedFileStruct(
          tracking.copy(),
          wrapped.contentType(),
          wrapped.location(),
          wrapped.fileFormat(),
          wrapped.recordCount(),
          wrapped.fileSizeInBytes(),
          specId,
          StructLikeUtil.copy(partition()),
          statsCopy,
          wrapped.sortOrderId(),
          dv != null ? dv.copy() : null,
          manifestInfo != null ? manifestInfo.copy() : null,
          wrapped.keyMetadata(),
          wrapped.splitOffsets(),
          copyColumnFiles(wrapped.columnFiles()));
    }

    private static List<ColumnFile> copyColumnFiles(List<ColumnFile> columnFiles) {
      if (columnFiles == null) {
        return null;
      }

      List<ColumnFile> copies = Lists.newArrayListWithCapacity(columnFiles.size());
      for (ColumnFile columnFile : columnFiles) {
        copies.add(columnFile.copy());
      }

      return copies;
    }

    private static List<StructLike> wrapColumnFiles(List<ColumnFile> columnFiles) {
      if (columnFiles == null) {
        return null;
      }

      List<StructLike> wrappers = Lists.newArrayListWithCapacity(columnFiles.size());
      for (ColumnFile columnFile : columnFiles) {
        wrappers.add(new ColumnFileWrapper(columnFile));
      }

      return wrappers;
    }

    private StructLike unionPartition() {
      if (specId == null) {
        return null;
      }

      StructProjection projection = partitionProjections.get(specId);
      return projection != null ? projection.wrap(wrapped.partition()) : wrapped.partition();
    }

    private static Map<Integer, StructProjection> partitionProjections(
        Iterable<PartitionSpec> specs, Types.StructType unionType) {
      Map<Integer, StructProjection> projections = Maps.newHashMap();
      for (PartitionSpec spec : specs) {
        Types.StructType partitionType = spec.partitionType();
        for (Types.NestedField field : partitionType.fields()) {
          Preconditions.checkArgument(
              unionType.field(field.fieldId()) != null,
              "Invalid union partition type, missing field %s from spec %s: %s",
              field.fieldId(),
              spec.specId(),
              unionType);
        }

        StructProjection projection =
            partitionType.equals(unionType)
                ? null
                : StructProjection.createAllowMissing(partitionType, unionType);
        projections.put(spec.specId(), projection);
      }

      return projections;
    }
  }

  /** Wrapper used to write a data file to a v4 leaf manifest. */
  static class LeafTrackedFileWrapper extends TrackedFileWrapper {
    private final TrackingWrapper trackingWrapper = new TrackingWrapper();

    LeafTrackedFileWrapper(
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType) {
      super(specsById, partitionType, contentStatsType);
    }

    LeafTrackedFileWrapper wrap(TrackedFile file, Tracking newTracking) {
      Preconditions.checkArgument(file != null, "Invalid file: null");
      Preconditions.checkArgument(newTracking != null, "Invalid tracking: null");
      Preconditions.checkArgument(
          file.contentType() == FileContent.DATA,
          "Invalid content type for leaf manifest: %s",
          file.contentType());

      checkNullSequenceNumbers(file, newTracking);

      trackingWrapper.wrap(
          newTracking,
          newTracking.snapshotId(),
          newTracking.dataSequenceNumber(),
          newTracking.fileSequenceNumber(),
          newTracking.firstRowId());
      wrapInternal(file, trackingWrapper, null /* manifestInfo */);
      return this;
    }
  }

  /** Wrapper used to write a data file or leaf manifest to a v4 root manifest. */
  static class RootTrackedFileWrapper extends TrackedFileWrapper {
    private final long commitSnapshotId;
    private final long commitSequenceNumber;
    private final TrackingWrapper trackingWrapper = new TrackingWrapper();
    private final ManifestInfoWrapper manifestInfoWrapper = new ManifestInfoWrapper();

    RootTrackedFileWrapper(
        long commitSnapshotId,
        long commitSequenceNumber,
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType) {
      super(specsById, partitionType, contentStatsType);
      this.commitSnapshotId = commitSnapshotId;
      this.commitSequenceNumber = commitSequenceNumber;
    }

    /**
     * Re-points this wrapper at a file and the tracking to write for it.
     *
     * @param file a data file or manifest
     * @param newTracking the tracking to write for the file
     * @param assignedFirstRowId a first row ID to assign to the file, or null if it is already
     *     assigned
     * @return this for method chaining
     */
    RootTrackedFileWrapper wrap(TrackedFile file, Tracking newTracking, Long assignedFirstRowId) {
      Preconditions.checkArgument(file != null, "Invalid file: null");
      Preconditions.checkArgument(newTracking != null, "Invalid tracking: null");

      FileContent content = file.contentType();
      Preconditions.checkArgument(
          content == FileContent.DATA
              || content == FileContent.DATA_MANIFEST
              || content == FileContent.DELETE_MANIFEST,
          "Invalid content type for root manifest: %s",
          content);
      boolean isManifest = content != FileContent.DATA;
      Preconditions.checkArgument(
          !isManifest || file.manifestInfo() != null,
          "Missing manifest info for manifest: %s",
          file.location());
      Preconditions.checkArgument(
          newTracking.snapshotId() != null, "Missing snapshot ID for file: %s", file.location());
      checkNullSequenceNumbers(file, newTracking);

      long snapshotId = newTracking.snapshotId();
      // a modified entry inherits the data sequence number of the commit that modified it
      Long dataSequenceSnapshotId =
          newTracking.status() == EntryStatus.MODIFIED
              ? newTracking.modifiedSnapshotId()
              : newTracking.snapshotId();
      trackingWrapper.wrap(
          newTracking,
          snapshotId,
          resolveSequenceNumber(file, newTracking.dataSequenceNumber(), dataSequenceSnapshotId),
          resolveSequenceNumber(file, newTracking.fileSequenceNumber(), snapshotId),
          resolveFirstRowId(file, newTracking, assignedFirstRowId));

      if (isManifest) {
        ManifestInfo info = file.manifestInfo();
        manifestInfoWrapper.wrap(
            info, resolveSequenceNumber(file, info.minSequenceNumber(), snapshotId));
        wrapInternal(file, trackingWrapper, manifestInfoWrapper);
      } else {
        wrapInternal(file, trackingWrapper, null /* manifestInfo */);
      }

      return this;
    }

    private long resolveSequenceNumber(
        TrackedFile file, Long existingSequenceNumber, Long entrySnapshotId) {
      if (existingSequenceNumber == null
          || existingSequenceNumber == ManifestWriter.UNASSIGNED_SEQ) {
        // the sequence number is assigned here, so the file must be changed by the current commit
        Preconditions.checkState(
            entrySnapshotId != null && entrySnapshotId == commitSnapshotId,
            "Cannot assign sequence number to file from snapshot %s (committing snapshot %s): %s",
            entrySnapshotId,
            commitSnapshotId,
            file.location());
        return commitSequenceNumber;
      }

      return existingSequenceNumber;
    }

    private static Long resolveFirstRowId(
        TrackedFile file, Tracking newTracking, Long assignedFirstRowId) {
      if (file.contentType() == FileContent.DELETE_MANIFEST) {
        Preconditions.checkState(
            assignedFirstRowId == null,
            "Cannot assign first-row-id to delete manifest: %s",
            file.location());
        return null;
      }

      if (assignedFirstRowId != null) {
        Preconditions.checkState(
            newTracking.status().isLive(),
            "Cannot assign first-row-id to file with status %s: %s",
            newTracking.status(),
            file.location());
        Preconditions.checkState(
            newTracking.firstRowId() == null,
            "Cannot assign first-row-id to file with existing first-row-id %s: %s",
            newTracking.firstRowId(),
            file.location());
        return assignedFirstRowId;
      }

      Preconditions.checkState(
          newTracking.firstRowId() != null, "Missing first-row-id for file: %s", file.location());
      return newTracking.firstRowId();
    }
  }

  /** Wrapper used to write {@link Tracking} using the v4 tracking schema. */
  private static class TrackingWrapper implements Tracking, StructLike {
    private static final int SIZE = Tracking.schema().fields().size();

    private Tracking wrapped = null;
    private Long snapshotId = null;
    private Long dataSequenceNumber = null;
    private Long fileSequenceNumber = null;
    private Long firstRowId = null;

    private TrackingWrapper wrap(
        Tracking tracking,
        Long newSnapshotId,
        Long newDataSequenceNumber,
        Long newFileSequenceNumber,
        Long newFirstRowId) {
      this.wrapped = tracking;
      this.snapshotId = newSnapshotId;
      this.dataSequenceNumber = newDataSequenceNumber;
      this.fileSequenceNumber = newFileSequenceNumber;
      this.firstRowId = newFirstRowId;
      return this;
    }

    @Override
    public int size() {
      return SIZE;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify TrackingWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      return switch (pos) {
        case 0 -> wrapped.status().id();
        case 1 -> snapshotId;
        case 2 -> dataSequenceNumber;
        case 3 -> fileSequenceNumber;
        case 4 -> wrapped.modifiedSnapshotId();
        case 5 -> firstRowId;
        case 6 -> wrapped.deletedPositions();
        case 7 -> wrapped.replacedPositions();
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
    }

    @Override
    public EntryStatus status() {
      return wrapped.status();
    }

    @Override
    public Long snapshotId() {
      return snapshotId;
    }

    @Override
    public Long dataSequenceNumber() {
      return dataSequenceNumber;
    }

    @Override
    public Long fileSequenceNumber() {
      return fileSequenceNumber;
    }

    @Override
    public Long modifiedSnapshotId() {
      return wrapped.modifiedSnapshotId();
    }

    @Override
    public Long firstRowId() {
      return firstRowId;
    }

    @Override
    public ByteBuffer deletedPositions() {
      return wrapped.deletedPositions();
    }

    @Override
    public ByteBuffer replacedPositions() {
      return wrapped.replacedPositions();
    }

    @Override
    public String manifestLocation() {
      return wrapped.manifestLocation();
    }

    @Override
    public long manifestPos() {
      return wrapped.manifestPos();
    }

    @Override
    public Tracking copy() {
      return new TrackingStruct(
          wrapped.status(),
          snapshotId,
          dataSequenceNumber,
          fileSequenceNumber,
          wrapped.modifiedSnapshotId(),
          firstRowId,
          ByteBuffers.toByteArray(wrapped.deletedPositions()),
          ByteBuffers.toByteArray(wrapped.replacedPositions()));
    }
  }

  /** Wrapper used to write {@link ManifestInfo} using the v4 manifest info schema. */
  private static class ManifestInfoWrapper implements ManifestInfo, StructLike {
    private static final int SIZE = ManifestInfo.schema().fields().size();

    private ManifestInfo wrapped = null;
    private long minSequenceNumber = ManifestWriter.UNASSIGNED_SEQ;

    private ManifestInfoWrapper wrap(ManifestInfo info, long newMinSequenceNumber) {
      this.wrapped = info;
      this.minSequenceNumber = newMinSequenceNumber;
      return this;
    }

    @Override
    public int size() {
      return SIZE;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify ManifestInfoWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      return switch (pos) {
        case 0 -> wrapped.addedFilesCount();
        case 1 -> wrapped.existingFilesCount();
        case 2 -> wrapped.deletedFilesCount();
        case 3 -> wrapped.replacedFilesCount();
        case 4 -> wrapped.modifiedFilesCount();
        case 5 -> wrapped.addedRowsCount();
        case 6 -> wrapped.existingRowsCount();
        case 7 -> wrapped.deletedRowsCount();
        case 8 -> wrapped.replacedRowsCount();
        case 9 -> wrapped.modifiedRowsCount();
        case 10 -> minSequenceNumber;
        case 11 -> mdvBuffer();
        case 12 -> wrapped.formatVersion();
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
    }

    private ByteBuffer mdvBuffer() {
      ManifestBitmap mdv = wrapped.manifestDeletionVector();
      return mdv != null ? mdv.buffer() : null;
    }

    @Override
    public int addedFilesCount() {
      return wrapped.addedFilesCount();
    }

    @Override
    public int existingFilesCount() {
      return wrapped.existingFilesCount();
    }

    @Override
    public int deletedFilesCount() {
      return wrapped.deletedFilesCount();
    }

    @Override
    public int replacedFilesCount() {
      return wrapped.replacedFilesCount();
    }

    @Override
    public int modifiedFilesCount() {
      return wrapped.modifiedFilesCount();
    }

    @Override
    public long addedRowsCount() {
      return wrapped.addedRowsCount();
    }

    @Override
    public long existingRowsCount() {
      return wrapped.existingRowsCount();
    }

    @Override
    public long deletedRowsCount() {
      return wrapped.deletedRowsCount();
    }

    @Override
    public long replacedRowsCount() {
      return wrapped.replacedRowsCount();
    }

    @Override
    public long modifiedRowsCount() {
      return wrapped.modifiedRowsCount();
    }

    @Override
    public long minSequenceNumber() {
      return minSequenceNumber;
    }

    @Override
    public ManifestBitmap manifestDeletionVector() {
      return wrapped.manifestDeletionVector();
    }

    @Override
    public int formatVersion() {
      return wrapped.formatVersion();
    }

    @Override
    public ManifestInfo copy() {
      return new ManifestInfoStruct(
          wrapped.addedFilesCount(),
          wrapped.existingFilesCount(),
          wrapped.deletedFilesCount(),
          wrapped.replacedFilesCount(),
          wrapped.modifiedFilesCount(),
          wrapped.addedRowsCount(),
          wrapped.existingRowsCount(),
          wrapped.deletedRowsCount(),
          wrapped.replacedRowsCount(),
          wrapped.modifiedRowsCount(),
          minSequenceNumber,
          ByteBuffers.toByteArray(mdvBuffer()),
          wrapped.formatVersion());
    }
  }

  /** Wrapper used to write {@link ContentStats} using a manifest's content stats type. */
  private static class ContentStatsWrapper implements StructLike {
    private final int[] posToFieldId;
    private final FieldStatsWrapper[] fieldStats;
    private ContentStats wrapped = null;

    private ContentStatsWrapper(Types.StructType type) {
      List<Types.NestedField> fields = type.fields();
      this.posToFieldId = ContentStatsStruct.posToId(type);
      this.fieldStats = new FieldStatsWrapper[fields.size()];
      for (int pos = 0; pos < fields.size(); pos += 1) {
        fieldStats[pos] = new FieldStatsWrapper(fields.get(pos).type().asStructType());
      }
    }

    private ContentStatsWrapper wrap(ContentStats stats) {
      this.wrapped = stats;
      return this;
    }

    @Override
    public int size() {
      return posToFieldId.length;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify ContentStatsWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      FieldStats<?> stats = wrapped.statsFor(posToFieldId[pos]);
      return stats != null ? javaClass.cast(fieldStats[pos].wrap(stats)) : null;
    }
  }

  /** Wrapper used to write {@link DeletionVector} using the v4 deletion vector schema. */
  private static class DeletionVectorWrapper implements StructLike {
    private static final int SIZE = DeletionVector.schema().fields().size();

    private DeletionVector wrapped = null;

    private DeletionVectorWrapper wrap(DeletionVector dv) {
      this.wrapped = dv;
      return this;
    }

    @Override
    public int size() {
      return SIZE;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException(
          "Cannot modify DeletionVectorWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      return switch (pos) {
        case 0 -> wrapped.location();
        case 1 -> wrapped.offset();
        case 2 -> wrapped.sizeInBytes();
        case 3 -> wrapped.cardinality();
        case 4 -> wrapped.keyMetadata();
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
    }
  }

  /** Wrapper used to write {@link ColumnFile} using the v4 column file schema. */
  private static class ColumnFileWrapper implements StructLike {
    private static final int SIZE = ColumnFile.schema().fields().size();

    private final ColumnFile wrapped;

    private ColumnFileWrapper(ColumnFile columnFile) {
      this.wrapped = columnFile;
    }

    @Override
    public int size() {
      return SIZE;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify ColumnFileWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      return switch (pos) {
        case 0 -> wrapped.location();
        case 1 -> ImmutableList.copyOf(wrapped.fieldIds());
        case 2 -> wrapped.fileFormat().toString();
        case 3 -> wrapped.fileSizeInBytes();
        case 4 -> wrapped.keyMetadata();
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
    }
  }

  /** Wrapper used to write {@link FieldStats} using a manifest's field stats type. */
  private static class FieldStatsWrapper implements StructLike {
    private final int[] posToOffset;
    private FieldStats<?> wrapped = null;

    private FieldStatsWrapper(Types.StructType type) {
      this.posToOffset = FieldStatsStruct.posToOffset(type);
    }

    private FieldStatsWrapper wrap(FieldStats<?> stats) {
      this.wrapped = stats;
      return this;
    }

    @Override
    public int size() {
      return posToOffset.length;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify FieldStatsWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(posToOffset[pos]));
    }

    private Object get(int offset) {
      return switch (offset) {
        case StatsUtil.LOWER_BOUND_OFFSET -> wrapped.lowerBound();
        case StatsUtil.UPPER_BOUND_OFFSET -> wrapped.upperBound();
        case StatsUtil.TIGHT_BOUNDS_OFFSET -> wrapped.tightBounds();
        case StatsUtil.VALUE_COUNT_OFFSET -> wrapped.hasValueCount() ? wrapped.valueCount() : null;
        case StatsUtil.NULL_VALUE_COUNT_OFFSET ->
            wrapped.hasNullValueCount() ? wrapped.nullValueCount() : null;
        case StatsUtil.NAN_VALUE_COUNT_OFFSET ->
            wrapped.hasNanValueCount() ? wrapped.nanValueCount() : null;
        case StatsUtil.TOTAL_BYTES_OFFSET -> wrapped.hasTotalBytes() ? wrapped.totalBytes() : null;
        default -> throw new UnsupportedOperationException("Unsupported stats offset: " + offset);
      };
    }
  }
}
