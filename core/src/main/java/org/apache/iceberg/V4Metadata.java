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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
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
  private abstract static class TrackedFileWrapper implements StructLike {
    private static final int SIZE =
        TrackedFile.schema(Types.StructType.of(), Types.StructType.of()).columns().size();

    private final Map<Integer, PartitionSpec> specsById;
    private final Map<Integer, StructProjection> partitionProjections;
    private final ContentStatsWrapper statsWrapper;
    private final DeletionVectorWrapper dvWrapper = new DeletionVectorWrapper();
    private final ManifestInfoWrapper manifestInfoWrapper;
    private final List<ColumnFileWrapper> columnFileWrappers = Lists.newArrayList();

    private TrackedFile wrapped = null;
    private TrackingWrapper trackingWrapper = null;
    private Integer specId = null;

    private TrackedFileWrapper(
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType,
        ManifestInfoWrapper manifestInfo) {
      validatePartitionType(specsById.values(), partitionType);
      this.specsById = specsById;
      this.partitionProjections = partitionProjections(specsById.values(), partitionType);
      this.statsWrapper = new ContentStatsWrapper(contentStatsType);
      this.manifestInfoWrapper = manifestInfo;
    }

    protected void wrapInternal(TrackedFile file, TrackingWrapper wrappedTracking) {
      validateTypeSpecificMetadata(file);

      this.wrapped = file;
      this.trackingWrapper = wrappedTracking;

      Integer fileSpecId = file.specId();
      PartitionSpec spec = fileSpecId != null ? specsById.get(fileSpecId) : null;
      Preconditions.checkArgument(
          fileSpecId == null || spec != null,
          "Cannot find partition spec for spec ID: %s",
          fileSpecId);
      this.specId = spec != null && !spec.isUnpartitioned() ? fileSpecId : null;
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
        case 0 -> trackingWrapper;
        case 1 -> wrapped.contentType().id();
        case 2 -> wrapped.location();
        case 3 -> wrapped.fileFormat().toString();
        case 4 -> wrapped.recordCount();
        case 5 -> wrapped.fileSizeInBytes();
        case 6 -> specId;
        case 7 -> unionPartition();
        case 8 -> statsWrapper.wrap(wrapped.contentStats());
        case 9 -> wrapped.tightStats();
        case 10 -> wrapped.sortOrderId();
        case 11 -> dvWrapper.wrap(wrapped.deletionVector());
        case 12 -> manifestInfoWrapper.wrap(wrapped.manifestInfo());
        case 13 -> wrapped.keyMetadata();
        case 14 -> wrapped.splitOffsets();
        case 15 -> wrapColumnFiles(wrapped.columnFiles());
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
    }

    private List<ColumnFileWrapper> wrapColumnFiles(List<ColumnFile> columnFiles) {
      if (columnFiles == null) {
        return null;
      }

      for (int pos = columnFileWrappers.size(); pos < columnFiles.size(); pos += 1) {
        columnFileWrappers.add(new ColumnFileWrapper());
      }

      for (int pos = 0; pos < columnFiles.size(); pos += 1) {
        columnFileWrappers.get(pos).wrap(columnFiles.get(pos));
      }

      return columnFileWrappers.subList(0, columnFiles.size());
    }

    protected void validateTypeSpecificMetadata(TrackedFile file) {
      FileContent content = file.contentType();
      if (content == FileContent.DATA) {
        Preconditions.checkArgument(
            file.manifestInfo() == null,
            "Invalid data file, manifest info must be null: %s",
            file.location());
      } else if (content == FileContent.DATA_MANIFEST || content == FileContent.DELETE_MANIFEST) {
        Preconditions.checkArgument(
            file.manifestInfo() != null,
            "Invalid manifest, missing manifest info: %s",
            file.location());
        Preconditions.checkArgument(
            file.deletionVector() == null,
            "Invalid manifest, deletion vector must be null: %s",
            file.location());
      }
    }

    private StructLike unionPartition() {
      if (specId == null) {
        return null;
      }

      StructProjection projection = partitionProjections.get(specId);
      // projection is null when the spec's partition type already matches the union type
      return projection != null ? projection.wrap(wrapped.partition()) : wrapped.partition();
    }

    private static void validatePartitionType(
        Iterable<PartitionSpec> specs, Types.StructType unionType) {
      for (PartitionSpec spec : specs) {
        for (Types.NestedField field : spec.partitionType().fields()) {
          Preconditions.checkArgument(
              unionType.field(field.fieldId()) != null,
              "Invalid union partition type, missing field %s from spec %s: %s",
              field.fieldId(),
              spec.specId(),
              unionType);
        }
      }
    }

    private static Map<Integer, StructProjection> partitionProjections(
        Iterable<PartitionSpec> specs, Types.StructType unionType) {
      Map<Integer, StructProjection> projections = Maps.newHashMap();
      for (PartitionSpec spec : specs) {
        Types.StructType partitionType = spec.partitionType();
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
    private final LeafTrackingWrapper trackingWrapper;

    LeafTrackedFileWrapper(
        Long snapshotId,
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType) {
      super(
          specsById,
          partitionType,
          contentStatsType,
          new ManifestInfoWrapper(null /* sequence numbers are inherited */));
      this.trackingWrapper = new LeafTrackingWrapper(snapshotId);
    }

    LeafTrackedFileWrapper wrap(TrackedFile file, Tracking tracking) {
      Preconditions.checkArgument(file != null, "Invalid file: null");
      Preconditions.checkArgument(tracking != null, "Invalid tracking: null");
      Preconditions.checkArgument(
          file.contentType() == FileContent.DATA,
          "Invalid content type for leaf manifest: %s",
          file.contentType());

      validateFirstRowId(file, tracking);

      wrapInternal(file, trackingWrapper.wrap(tracking));

      return this;
    }

    private static void validateFirstRowId(TrackedFile file, Tracking tracking) {
      if (tracking.status() == EntryStatus.ADDED) {
        Preconditions.checkArgument(
            tracking.firstRowId() == null,
            "Invalid first-row-id for ADDED file (must be null): %s",
            file.location());
      } else {
        Preconditions.checkArgument(
            tracking.firstRowId() != null,
            "Missing first-row-id for %s file: %s",
            tracking.status(),
            file.location());
      }
    }
  }

  /** Wrapper used to write a data file or leaf manifest to a v4 root manifest. */
  static class RootTrackedFileWrapper extends TrackedFileWrapper {
    private final RootTrackingWrapper trackingWrapper;

    RootTrackedFileWrapper(
        long commitSnapshotId,
        long commitSequenceNumber,
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType) {
      super(
          specsById,
          partitionType,
          contentStatsType,
          new ManifestInfoWrapper(commitSequenceNumber));
      this.trackingWrapper = new RootTrackingWrapper(commitSnapshotId, commitSequenceNumber);
    }

    RootTrackedFileWrapper wrap(TrackedFile file, Tracking tracking, Long assignedFirstRowId) {
      Preconditions.checkArgument(file != null, "Invalid file: null");
      Preconditions.checkArgument(tracking != null, "Invalid tracking: null");
      Preconditions.checkArgument(
          file.contentType() == FileContent.DATA
              || file.contentType() == FileContent.DATA_MANIFEST
              || file.contentType() == FileContent.DELETE_MANIFEST,
          "Invalid content type for root manifest: %s",
          file.contentType());

      validateFirstRowId(file, tracking, assignedFirstRowId);

      wrapInternal(file, trackingWrapper.wrap(tracking, assignedFirstRowId, file.location()));

      return this;
    }

    private static void validateFirstRowId(
        TrackedFile file, Tracking tracking, Long assignedFirstRowId) {
      FileContent content = file.contentType();
      if (content == FileContent.DATA || content == FileContent.DATA_MANIFEST) {
        if (tracking.status() == EntryStatus.ADDED) {
          Preconditions.checkArgument(
              tracking.firstRowId() == null,
              "Invalid first-row-id for ADDED file (must be null): %s",
              file.location());
          Preconditions.checkArgument(
              assignedFirstRowId != null,
              "Missing first-row-id assignment for ADDED file: %s",
              file.location());
        } else {
          Preconditions.checkArgument(
              tracking.firstRowId() != null,
              "Missing first-row-id for %s file: %s",
              tracking.status(),
              file.location());
          Preconditions.checkArgument(
              assignedFirstRowId == null,
              "Cannot assign first-row-id to %s file: %s",
              tracking.status(),
              file.location());
        }
      } else if (content == FileContent.DELETE_MANIFEST) {
        Preconditions.checkArgument(
            assignedFirstRowId == null,
            "Cannot assign first-row-id to delete manifest: %s",
            file.location());
        Preconditions.checkArgument(
            tracking.firstRowId() == null,
            "Invalid first-row-id for delete manifest (must be null): %s",
            file.location());
      }
    }
  }

  /** Wrapper used to write {@link Tracking} using the v4 tracking schema. */
  private static class TrackingWrapper implements StructLike {
    private static final int SIZE = Tracking.schema().fields().size();

    private final Long commitSnapshotId;
    private final Long commitSequenceNumber;

    private Tracking wrapped = null;
    private Long assignedFirstRowId = null;

    private TrackingWrapper(Long commitSnapshotId, Long commitSequenceNumber) {
      this.commitSnapshotId = commitSnapshotId;
      this.commitSequenceNumber = commitSequenceNumber;
    }

    protected void validateSequenceNumbers(Tracking tracking) {
      EntryStatus status = tracking.status();

      switch (status) {
        case ADDED -> {
          Preconditions.checkArgument(
              tracking.snapshotId() == null || tracking.snapshotId().equals(commitSnapshotId),
              "Found invalid snapshot ID for ADDED file: %s (committing %s)",
              tracking.snapshotId(),
              commitSnapshotId);
          Preconditions.checkArgument(
              tracking.fileSequenceNumber() == null,
              "Invalid file sequence number for ADDED file: %s",
              tracking.fileSequenceNumber());
        }
        case MODIFIED ->
            Preconditions.checkArgument(
                tracking.modifiedSnapshotId() != null
                    && tracking.modifiedSnapshotId().equals(commitSnapshotId),
                "Found invalid snapshot ID for MODIFIED file: %s (committing %s)",
                tracking.modifiedSnapshotId(),
                commitSnapshotId);
        default ->
            Preconditions.checkArgument(
                tracking.dataSequenceNumber() != null,
                "Invalid data sequence number (null) for %s file",
                status);
      }
    }

    protected TrackingWrapper wrapInternal(Tracking tracking, Long firstRowId) {
      this.wrapped = tracking;
      this.assignedFirstRowId = firstRowId;
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
        case 1 -> wrapped.snapshotId();
        case 2 -> sequenceNumber(wrapped.dataSequenceNumber());
        case 3 -> sequenceNumber(wrapped.fileSequenceNumber());
        case 4 -> wrapped.modifiedSnapshotId();
        case 5 -> firstRowId();
        case 6 -> wrapped.deletedPositions();
        case 7 -> wrapped.replacedPositions();
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
    }

    private Long sequenceNumber(Long sequenceNumber) {
      return sequenceNumber != null ? sequenceNumber : commitSequenceNumber;
    }

    private Long firstRowId() {
      return assignedFirstRowId != null ? assignedFirstRowId : wrapped.firstRowId();
    }
  }

  private static class LeafTrackingWrapper extends TrackingWrapper {
    private LeafTrackingWrapper(Long commitSnapshotId) {
      super(commitSnapshotId, null /* sequence numbers are inherited */);
    }

    private TrackingWrapper wrap(Tracking tracking) {
      validateSequenceNumbers(tracking);
      return wrapInternal(tracking, null /* first row ID is inherited */);
    }
  }

  private static class RootTrackingWrapper extends TrackingWrapper {
    private RootTrackingWrapper(long commitSnapshotId, long commitSequenceNumber) {
      super(commitSnapshotId, commitSequenceNumber);
    }

    private TrackingWrapper wrap(Tracking tracking, Long firstRowId, String location) {
      Preconditions.checkArgument(
          tracking.snapshotId() != null, "Missing snapshot ID for file: %s", location);
      validateSequenceNumbers(tracking);
      return wrapInternal(tracking, firstRowId);
    }
  }

  /** Wrapper used to write {@link ManifestInfo} using the v4 manifest info schema. */
  private static class ManifestInfoWrapper implements StructLike {
    private static final int SIZE = ManifestInfo.schema().fields().size();

    private final Long commitSequenceNumber;
    private ManifestInfo wrapped = null;

    private ManifestInfoWrapper(Long commitSequenceNumber) {
      this.commitSequenceNumber = commitSequenceNumber;
    }

    private ManifestInfoWrapper wrap(ManifestInfo info) {
      if (info == null) {
        return null;
      }

      this.wrapped = info;
      return this.wrapped != null ? this : null;
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
        case 10 -> minSequenceNumber();
        case 11 -> mdvBuffer();
        case 12 -> wrapped.formatVersion();
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
    }

    private long minSequenceNumber() {
      if (wrapped.minSequenceNumber() == ManifestWriter.UNASSIGNED_SEQ) {
        return commitSequenceNumber;
      }

      return wrapped.minSequenceNumber();
    }

    private ByteBuffer mdvBuffer() {
      ManifestBitmap mdv = wrapped.manifestDeletionVector();
      return mdv != null ? mdv.buffer() : null;
    }
  }

  /** Wrapper used to write {@link DeletionVector} using the v4 deletion vector schema. */
  private static class DeletionVectorWrapper implements StructLike {
    private static final int SIZE = DeletionVector.schema().fields().size();

    private DeletionVector wrapped = null;

    private DeletionVectorWrapper wrap(DeletionVector dv) {
      if (dv == null) {
        return null;
      }

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

    private ColumnFile wrapped = null;

    private ColumnFileWrapper wrap(ColumnFile columnFile) {
      this.wrapped = columnFile;
      return this;
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
        case 1 -> wrapped.fieldIds();
        case 2 -> wrapped.fileFormat().toString();
        case 3 -> wrapped.fileSizeInBytes();
        case 4 -> wrapped.keyMetadata();
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
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
      return this.wrapped != null ? this : null;
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
      return javaClass.cast(fieldStats[pos].wrap(stats));
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
      return this.wrapped != null ? this : null;
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
