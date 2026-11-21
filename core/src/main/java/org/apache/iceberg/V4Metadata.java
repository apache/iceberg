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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

class V4Metadata {
  private V4Metadata() {}

  /** Base wrapper used to write a {@link TrackedFile} using the v4 tracked file schema. */
  private abstract static class TrackedFileWrapper implements StructLike {
    private static final int SIZE =
        TrackedFile.schema(Types.StructType.of(), Types.StructType.of()).columns().size();

    private final Map<Integer, PartitionSpec> specsById;
    private final Map<Integer, StructProjection> partitionProjections;
    private final ContentStatsWrapper statsWrapper;
    private final DeletionVectorWrapper dvWrapper = new DeletionVectorWrapper();
    private final List<ColumnFileWrapper> columnFileWrappers = Lists.newArrayList();

    private TrackedFileWrapper(
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType) {
      validatePartitionType(specsById.values(), partitionType);
      this.specsById = specsById;
      this.partitionProjections = partitionProjections(specsById.values(), partitionType);
      this.statsWrapper = new ContentStatsWrapper(contentStatsType);
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
      return javaClass.cast(getInternal(pos));
    }

    protected abstract Object getInternal(int pos);

    protected List<ColumnFileWrapper> wrapColumnFiles(List<ColumnFile> columnFiles) {
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

    protected Integer specId(TrackedFile file) {
      Integer fileSpecId = file.specId();
      if (file.specId() != null) {
        PartitionSpec spec = specsById.get(fileSpecId);
        return spec.isUnpartitioned() ? null : fileSpecId;
      }

      return null;
    }

    protected StructLike unionPartition(TrackedFile file) {
      Integer specId = specId(file);
      if (specId != null) {
        StructProjection projection = partitionProjections.get(specId);
        // projection is null when the spec's partition type already matches the union type
        return projection != null ? projection.wrap(file.partition()) : file.partition();
      }

      return null;
    }

    protected ContentStatsWrapper wrapStats(ContentStats stats) {
      return statsWrapper.wrap(stats);
    }

    protected DeletionVectorWrapper wrapDV(DeletionVector dv) {
      return dvWrapper.wrap(dv);
    }

    protected void validatePartitionSpecId(TrackedFile file) {
      Integer fileSpecId = file.specId();
      if (fileSpecId != null) {
        PartitionSpec spec = specsById.get(fileSpecId);
        Preconditions.checkArgument(
            spec != null, "Cannot find partition spec for spec ID: %s", fileSpecId);
      }
    }

    protected static void validateTypeSpecificMetadata(TrackedFile file) {
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
            "Invalid manifest, data deletion vector must be null: %s",
            file.location());
      }
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

  static Schema leafManifestSchema(Types.StructType partitionType, Types.StructType statsType) {
    return new Schema(
        Types.NestedField.from(TrackedFile.TRACKING).ofType(LEAF_TRACKING_TYPE).build(),
        TrackedFile.CONTENT_TYPE,
        TrackedFile.LOCATION,
        TrackedFile.FILE_FORMAT,
        TrackedFile.RECORD_COUNT,
        TrackedFile.FILE_SIZE_IN_BYTES,
        TrackedFile.SPEC_ID,
        Types.NestedField.optional(
            TrackedFile.PARTITION_ID,
            TrackedFile.PARTITION_NAME,
            partitionType.fields().isEmpty() ? Types.UnknownType.get() : partitionType,
            TrackedFile.PARTITION_DOC),
        Types.NestedField.optional(
            TrackedFile.CONTENT_STATS_ID,
            TrackedFile.CONTENT_STATS_NAME,
            statsType.fields().isEmpty() ? Types.UnknownType.get() : statsType,
            TrackedFile.CONTENT_STATS_DOC),
        TrackedFile.TIGHT_STATS,
        TrackedFile.SORT_ORDER_ID,
        Types.NestedField.from(TrackedFile.DELETION_VECTOR).ofType(DELETION_VECTOR_TYPE).build(),
        TrackedFile.KEY_METADATA,
        TrackedFile.SPLIT_OFFSETS,
        Types.NestedField.from(TrackedFile.COLUMN_FILES).ofType(COLUMN_FILES_LIST_TYPE).build());
  }

  /** Wrapper used to write a data file to a v4 leaf manifest. */
  static class LeafTrackedFileWrapper extends TrackedFileWrapper {
    private final LeafTrackingWrapper trackingWrapper;
    private TrackedFile wrapped;

    LeafTrackedFileWrapper(
        Long snapshotId,
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType) {
      super(specsById, partitionType, contentStatsType);
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
      validateTypeSpecificMetadata(file);
      validatePartitionSpecId(file);

      this.wrapped = file;
      trackingWrapper.wrap(tracking);

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

    @Override
    protected Object getInternal(int pos) {
      return switch (pos) {
        case 0 -> trackingWrapper;
        case 1 -> wrapped.contentType().id();
        case 2 -> wrapped.location();
        case 3 -> wrapped.fileFormat().metadataValue();
        case 4 -> wrapped.recordCount();
        case 5 -> wrapped.fileSizeInBytes();
        case 6 -> specId(wrapped);
        case 7 -> unionPartition(wrapped);
        case 8 -> wrapStats(wrapped.contentStats());
        case 9 -> wrapped.tightStats();
        case 10 -> wrapped.sortOrderId();
        case 11 -> wrapDV(wrapped.deletionVector());
        case 12 -> wrapped.keyMetadata();
        case 13 -> wrapped.splitOffsets();
        case 14 -> wrapColumnFiles(wrapped.columnFiles());
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
    }
  }

  static Schema rootManifestSchema(Types.StructType partitionType, Types.StructType statsType) {
    return new Schema(
        Types.NestedField.from(TrackedFile.TRACKING).ofType(ROOT_TRACKING_TYPE).build(),
        TrackedFile.CONTENT_TYPE,
        TrackedFile.LOCATION,
        TrackedFile.FILE_FORMAT,
        TrackedFile.RECORD_COUNT,
        TrackedFile.FILE_SIZE_IN_BYTES,
        TrackedFile.SPEC_ID,
        Types.NestedField.optional(
            TrackedFile.PARTITION_ID,
            TrackedFile.PARTITION_NAME,
            partitionType.fields().isEmpty() ? Types.UnknownType.get() : partitionType,
            TrackedFile.PARTITION_DOC),
        Types.NestedField.optional(
            TrackedFile.CONTENT_STATS_ID,
            TrackedFile.CONTENT_STATS_NAME,
            statsType.fields().isEmpty() ? Types.UnknownType.get() : statsType,
            TrackedFile.CONTENT_STATS_DOC),
        TrackedFile.TIGHT_STATS,
        TrackedFile.SORT_ORDER_ID,
        Types.NestedField.from(TrackedFile.DELETION_VECTOR).ofType(DELETION_VECTOR_TYPE).build(),
        Types.NestedField.from(TrackedFile.MANIFEST_INFO).ofType(MANIFEST_INFO_TYPE).build(),
        TrackedFile.KEY_METADATA,
        TrackedFile.SPLIT_OFFSETS,
        Types.NestedField.from(TrackedFile.COLUMN_FILES).ofType(COLUMN_FILES_LIST_TYPE).build());
  }

  /** Wrapper used to write a data file or leaf manifest to a v4 root manifest. */
  static class RootTrackedFileWrapper extends TrackedFileWrapper {
    private final RootTrackingWrapper trackingWrapper;
    private final ManifestInfoWrapper manifestInfoWrapper;
    private TrackedFile wrapped;

    RootTrackedFileWrapper(
        long commitSnapshotId,
        long commitSequenceNumber,
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType) {
      super(specsById, partitionType, contentStatsType);
      this.trackingWrapper = new RootTrackingWrapper(commitSnapshotId, commitSequenceNumber);
      this.manifestInfoWrapper = new ManifestInfoWrapper(commitSequenceNumber);
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
      validateTypeSpecificMetadata(file);
      validatePartitionSpecId(file);

      this.wrapped = file;
      trackingWrapper.wrap(tracking, assignedFirstRowId, file.location());

      return this;
    }

    @Override
    protected Object getInternal(int pos) {
      return switch (pos) {
        case 0 -> trackingWrapper;
        case 1 -> wrapped.contentType().id();
        case 2 -> wrapped.location();
        case 3 -> wrapped.fileFormat().metadataValue();
        case 4 -> wrapped.recordCount();
        case 5 -> wrapped.fileSizeInBytes();
        case 6 -> specId(wrapped);
        case 7 -> unionPartition(wrapped);
        case 8 -> wrapStats(wrapped.contentStats());
        case 9 -> wrapped.tightStats();
        case 10 -> wrapped.sortOrderId();
        case 11 -> wrapDV(wrapped.deletionVector());
        case 12 -> manifestInfoWrapper.wrap(wrapped.manifestInfo());
        case 13 -> wrapped.keyMetadata();
        case 14 -> wrapped.splitOffsets();
        case 15 -> wrapColumnFiles(wrapped.columnFiles());
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
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

  private static final Types.StructType ROOT_TRACKING_TYPE =
      Types.StructType.of(
          Tracking.STATUS,
          Tracking.SNAPSHOT_ID,
          Tracking.SEQUENCE_NUMBER,
          Tracking.FILE_SEQUENCE_NUMBER,
          Tracking.MODIFIED_SNAPSHOT_ID,
          Tracking.FIRST_ROW_ID,
          Tracking.DELETED_POSITIONS,
          Tracking.REPLACED_POSITIONS);

  private static final Types.StructType LEAF_TRACKING_TYPE =
      Types.StructType.of(
          Tracking.STATUS,
          Tracking.SNAPSHOT_ID,
          Tracking.SEQUENCE_NUMBER,
          Tracking.FILE_SEQUENCE_NUMBER,
          Tracking.MODIFIED_SNAPSHOT_ID,
          Tracking.FIRST_ROW_ID);

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

  private static final Types.StructType MANIFEST_INFO_TYPE =
      Types.StructType.of(
          ManifestInfo.ADDED_FILES_COUNT,
          ManifestInfo.EXISTING_FILES_COUNT,
          ManifestInfo.DELETED_FILES_COUNT,
          ManifestInfo.REPLACED_FILES_COUNT,
          ManifestInfo.MODIFIED_FILES_COUNT,
          ManifestInfo.ADDED_ROWS_COUNT,
          ManifestInfo.EXISTING_ROWS_COUNT,
          ManifestInfo.DELETED_ROWS_COUNT,
          ManifestInfo.REPLACED_ROWS_COUNT,
          ManifestInfo.MODIFIED_ROWS_COUNT,
          ManifestInfo.MIN_SEQUENCE_NUMBER,
          ManifestInfo.DV,
          ManifestInfo.FORMAT_VERSION);

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

  private static final Types.StructType DELETION_VECTOR_TYPE =
      Types.StructType.of(
          DeletionVector.LOCATION,
          DeletionVector.OFFSET,
          DeletionVector.SIZE_IN_BYTES,
          DeletionVector.CARDINALITY,
          DeletionVector.KEY_METADATA);

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

  private static final Types.StructType COLUMN_FILE_TYPE =
      Types.StructType.of(
          ColumnFile.LOCATION,
          ColumnFile.FIELD_IDS,
          ColumnFile.FILE_FORMAT,
          ColumnFile.FILE_SIZE_IN_BYTES,
          ColumnFile.KEY_METADATA);

  private static final Types.ListType COLUMN_FILES_LIST_TYPE =
      Types.ListType.ofRequired(
          TrackedFile.COLUMN_FILES.type().asListType().elementId(), COLUMN_FILE_TYPE);

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
        case 2 -> wrapped.fileFormat().metadataValue();
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
