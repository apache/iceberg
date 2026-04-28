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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LocationUtil;

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

  private static final Types.StructType ROOT_MANIFEST_WRITE_TYPE =
      entrySchema(Types.StructType.of()).asStruct();

  /**
   * Converts a {@link ManifestFile} to a {@link TrackedFileStruct} for writing into a root
   * manifest.
   *
   * <p>The returned struct uses the entry schema projection so that field positions match the write
   * schema (which excludes content_stats).
   */
  static TrackedFileStruct manifestFileToTrackedFile(
      ManifestFile manifest,
      long commitSnapshotId,
      long commitSequenceNumber,
      String tableLocation) {
    long seqNum = resolveSeqNum(manifest.sequenceNumber(), commitSequenceNumber);
    long minSeqNum = resolveSeqNum(manifest.minSequenceNumber(), commitSequenceNumber);

    TrackingStruct tracking = buildTracking(manifest, commitSnapshotId, seqNum);
    ManifestInfoStruct info = buildManifestInfo(manifest, minSeqNum);

    FileContent contentType =
        manifest.content() == ManifestContent.DATA
            ? FileContent.DATA_MANIFEST
            : FileContent.DELETE_MANIFEST;

    int totalEntries =
        intOrZero(manifest.addedFilesCount())
            + intOrZero(manifest.existingFilesCount())
            + intOrZero(manifest.deletedFilesCount());

    // use the entry schema as projection so positions match the write schema
    TrackedFileStruct tf = new TrackedFileStruct(ROOT_MANIFEST_WRITE_TYPE);
    tf.set(0, tracking);
    tf.set(1, contentType.id());
    tf.set(2, LocationUtil.relativize(manifest.path(), tableLocation));
    tf.set(3, FileFormat.PARQUET.toString());
    tf.set(4, (long) totalEntries);
    tf.set(5, manifest.length());
    tf.set(6, manifest.partitionSpecId());
    tf.set(9, info);

    if (manifest.keyMetadata() != null) {
      tf.set(10, manifest.keyMetadata());
    }

    return tf;
  }

  /** Converts a {@link TrackedFile} read from a root manifest back to a {@link ManifestFile}. */
  static ManifestFile trackedFileToManifestFile(TrackedFile tf, String tableLocation) {
    ManifestInfo info = tf.manifestInfo();
    Tracking tracking = tf.tracking();
    ManifestContent content =
        tf.contentType() == FileContent.DATA_MANIFEST
            ? ManifestContent.DATA
            : ManifestContent.DELETES;

    return new GenericManifestFile(
        LocationUtil.resolve(tf.location(), tableLocation),
        tf.fileSizeInBytes(),
        tf.specId() != null ? tf.specId() : 0,
        content,
        sequenceNumberFrom(tracking),
        info != null ? info.minSequenceNumber() : 0L,
        tracking != null ? tracking.snapshotId() : null,
        null,
        tf.keyMetadata(),
        info != null ? info.addedFilesCount() : 0,
        info != null ? info.addedRowsCount() : 0L,
        info != null ? info.existingFilesCount() : 0,
        info != null ? info.existingRowsCount() : 0L,
        info != null ? info.deletedFilesCount() : 0,
        info != null ? info.deletedRowsCount() : 0L,
        null);
  }

  private static long resolveSeqNum(long seqNum, long commitSequenceNumber) {
    return seqNum == ManifestWriter.UNASSIGNED_SEQ ? commitSequenceNumber : seqNum;
  }

  private static TrackingStruct buildTracking(
      ManifestFile manifest, long commitSnapshotId, long seqNum) {
    TrackingStruct tracking = new TrackingStruct();
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, manifest.snapshotId() != null ? manifest.snapshotId() : commitSnapshotId);
    tracking.set(2, seqNum);
    tracking.set(3, seqNum);
    return tracking;
  }

  private static ManifestInfoStruct buildManifestInfo(ManifestFile manifest, long minSeqNum) {
    ManifestInfoStruct info = new ManifestInfoStruct();
    info.set(0, intOrZero(manifest.addedFilesCount()));
    info.set(1, intOrZero(manifest.existingFilesCount()));
    info.set(2, intOrZero(manifest.deletedFilesCount()));
    info.set(3, 0);
    info.set(4, longOrZero(manifest.addedRowsCount()));
    info.set(5, longOrZero(manifest.existingRowsCount()));
    info.set(6, longOrZero(manifest.deletedRowsCount()));
    info.set(7, 0L);
    info.set(8, minSeqNum);
    return info;
  }

  private static int intOrZero(Integer value) {
    return value != null ? value : 0;
  }

  private static long longOrZero(Long value) {
    return value != null ? value : 0L;
  }

  private static long sequenceNumberFrom(Tracking tracking) {
    if (tracking != null && tracking.dataSequenceNumber() != null) {
      return tracking.dataSequenceNumber();
    }

    return 0L;
  }

  static Schema entrySchema(Types.StructType partitionType) {
    return new Schema(
        TrackedFile.TRACKING,
        TrackedFile.CONTENT_TYPE,
        TrackedFile.LOCATION,
        TrackedFile.FILE_FORMAT,
        TrackedFile.RECORD_COUNT,
        TrackedFile.FILE_SIZE_IN_BYTES,
        TrackedFile.SPEC_ID,
        TrackedFile.SORT_ORDER_ID,
        TrackedFile.DELETION_VECTOR,
        TrackedFile.MANIFEST_INFO,
        TrackedFile.KEY_METADATA,
        TrackedFile.SPLIT_OFFSETS,
        TrackedFile.EQUALITY_IDS);
  }

  static class ManifestEntryWrapper<F extends ContentFile<F>>
      implements ManifestEntry<F>, StructLike {
    private static final int ENTRY_FIELD_COUNT = 13;

    private final TrackingWriteWrapper trackingWrapper;
    private final String tableLocation;
    private ManifestEntry<F> wrapped = null;

    ManifestEntryWrapper(
        Long commitSnapshotId, Types.StructType partitionType, String tableLocation) {
      this.trackingWrapper = new TrackingWriteWrapper(commitSnapshotId);
      this.tableLocation = tableLocation;
    }

    public ManifestEntryWrapper<F> wrap(ManifestEntry<F> entry) {
      this.wrapped = entry;
      return this;
    }

    @Override
    public int size() {
      return ENTRY_FIELD_COUNT;
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
          return trackingWrapper.wrap(wrapped);
        case 1:
          return wrapped.file().content().id();
        case 2:
          return LocationUtil.relativize(wrapped.file().location(), tableLocation);
        case 3:
          return wrapped.file().format() != null ? wrapped.file().format().toString() : null;
        case 4:
          return wrapped.file().recordCount();
        case 5:
          return wrapped.file().fileSizeInBytes();
        case 6:
          return wrapped.file().specId();
        case 7:
          return wrapped.file().sortOrderId();
        case 8:
          return null; // deletion_vector (future)
        case 9:
          return null; // manifest_info (null for data files)
        case 10:
          return wrapped.file().keyMetadata();
        case 11:
          return wrapped.file().splitOffsets();
        case 12:
          return wrapped.file().equalityFieldIds();
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

  /** Wrapper that writes tracking fields from a ManifestEntry as a StructLike. */
  static class TrackingWriteWrapper implements StructLike {
    private static final int TRACKING_FIELD_COUNT = 8;

    private final Long commitSnapshotId;
    private ManifestEntry<?> entry = null;

    TrackingWriteWrapper(Long commitSnapshotId) {
      this.commitSnapshotId = commitSnapshotId;
    }

    TrackingWriteWrapper wrap(ManifestEntry<?> newEntry) {
      this.entry = newEntry;
      return this;
    }

    @Override
    public int size() {
      return TRACKING_FIELD_COUNT;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify TrackingWriteWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      switch (pos) {
        case 0:
          return entry.status().id();
        case 1:
          return entry.snapshotId();
        case 2:
          if (entry.dataSequenceNumber() == null) {
            Preconditions.checkState(
                entry.snapshotId() == null || entry.snapshotId().equals(commitSnapshotId),
                "Found unassigned sequence number for an entry from snapshot: %s",
                entry.snapshotId());
            Preconditions.checkState(
                entry.status() == ManifestEntry.Status.ADDED,
                "Only entries with status ADDED can have null sequence number");
            return null;
          }

          return entry.dataSequenceNumber();
        case 3:
          return entry.fileSequenceNumber();
        case 4:
          return null; // dv_snapshot_id (future)
        case 5:
          if (entry.file().content() == FileContent.DATA) {
            return entry.file().firstRowId();
          } else {
            return null;
          }
        case 6:
          return null; // deleted_positions (future)
        case 7:
          return null; // replaced_positions (future)
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }
  }
}
