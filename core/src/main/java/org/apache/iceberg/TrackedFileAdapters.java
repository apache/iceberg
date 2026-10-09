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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Adapts between the {@link TrackedFile} model and the {@link DataFile} / {@link DeleteFile} /
 * {@link ManifestFile} APIs in both directions.
 */
class TrackedFileAdapters {

  private TrackedFileAdapters() {}

  static DataFile asDataFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA,
        "Invalid content type for DataFile: %s",
        file.contentType());
    return new TrackedDataFile(file, resolveSpecId(file, specsById));
  }

  static DeleteFile asDVDeleteFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA,
        "Invalid content type for DV delete file: %s",
        file.contentType());
    return new TrackedDVDeleteFile(file, resolveSpecId(file, specsById));
  }

  static DeleteFile asEqualityDeleteFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.EQUALITY_DELETES,
        "Invalid content type for equality delete file: %s",
        file.contentType());
    return new TrackedEqualityDeleteFile(file, resolveSpecId(file, specsById));
  }

  static ManifestFile asManifestFile(TrackedFile file) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA_MANIFEST
            || file.contentType() == FileContent.DELETE_MANIFEST,
        "Invalid content type for ManifestFile: %s",
        file.contentType());
    return new TrackedManifestFile(file);
  }

  /** Returns a reusable adapter from {@link DataFile} to {@link TrackedFile}. */
  static DataTrackedFile forDataFile(Schema tableSchema) {
    return new DataTrackedFile(tableSchema);
  }

  /** Returns a reusable adapter from {@link ManifestFile} to {@link TrackedFile}. */
  static ManifestTrackedFile forManifestFile() {
    return new ManifestTrackedFile();
  }

  /** Shared base for data and delete file adapters. */
  private abstract static class TrackedFileAdapter<F extends ContentFile<F>>
      implements ContentFile<F>, Serializable {
    private final TrackedFile file;
    private final int specId;

    private TrackedFileAdapter(TrackedFile file, int specId) {
      this.file = file;
      this.specId = specId;
    }

    protected TrackedFile file() {
      return file;
    }

    protected Tracking tracking() {
      return file.tracking();
    }

    @Override
    public Long pos() {
      Tracking tracking = tracking();
      return tracking != null ? tracking.manifestPos() : null;
    }

    @Override
    public String manifestLocation() {
      Tracking tracking = tracking();
      return tracking != null ? tracking.manifestLocation() : null;
    }

    @Override
    public int specId() {
      return specId;
    }

    @Override
    public StructLike partition() {
      return file().partition() != null ? file().partition() : PartitionData.EMPTY;
    }

    @Override
    public Long dataSequenceNumber() {
      Tracking tracking = tracking();
      return tracking != null ? tracking.dataSequenceNumber() : null;
    }

    @Override
    public Long fileSequenceNumber() {
      Tracking tracking = tracking();
      return tracking != null ? tracking.fileSequenceNumber() : null;
    }
  }

  /**
   * Shared base for adapters where the {@link ContentFile} is the {@link TrackedFile} itself, as
   * opposed to {@link TrackedDVDeleteFile} which represents the tracked file's deletion vector.
   */
  private abstract static class TrackedContentFile<F extends ContentFile<F>>
      extends TrackedFileAdapter<F> {
    private TrackedContentFile(TrackedFile file, int specId) {
      super(file, specId);
    }

    @SuppressWarnings("deprecation")
    @Override
    public CharSequence path() {
      return file().location();
    }

    @Override
    public String location() {
      return file().location();
    }

    @Override
    public FileFormat format() {
      return file().fileFormat();
    }

    @Override
    public long recordCount() {
      return file().recordCount();
    }

    @Override
    public long fileSizeInBytes() {
      return file().fileSizeInBytes();
    }

    @Override
    public Integer sortOrderId() {
      return file().sortOrderId();
    }

    @Override
    public ByteBuffer keyMetadata() {
      return file().keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
      return file().splitOffsets();
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return null;
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return ContentStatsBackedMap.valueCounts(file().contentStats());
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return ContentStatsBackedMap.nullValueCounts(file().contentStats());
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return ContentStatsBackedMap.nanValueCounts(file().contentStats());
    }

    @Override
    public Map<Integer, Long> totalBytes() {
      return ContentStatsBackedMap.totalBytes(file().contentStats());
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return ContentStatsBackedMap.lowerBounds(file().contentStats());
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return ContentStatsBackedMap.upperBounds(file().contentStats());
    }
  }

  /** Adapts a TrackedFile DATA entry to the {@link DataFile} interface. */
  private static class TrackedDataFile extends TrackedContentFile<DataFile> implements DataFile {
    private TrackedDataFile(TrackedFile file, int specId) {
      super(file, specId);
    }

    @Override
    public FileContent content() {
      return FileContent.DATA;
    }

    @Override
    public Long firstRowId() {
      return tracking() != null ? tracking().firstRowId() : null;
    }

    @Override
    public DeletionVector deletionVector() {
      return file().deletionVector();
    }

    @Override
    public DataFile copy() {
      return new TrackedDataFile(file().copy(), specId());
    }

    @Override
    public DataFile copy(boolean withStats) {
      return withStats ? copy() : copyWithoutStats();
    }

    @Override
    public DataFile copyWithoutStats() {
      return new TrackedDataFile(file().copyWithoutStats(), specId());
    }

    @Override
    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedDataFile(file().copyWithStats(requestedColumnIds), specId());
    }
  }

  /** Adapts a TrackedFile EQUALITY_DELETES entry to the {@link DeleteFile} interface. */
  private static class TrackedEqualityDeleteFile extends TrackedContentFile<DeleteFile>
      implements DeleteFile {
    private TrackedEqualityDeleteFile(TrackedFile file, int specId) {
      super(file, specId);
    }

    @Override
    public FileContent content() {
      return FileContent.EQUALITY_DELETES;
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return file().equalityIds();
    }

    @Override
    public DeleteFile copy() {
      return new TrackedEqualityDeleteFile(file().copy(), specId());
    }

    @Override
    public DeleteFile copy(boolean withStats) {
      return withStats ? copy() : copyWithoutStats();
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return new TrackedEqualityDeleteFile(file().copyWithoutStats(), specId());
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedEqualityDeleteFile(file().copyWithStats(requestedColumnIds), specId());
    }
  }

  /**
   * Adapts the deletion vector from a TrackedFile DATA entry to the {@link DeleteFile} interface.
   */
  private static class TrackedDVDeleteFile extends TrackedFileAdapter<DeleteFile>
      implements DeleteFile {
    private final DeletionVector dv;

    private TrackedDVDeleteFile(TrackedFile file, int specId) {
      super(file, specId);
      Preconditions.checkArgument(
          file.deletionVector() != null, "Cannot create DV delete file: no deletion vector");
      this.dv = file.deletionVector();
    }

    @Override
    public FileContent content() {
      return FileContent.POSITION_DELETES;
    }

    @SuppressWarnings("deprecation")
    @Override
    public CharSequence path() {
      return dv.location();
    }

    @Override
    public String location() {
      return dv.location();
    }

    @Override
    public FileFormat format() {
      return FileFormat.PUFFIN;
    }

    @Override
    public long recordCount() {
      return dv.cardinality();
    }

    // Returns the DV blob size, not the full Puffin file size. The DeletionVector metadata does not
    // include the Puffin file size, so this is the best approximation available. Space accounting
    // that sums fileSizeInBytes() was already imprecise in v3 (multiple DVs sharing a Puffin file
    // each reported the full file size).
    @Override
    public long fileSizeInBytes() {
      return dv.sizeInBytes();
    }

    // From the spec: position deletes are required to be sorted by file and position, not a table
    // order, and should set sort order id to null
    @Override
    public Integer sortOrderId() {
      return null;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return dv.keyMetadata();
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return null;
    }

    @Override
    public String referencedDataFile() {
      return file().location();
    }

    @Override
    public Long contentOffset() {
      return dv.offset();
    }

    @Override
    public Long contentSizeInBytes() {
      return dv.sizeInBytes();
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
    public DeleteFile copy() {
      return new TrackedDVDeleteFile(file().copyWithoutStats(), specId());
    }

    @Override
    public DeleteFile copy(boolean withStats) {
      return copy();
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return copy();
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
      return copy();
    }
  }

  /** Adapts a TrackedFile to {@link ManifestFile}. */
  private static class TrackedManifestFile implements ManifestFile {
    private final TrackedFile file;

    private TrackedManifestFile(TrackedFile file) {
      this.file = file;
    }

    private TrackedFile file() {
      return file;
    }

    @Override
    public String path() {
      return file.location();
    }

    @Override
    public long length() {
      return file.fileSizeInBytes();
    }

    @Override
    public int partitionSpecId() {
      throw new UnsupportedOperationException(
          "v4 manifests are not bound to a single partition spec");
    }

    @Override
    public ManifestContent content() {
      switch (file.contentType()) {
        case DATA_MANIFEST:
          return ManifestContent.DATA;
        case DELETE_MANIFEST:
          return ManifestContent.DELETES;
        default:
          throw new UnsupportedOperationException(
              "Unsupported content type for manifests: " + file.contentType());
      }
    }

    @Override
    public long sequenceNumber() {
      return file.tracking().dataSequenceNumber();
    }

    @Override
    public long minSequenceNumber() {
      return file.manifestInfo().minSequenceNumber();
    }

    @Override
    public Long snapshotId() {
      return file.tracking().snapshotId();
    }

    @Override
    public Integer addedFilesCount() {
      return file.manifestInfo().addedFilesCount();
    }

    @Override
    public Long addedRowsCount() {
      return file.manifestInfo().addedRowsCount();
    }

    @Override
    public Integer existingFilesCount() {
      return file.manifestInfo().existingFilesCount();
    }

    @Override
    public Long existingRowsCount() {
      return file.manifestInfo().existingRowsCount();
    }

    @Override
    public Integer deletedFilesCount() {
      return file.manifestInfo().deletedFilesCount();
    }

    @Override
    public Long deletedRowsCount() {
      return file.manifestInfo().deletedRowsCount();
    }

    @Override
    public Integer replacedFilesCount() {
      return file.manifestInfo().replacedFilesCount();
    }

    @Override
    public Long replacedRowsCount() {
      return file.manifestInfo().replacedRowsCount();
    }

    @Override
    public Integer modifiedFilesCount() {
      return file.manifestInfo().modifiedFilesCount();
    }

    @Override
    public Long modifiedRowsCount() {
      return file.manifestInfo().modifiedRowsCount();
    }

    @Override
    public List<PartitionFieldSummary> partitions() {
      return null;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return file.keyMetadata();
    }

    @Override
    public Long firstRowId() {
      return file.tracking().firstRowId();
    }

    @Override
    public ManifestBitmap manifestDeletionVector() {
      return file.manifestInfo().manifestDeletionVector();
    }

    @Override
    public int formatVersion() {
      return file.manifestInfo().formatVersion();
    }

    @Override
    public ManifestFile copy() {
      return new TrackedManifestFile(file.copy());
    }
  }

  /** Adapts a {@link DataFile} to {@link TrackedFile}. */
  static class DataTrackedFile implements TrackedFile {
    private final MapBackedContentStats statsWrapper;
    private final WrappedEntryTracking trackingWrapper = new WrappedEntryTracking();

    private Tracking tracking;
    private DataFile file;
    private ContentStats stats;

    DataTrackedFile(Schema tableSchema) {
      this.statsWrapper = new MapBackedContentStats(tableSchema);
    }

    /**
     * Re-points this adapter at a {@link DataFile} from the public API. Tracking is unset.
     *
     * <p>Returns the inner {@link TrackedFile} when {@code newFile} is a {@link TrackedDataFile}.
     */
    public TrackedFile wrap(DataFile newFile) {
      if (newFile instanceof TrackedDataFile tracked) {
        return tracked.file();
      }

      wrapInternal(newFile);
      this.tracking = null;
      return this;
    }

    /**
     * Re-points this adapter at a {@link ManifestEntry}. Converts the contained data file and the
     * entry's tracking fields.
     *
     * <p>Returns the inner {@link TrackedFile} when the entry's file is a {@link TrackedDataFile}.
     */
    public TrackedFile wrap(ManifestEntry<DataFile> entry) {
      Preconditions.checkArgument(entry != null, "Invalid entry: null");
      if (entry.file() instanceof TrackedDataFile tracked) {
        return tracked.file();
      }

      wrapInternal(entry.file());
      this.tracking = trackingWrapper.wrap(entry);
      return this;
    }

    private void wrapInternal(DataFile newFile) {
      Preconditions.checkArgument(newFile != null, "Invalid file: null");
      Preconditions.checkArgument(
          newFile.content() == FileContent.DATA,
          "Invalid content for data file: %s",
          newFile.content());

      this.file = newFile;
      this.stats = hasContentStats(newFile) ? statsWrapper.wrap(newFile) : null;
    }

    @Override
    public Tracking tracking() {
      return tracking;
    }

    @Override
    public FileContent contentType() {
      return FileContent.DATA;
    }

    @Override
    public String location() {
      return file.location();
    }

    @Override
    public FileFormat fileFormat() {
      return file.format();
    }

    @Override
    public long recordCount() {
      return file.recordCount();
    }

    @Override
    public long fileSizeInBytes() {
      return file.fileSizeInBytes();
    }

    @Override
    public Integer specId() {
      // Files in one manifest may use different specs; this is the spec for this data file only.
      return file.specId();
    }

    @Override
    public StructLike partition() {
      return file.partition();
    }

    @Override
    public ContentStats contentStats() {
      return stats;
    }

    @Override
    public Integer sortOrderId() {
      return file.sortOrderId();
    }

    @Override
    public DeletionVector deletionVector() {
      return file.deletionVector();
    }

    @Override
    public ManifestInfo manifestInfo() {
      return null;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return file.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
      return file.splitOffsets();
    }

    @Override
    public List<Integer> equalityIds() {
      return null;
    }

    @Override
    public TrackedFile copy() {
      throw new UnsupportedOperationException("copy is not implemented");
    }

    @Override
    public TrackedFile copyWithStats(Set<Integer> requestedColumnIds) {
      throw new UnsupportedOperationException("copy is not implemented");
    }
  }

  /** Adapts a {@link ManifestFile} to {@link TrackedFile}. */
  static class ManifestTrackedFile implements TrackedFile {
    private final WrappedManifestInfo manifestInfo = new WrappedManifestInfo();
    private final WrappedManifestTracking trackingWrapper = new WrappedManifestTracking();
    private Tracking tracking;
    private ManifestFile manifest;
    private long recordCount;
    private FileContent contentType;

    ManifestTrackedFile() {}

    /**
     * Re-points this adapter at {@code newManifest}. Converts the manifest's own fields only;
     * write-time tracking updates are applied by the versioned writer.
     *
     * <p>Returns the inner {@link TrackedFile} when {@code newManifest} is a {@link
     * TrackedManifestFile}.
     */
    public TrackedFile wrap(ManifestFile newManifest) {
      if (newManifest instanceof TrackedManifestFile tracked) {
        return tracked.file();
      }

      Preconditions.checkArgument(newManifest != null, "Invalid manifest file: null");

      this.manifest = newManifest;
      this.contentType =
          newManifest.content() == ManifestContent.DATA
              ? FileContent.DATA_MANIFEST
              : FileContent.DELETE_MANIFEST;
      this.recordCount = manifestRecordCount(newManifest);
      this.tracking = trackingWrapper.wrap(newManifest);
      this.manifestInfo.wrap(newManifest);
      return this;
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
      return manifest.path();
    }

    @Override
    public FileFormat fileFormat() {
      // Manifest files before v4 are always Avro.
      return FileFormat.AVRO;
    }

    @Override
    public long recordCount() {
      // Number of TrackedFile rows stored in the manifest.
      return recordCount;
    }

    @Override
    public long fileSizeInBytes() {
      return manifest.length();
    }

    @Override
    public Integer specId() {
      // Spec the wrapped manifest was written with. Data file entries in a v4 manifest file may
      // use different specs.
      return manifest.partitionSpecId();
    }

    @Override
    public StructLike partition() {
      return null;
    }

    @Override
    public ContentStats contentStats() {
      return null;
    }

    @Override
    public Integer sortOrderId() {
      // Manifests have no table sort order.
      return null;
    }

    @Override
    public DeletionVector deletionVector() {
      return null;
    }

    @Override
    public ManifestInfo manifestInfo() {
      return manifestInfo;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return manifest.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
      return null;
    }

    @Override
    public List<Integer> equalityIds() {
      return null;
    }

    @Override
    public TrackedFile copy() {
      throw new UnsupportedOperationException("copy is not implemented");
    }

    @Override
    public TrackedFile copyWithStats(Set<Integer> requestedColumnIds) {
      throw new UnsupportedOperationException("copy is not implemented");
    }
  }

  /** Reusable {@link ManifestInfo} view over a {@link ManifestFile}'s counts. */
  private static class WrappedManifestInfo implements ManifestInfo {
    private ManifestFile manifest = null;

    WrappedManifestInfo wrap(ManifestFile newManifest) {
      this.manifest = newManifest;
      return this;
    }

    @Override
    public int addedFilesCount() {
      return manifest.addedFilesCount();
    }

    @Override
    public int existingFilesCount() {
      return manifest.existingFilesCount();
    }

    @Override
    public int deletedFilesCount() {
      return manifest.deletedFilesCount();
    }

    @Override
    public int replacedFilesCount() {
      return manifest.replacedFilesCount();
    }

    @Override
    public int modifiedFilesCount() {
      return manifest.modifiedFilesCount();
    }

    @Override
    public long addedRowsCount() {
      return manifest.addedRowsCount();
    }

    @Override
    public long existingRowsCount() {
      return manifest.existingRowsCount();
    }

    @Override
    public long deletedRowsCount() {
      return manifest.deletedRowsCount();
    }

    @Override
    public long replacedRowsCount() {
      return manifest.replacedRowsCount();
    }

    @Override
    public long modifiedRowsCount() {
      return manifest.modifiedRowsCount();
    }

    @Override
    public long minSequenceNumber() {
      return manifest.minSequenceNumber();
    }

    @Override
    public ManifestBitmap manifestDeletionVector() {
      return manifest.manifestDeletionVector();
    }

    @Override
    public int formatVersion() {
      return manifest.formatVersion();
    }

    @Override
    public ManifestInfo copy() {
      throw new UnsupportedOperationException("copy is not implemented");
    }
  }

  private static class WrappedEntryTracking implements Tracking {
    private ManifestEntry<DataFile> entry = null;

    WrappedEntryTracking wrap(ManifestEntry<DataFile> newEntry) {
      this.entry = newEntry;
      return this;
    }

    @Override
    public EntryStatus status() {
      return entryStatus(entry.status());
    }

    @Override
    public Long snapshotId() {
      return entry.snapshotId();
    }

    @Override
    public Long dataSequenceNumber() {
      return entry.dataSequenceNumber();
    }

    @Override
    public Long fileSequenceNumber() {
      return entry.fileSequenceNumber();
    }

    @Override
    public Long modifiedSnapshotId() {
      return null;
    }

    @Override
    public Long firstRowId() {
      return entry.file().firstRowId();
    }

    @Override
    public ByteBuffer deletedPositions() {
      return null;
    }

    @Override
    public ByteBuffer replacedPositions() {
      return null;
    }

    @Override
    public String manifestLocation() {
      return entry.file().manifestLocation();
    }

    @Override
    public long manifestPos() {
      Long pos = entry.file().pos();
      return pos != null ? pos : -1L;
    }

    @Override
    public Tracking copy() {
      throw new UnsupportedOperationException("copy is not implemented");
    }
  }

  private static class WrappedManifestTracking implements Tracking {
    private ManifestFile manifest = null;

    WrappedManifestTracking wrap(ManifestFile newManifest) {
      this.manifest = newManifest;
      return this;
    }

    @Override
    public EntryStatus status() {
      // Pre-v4 manifests have no status and are live; the writer sets EXISTING or MODIFIED.
      return EntryStatus.EXISTING;
    }

    @Override
    public Long snapshotId() {
      return manifest.snapshotId();
    }

    @Override
    public Long dataSequenceNumber() {
      return manifest.sequenceNumber();
    }

    @Override
    public Long fileSequenceNumber() {
      return manifest.sequenceNumber();
    }

    @Override
    public Long modifiedSnapshotId() {
      return null;
    }

    @Override
    public Long firstRowId() {
      return manifest.firstRowId();
    }

    @Override
    public ByteBuffer deletedPositions() {
      return null;
    }

    @Override
    public ByteBuffer replacedPositions() {
      return null;
    }

    @Override
    public String manifestLocation() {
      return null;
    }

    @Override
    public long manifestPos() {
      return -1L;
    }

    @Override
    public Tracking copy() {
      throw new UnsupportedOperationException("copy is not implemented");
    }
  }

  private static EntryStatus entryStatus(ManifestEntry.Status status) {
    return switch (status) {
      case EXISTING -> EntryStatus.EXISTING;
      case ADDED -> EntryStatus.ADDED;
      case DELETED -> EntryStatus.DELETED;
    };
  }

  private static boolean hasContentStats(ContentFile<?> file) {
    return isPresent(file.valueCounts())
        || isPresent(file.nullValueCounts())
        || isPresent(file.nanValueCounts())
        || isPresent(file.avgValueSizes())
        || isPresent(file.lowerBounds())
        || isPresent(file.upperBounds());
  }

  private static boolean isPresent(Map<?, ?> map) {
    return map != null && !map.isEmpty();
  }

  /**
   * Record count of a manifest is the number of TrackedFile rows it stores: the sum of per-status
   * file counts. Missing counts fail rather than producing an incorrect total.
   */
  private static long manifestRecordCount(ManifestFile manifest) {
    Preconditions.checkNotNull(
        manifest.addedFilesCount(),
        "Cannot convert manifest %s: missing added files count",
        manifest.path());
    Preconditions.checkNotNull(
        manifest.existingFilesCount(),
        "Cannot convert manifest %s: missing existing files count",
        manifest.path());
    Preconditions.checkNotNull(
        manifest.deletedFilesCount(),
        "Cannot convert manifest %s: missing deleted files count",
        manifest.path());
    Preconditions.checkArgument(
        Objects.equals(manifest.replacedFilesCount(), 0),
        "Cannot convert manifest %s: Invalid replaced file count: %s",
        manifest.path(),
        manifest.replacedFilesCount());
    Preconditions.checkArgument(
        Objects.equals(manifest.modifiedFilesCount(), 0),
        "Cannot convert manifest %s: Invalid modified file count: %s",
        manifest.path(),
        manifest.modifiedFilesCount());
    return (long) manifest.addedFilesCount()
        + manifest.existingFilesCount()
        + manifest.deletedFilesCount()
        + manifest.replacedFilesCount()
        + manifest.modifiedFilesCount();
  }

  private static int resolveSpecId(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Integer specId = file.specId();
    if (specId != null) {
      Preconditions.checkArgument(
          specsById.containsKey(specId), "Cannot find partition spec for spec ID: %s", specId);
      return specId;
    }

    // A null spec ID means the file is unpartitioned; use the table's unpartitioned spec.
    for (PartitionSpec spec : specsById.values()) {
      if (spec.isUnpartitioned()) {
        return spec.specId();
      }
    }

    throw new IllegalArgumentException(
        "Cannot find unpartitioned spec in specs: " + specsById.keySet());
  }
}
