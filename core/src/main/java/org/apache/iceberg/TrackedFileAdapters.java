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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

/**
 * Adapts between the {@link TrackedFile} row and the {@link DataFile} / {@link DeleteFile} / {@link
 * ManifestFile} APIs in both directions.
 */
class TrackedFileAdapters {

  private static final int TRACKED_FILE_FIELD_COUNT =
      TrackedFile.schema(Types.StructType.of(), Types.StructType.of()).asStruct().fields().size();

  private static final int MANIFEST_INFO_FIELD_COUNT = ManifestInfo.schema().fields().size();

  private TrackedFileAdapters() {}

  static DataFile asDataFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA,
        "Invalid content type for DataFile: %s",
        file.contentType());
    return new TrackedDataFile(file, resolveSpec(file, specsById));
  }

  static DeleteFile asDVDeleteFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA,
        "Invalid content type for DV delete file: %s",
        file.contentType());
    return new TrackedDVDeleteFile(file, resolveSpec(file, specsById));
  }

  static DeleteFile asEqualityDeleteFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.EQUALITY_DELETES,
        "Invalid content type for equality delete file: %s",
        file.contentType());
    return new TrackedEqualityDeleteFile(file, resolveSpec(file, specsById));
  }

  static ManifestFile asManifestFile(TrackedFile file) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA_MANIFEST
            || file.contentType() == FileContent.DELETE_MANIFEST,
        "Invalid content type for ManifestFile: %s",
        file.contentType());
    return new TrackedManifestFile(file);
  }

  /**
   * Returns a reusable wrapper that presents a {@link DataFile} as a {@link TrackedFile} row.
   *
   * @param writeSchema the TrackedFile writer schema, including partition and content-stats types
   */
  static DataTrackedFile forDataFile(Schema writeSchema) {
    return new DataTrackedFile(writeSchema);
  }

  /**
   * Returns a reusable wrapper that presents an equality {@link DeleteFile} as a {@link
   * TrackedFile} row.
   *
   * @param writeSchema the TrackedFile writer schema, including partition and content-stats types
   */
  static EqualityDeleteTrackedFile forEqualityDeleteFile(Schema writeSchema) {
    return new EqualityDeleteTrackedFile(writeSchema);
  }

  /**
   * Returns a reusable wrapper that presents a {@link ManifestFile} as a {@link TrackedFile} leaf
   * manifest row.
   */
  static ManifestTrackedFile forManifestReference() {
    return new ManifestTrackedFile();
  }

  /** Shared base for data and delete file adapters. */
  private abstract static class TrackedFileAdapter<F extends ContentFile<F>>
      implements ContentFile<F> {
    private final TrackedFile file;
    private final PartitionSpec spec;

    private TrackedFileAdapter(TrackedFile file, PartitionSpec spec) {
      Preconditions.checkArgument(
          file.specId() == null ? spec.isUnpartitioned() : file.specId() == spec.specId(),
          "File spec ID %s does not match partition spec %s",
          file.specId(),
          spec.specId());
      this.file = file;
      this.spec = spec;
    }

    protected TrackedFile file() {
      return file;
    }

    protected PartitionSpec spec() {
      return spec;
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
      return spec.specId();
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
    private TrackedContentFile(TrackedFile file, PartitionSpec spec) {
      super(file, spec);
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
    public Map<Integer, Integer> avgValueSizes() {
      return ContentStatsBackedMap.avgValueSizes(file().contentStats());
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
    private TrackedDataFile(TrackedFile file, PartitionSpec spec) {
      super(file, spec);
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
      return new TrackedDataFile(file().copy(), spec());
    }

    @Override
    public DataFile copy(boolean withStats) {
      return withStats ? copy() : copyWithoutStats();
    }

    @Override
    public DataFile copyWithoutStats() {
      return new TrackedDataFile(file().copyWithoutStats(), spec());
    }

    @Override
    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedDataFile(file().copyWithStats(requestedColumnIds), spec());
    }
  }

  /** Adapts a TrackedFile EQUALITY_DELETES entry to the {@link DeleteFile} interface. */
  private static class TrackedEqualityDeleteFile extends TrackedContentFile<DeleteFile>
      implements DeleteFile {
    private TrackedEqualityDeleteFile(TrackedFile file, PartitionSpec spec) {
      super(file, spec);
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
      return new TrackedEqualityDeleteFile(file().copy(), spec());
    }

    @Override
    public DeleteFile copy(boolean withStats) {
      return withStats ? copy() : copyWithoutStats();
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return new TrackedEqualityDeleteFile(file().copyWithoutStats(), spec());
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedEqualityDeleteFile(file().copyWithStats(requestedColumnIds), spec());
    }
  }

  /**
   * Adapts the deletion vector from a TrackedFile DATA entry to the {@link DeleteFile} interface.
   */
  private static class TrackedDVDeleteFile extends TrackedFileAdapter<DeleteFile>
      implements DeleteFile {
    private final DeletionVector dv;

    private TrackedDVDeleteFile(TrackedFile file, PartitionSpec spec) {
      super(file, spec);
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
      return new TrackedDVDeleteFile(file().copyWithoutStats(), spec());
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
      ByteBuffer dv = file.manifestInfo().dv();
      if (dv == null) {
        return null;
      }

      return new ManifestBitmap() {
        @Override
        public int cardinality() {
          throw new UnsupportedOperationException("Bitmap decoding has not been implemented");
        }

        @Override
        public boolean isSet(int position) {
          throw new UnsupportedOperationException("Bitmap decoding has not been implemented");
        }

        @Override
        public ByteBuffer buffer() {
          return dv;
        }
      };
    }

    @Override
    public ManifestFile copy() {
      return new TrackedManifestFile(file.copy());
    }
  }

  /** Shared base for content-file (DATA / EQUALITY_DELETES) write-direction wrappers. */
  abstract static class ContentTrackedFile<F extends ContentFile<F>>
      implements TrackedFile, StructLike {
    private final Types.StructType partitionType;
    private final MapBackedContentStats statsWrapper;

    private Tracking tracking;
    private F file;
    private StructProjection partition;
    private ContentStats stats;

    ContentTrackedFile(Schema writeSchema) {
      Preconditions.checkArgument(writeSchema != null, "Invalid write schema: null");
      Types.StructType statsType = nestedStruct(writeSchema, TrackedFile.CONTENT_STATS_ID);
      this.partitionType = nestedStruct(writeSchema, TrackedFile.PARTITION_ID);
      this.statsWrapper = statsType != null ? new MapBackedContentStats(statsType) : null;
    }

    TrackedFile wrapWithTracking(F newFile, Tracking newTracking) {
      Preconditions.checkArgument(newFile != null, "Invalid file: null");
      Preconditions.checkArgument(newTracking != null, "Invalid tracking: null");
      validateContent(newFile);

      if (newFile instanceof TrackedContentFile) {
        return ((TrackedContentFile<?>) newFile).file();
      }

      this.file = newFile;
      this.partition = partitionType != null ? projectPartition(newFile, partitionType) : null;
      this.stats = statsWrapper != null ? statsWrapper.wrap(newFile) : null;
      this.tracking = newTracking;
      return this;
    }

    /** Content-type-specific validation of the wrapped file. */
    abstract void validateContent(F newFile);

    protected F file() {
      return file;
    }

    @Override
    public Tracking tracking() {
      return tracking;
    }

    @Override
    public int formatVersion() {
      return TableMetadata.MIN_FORMAT_VERSION_PARQUET_MANIFESTS;
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
      return file.specId();
    }

    @Override
    public StructLike partition() {
      return partition;
    }

    @Override
    public ContentStats contentStats() {
      return stats;
    }

    @Override
    public Integer sortOrderId() {
      return null;
    }

    @Override
    public DeletionVector deletionVector() {
      return null;
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
      throw new UnsupportedOperationException(
          "Reusable content-file wrapper does not support copy(); materialize via a writer instead");
    }

    @Override
    public TrackedFile copyWithStats(Set<Integer> requestedColumnIds) {
      throw new UnsupportedOperationException(
          "Reusable content-file wrapper does not support copyWithStats()");
    }

    @Override
    public int size() {
      return TRACKED_FILE_FIELD_COUNT;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(TrackedFileStruct.getByPos(this, pos));
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException(
          "Reusable content-file wrapper does not support set()");
    }
  }

  /** Wraps a {@link DataFile} as a {@link TrackedFile} row. */
  static class DataTrackedFile extends ContentTrackedFile<DataFile> {
    DataTrackedFile(Schema writeSchema) {
      super(writeSchema);
    }

    /** Re-points this wrapper at {@code newFile} in place. */
    public TrackedFile wrap(DataFile newFile, Tracking tracking) {
      return wrapWithTracking(newFile, tracking);
    }

    @Override
    void validateContent(DataFile newFile) {
      Preconditions.checkArgument(
          newFile.content() == FileContent.DATA,
          "Invalid content for data file: %s",
          newFile.content());
    }

    @Override
    public FileContent contentType() {
      return FileContent.DATA;
    }

    @Override
    public Integer sortOrderId() {
      return file().sortOrderId();
    }
  }

  /** Wraps an equality {@link DeleteFile} as a {@link TrackedFile} row. */
  static class EqualityDeleteTrackedFile extends ContentTrackedFile<DeleteFile> {
    EqualityDeleteTrackedFile(Schema writeSchema) {
      super(writeSchema);
    }

    /** Re-points this wrapper at {@code newFile} in place. */
    public TrackedFile wrap(DeleteFile newFile, Tracking tracking) {
      return wrapWithTracking(newFile, tracking);
    }

    @Override
    void validateContent(DeleteFile newFile) {
      Preconditions.checkArgument(
          newFile.content() == FileContent.EQUALITY_DELETES,
          "Invalid content for delete file: %s",
          newFile.content());
    }

    @Override
    public FileContent contentType() {
      return FileContent.EQUALITY_DELETES;
    }

    @Override
    public List<Integer> equalityIds() {
      return file().equalityFieldIds();
    }
  }

  /** Wraps a {@link ManifestFile} as a v4+ leaf manifest row. */
  static class ManifestTrackedFile implements TrackedFile, StructLike {
    private final WrappedManifestInfo manifestInfo = new WrappedManifestInfo();
    private Tracking tracking;
    private ManifestFile manifest;
    private long recordCount;
    private FileContent contentType;

    ManifestTrackedFile() {}

    /**
     * Re-points this wrapper at {@code newManifest} in place.
     *
     * @param newManifest manifest file being referenced; must carry an assigned {@code
     *     sequence_number} and {@code min_sequence_number}
     * @param status entry status for the reference
     * @param firstRowId first-row-id resolved by the caller for a DATA manifest reference, or null
     *     for a DELETE manifest reference
     * @return this wrapper, or the original {@link TrackedFile} if {@code newManifest} is already
     *     adapted from a tracked file
     */
    public TrackedFile wrap(ManifestFile newManifest, EntryStatus status, Long firstRowId) {
      Preconditions.checkArgument(newManifest != null, "Invalid manifest file: null");
      Preconditions.checkArgument(status != null, "Invalid status: null");

      if (newManifest instanceof TrackedManifestFile) {
        return ((TrackedManifestFile) newManifest).file();
      }

      Long manifestSnapshotId = newManifest.snapshotId();
      Preconditions.checkArgument(manifestSnapshotId != null, "Invalid manifest snapshot id: null");
      long manifestSeq = newManifest.sequenceNumber();
      Preconditions.checkArgument(
          manifestSeq != ManifestWriter.UNASSIGNED_SEQ,
          "Invalid manifest reference %s: sequence_number is unassigned",
          newManifest.path());
      Preconditions.checkArgument(
          newManifest.minSequenceNumber() != ManifestWriter.UNASSIGNED_SEQ,
          "Invalid manifest reference %s: min_sequence_number is unassigned",
          newManifest.path());
      Preconditions.checkArgument(
          firstRowId == null || newManifest.content() == ManifestContent.DATA,
          "firstRowId is only valid for DATA manifests, but content is %s",
          newManifest.content());

      this.manifest = newManifest;
      this.contentType =
          newManifest.content() == ManifestContent.DATA
              ? FileContent.DATA_MANIFEST
              : FileContent.DELETE_MANIFEST;
      this.recordCount = resolveRecordCount(newManifest);
      this.tracking =
          new TrackingStruct(
              status, manifestSnapshotId, manifestSeq, manifestSeq, null, firstRowId, null, null);
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
    public int formatVersion() {
      // newly written ManifestFile instances have no persisted version; carry-over rows keep
      // theirs via unwrap of TrackedManifestFile
      return TableMetadata.MIN_FORMAT_VERSION_PARQUET_MANIFESTS;
    }

    @Override
    public String location() {
      return manifest.path();
    }

    @Override
    public FileFormat fileFormat() {
      return FileFormat.fromFileName(manifest.path());
    }

    @Override
    public long recordCount() {
      return recordCount;
    }

    @Override
    public long fileSizeInBytes() {
      return manifest.length();
    }

    @Override
    public Integer specId() {
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
      throw new UnsupportedOperationException(
          "Reusable manifest-reference wrapper does not support copy()");
    }

    @Override
    public TrackedFile copyWithStats(Set<Integer> requestedColumnIds) {
      throw new UnsupportedOperationException(
          "Reusable manifest-reference wrapper does not support copyWithStats()");
    }

    @Override
    public int size() {
      return TRACKED_FILE_FIELD_COUNT;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(TrackedFileStruct.getByPos(this, pos));
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException(
          "Reusable manifest-reference wrapper does not support set()");
    }
  }

  /** Reusable {@link ManifestInfo} view over a {@link ManifestFile}'s counts. */
  private static class WrappedManifestInfo implements ManifestInfo, StructLike {
    private ManifestFile manifest;

    void wrap(ManifestFile newManifest) {
      this.manifest = newManifest;
    }

    @Override
    public int addedFilesCount() {
      return zeroIfNull(manifest.addedFilesCount());
    }

    @Override
    public int existingFilesCount() {
      return zeroIfNull(manifest.existingFilesCount());
    }

    @Override
    public int deletedFilesCount() {
      return zeroIfNull(manifest.deletedFilesCount());
    }

    @Override
    public int replacedFilesCount() {
      return zeroIfNull(manifest.replacedFilesCount());
    }

    @Override
    public long addedRowsCount() {
      return zeroIfNull(manifest.addedRowsCount());
    }

    @Override
    public long existingRowsCount() {
      return zeroIfNull(manifest.existingRowsCount());
    }

    @Override
    public long deletedRowsCount() {
      return zeroIfNull(manifest.deletedRowsCount());
    }

    @Override
    public long replacedRowsCount() {
      return zeroIfNull(manifest.replacedRowsCount());
    }

    @Override
    public long minSequenceNumber() {
      return manifest.minSequenceNumber();
    }

    @Override
    public ByteBuffer dv() {
      return null;
    }

    @Override
    public Long dvCardinality() {
      return null;
    }

    @Override
    public ManifestInfo copy() {
      throw new UnsupportedOperationException(
          "Reusable manifest-info wrapper does not support copy()");
    }

    @Override
    public int size() {
      return MANIFEST_INFO_FIELD_COUNT;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      Object value =
          switch (pos) {
            case 0 -> addedFilesCount();
            case 1 -> existingFilesCount();
            case 2 -> deletedFilesCount();
            case 3 -> replacedFilesCount();
            case 4 -> addedRowsCount();
            case 5 -> existingRowsCount();
            case 6 -> deletedRowsCount();
            case 7 -> replacedRowsCount();
            case 8 -> minSequenceNumber();
            case 9 -> dv();
            case 10 -> dvCardinality();
            default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
          };
      return javaClass.cast(value);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException(
          "Reusable manifest-info wrapper does not support set()");
    }

    private static int zeroIfNull(Integer value) {
      return value != null ? value : 0;
    }

    private static long zeroIfNull(Long value) {
      return value != null ? value : 0L;
    }
  }

  /**
   * Returns the struct type of a nested field in a write schema, or null if the schema projects no
   * fields for it. A field with no fields to write is unknown in the schema and is reported as
   * missing rather than empty, matching {@link TrackedFileStruct}.
   */
  private static Types.StructType nestedStruct(Schema writeSchema, int fieldId) {
    Type type = writeSchema.findType(fieldId);
    Preconditions.checkArgument(type != null, "Invalid write schema: missing field ID %s", fieldId);
    if (type.typeId() == Type.TypeID.UNKNOWN) {
      return null;
    }

    Preconditions.checkArgument(
        type.isStructType(),
        "Invalid write schema field %s: expected struct or unknown, got %s",
        fieldId,
        type);

    Types.StructType struct = type.asStructType();
    return struct.fields().isEmpty() ? null : struct;
  }

  private static PartitionSpec resolveSpec(
      TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Integer specId = file.specId();
    if (specId != null) {
      PartitionSpec spec = specsById.get(specId);
      Preconditions.checkArgument(
          spec != null, "Cannot find partition spec for spec ID: %s", specId);
      return spec;
    }

    // A null spec ID means the file is unpartitioned; use the table's unpartitioned spec.
    for (PartitionSpec spec : specsById.values()) {
      if (spec.isUnpartitioned()) {
        return spec;
      }
    }

    throw new IllegalArgumentException(
        "Cannot find unpartitioned spec in specs: " + specsById.keySet());
  }

  /**
   * Projects the file's per-spec partition tuple into the target partition schema by field ID.
   * Fields present in the target but not in the file's spec land as null.
   */
  private static StructProjection projectPartition(
      ContentFile<?> file, Types.StructType partitionType) {
    StructLike partition = file.partition();
    Types.StructType sourceType;
    if (partition instanceof PartitionData) {
      sourceType = ((PartitionData) partition).getPartitionType();
    } else if (partition == null || partition.size() == 0) {
      sourceType = Types.StructType.of();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Cannot project partition for %s: partition type is unavailable for %s",
              file.location(), partition));
    }

    return StructProjection.createAllowMissing(sourceType, partitionType).wrap(partition);
  }

  /** Resolves record_count for a manifest-reference row from its per-status file counts. */
  private static long resolveRecordCount(ManifestFile manifest) {
    long total = 0L;
    if (manifest.addedFilesCount() != null) {
      total += manifest.addedFilesCount();
    }

    if (manifest.existingFilesCount() != null) {
      total += manifest.existingFilesCount();
    }

    if (manifest.deletedFilesCount() != null) {
      total += manifest.deletedFilesCount();
    }

    if (manifest.replacedFilesCount() != null) {
      total += manifest.replacedFilesCount();
    }

    return total;
  }
}
