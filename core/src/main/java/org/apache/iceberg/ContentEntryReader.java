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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructProjection;

/**
 * Reads v4+ leaf manifest files using the {@code content_entry} Parquet schema. Rows are projected
 * to legacy {@link ManifestEntry} instances so downstream consumers work unchanged.
 *
 * <p>Each row is validated: {@link #SUPPORTED_FORMAT_VERSION} is an upper bound on the per-row
 * {@code format_version} field.
 *
 * <p>Dispatch: callers with a {@code format_version} hint from the parent root manifest entry call
 * this reader directly; callers without a hint reach this reader via {@link ManifestFiles}
 * schema-shape detection (field id 134 or 147 in the Parquet footer).
 */
class ContentEntryReader extends CloseableGroup {
  static final int SUPPORTED_FORMAT_VERSION = 4;

  private final InputFile file;
  private final ManifestContent contentType;
  private final Map<Integer, PartitionSpec> specsById;
  private final int defaultSpecId;
  private final InheritableMetadata inheritableMetadata;

  private ContentEntryReader(
      InputFile file,
      ManifestContent contentType,
      Map<Integer, PartitionSpec> specsById,
      int defaultSpecId,
      InheritableMetadata inheritableMetadata) {
    this.file = file;
    this.contentType = contentType;
    this.specsById = specsById;
    this.defaultSpecId = defaultSpecId;
    this.inheritableMetadata = inheritableMetadata;
  }

  /** Opens a content_entry reader for a data manifest (v4+ leaf). */
  static ContentEntryReader forData(
      InputFile file,
      int specId,
      Map<Integer, PartitionSpec> specsById,
      InheritableMetadata inheritableMetadata) {
    return new ContentEntryReader(
        file, ManifestContent.DATA, specsById, specId, inheritableMetadata);
  }

  /** Opens a content_entry reader for a delete manifest (v4+ leaf). */
  static ContentEntryReader forDelete(
      InputFile file,
      int specId,
      Map<Integer, PartitionSpec> specsById,
      InheritableMetadata inheritableMetadata) {
    return new ContentEntryReader(
        file, ManifestContent.DELETES, specsById, specId, inheritableMetadata);
  }

  /** Returns all entries (including deleted) as data manifest entries. */
  CloseableIterable<ManifestEntry<DataFile>> dataEntries() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read data entries from a delete manifest: %s",
        file.location());
    return readEntries();
  }

  /**
   * Returns data-file changes encoded as {@code (v4+ status, DataFile)} pairs. Unlike {@link
   * #dataEntries()} which collapses REPLACED → DELETED and MODIFIED → EXISTING for legacy
   * consumers, this method surfaces the v4+ tracking status directly so callers can distinguish
   * data-file changes (ADDED, DELETED) from DV-state transitions (REPLACED, MODIFIED). Used by
   * {@code SnapshotChanges} and {@code BaseSnapshot} to correctly classify per-snapshot data-file
   * changes on v4+ leaves.
   */
  CloseableIterable<Pair<EntryStatus, DataFile>> dataFileChanges() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read data file changes from a delete manifest: %s",
        file.location());
    return readDataFileChanges();
  }

  /**
   * Returns the colocated deletion vectors carried by live data rows in this data manifest, each
   * projected as a {@link DeleteFile} with content {@link FileContent#POSITION_DELETES} and format
   * {@link FileFormat#PUFFIN}. REPLACED rows are excluded — only live (ADDED or MODIFIED) rows
   * surface their attached DV. Rows without a {@code deletion_vector} are skipped.
   */
  CloseableIterable<DeleteFile> colocatedDVDeleteFiles() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read deletion vectors from a delete manifest: %s",
        file.location());
    return readDVDeleteFiles();
  }

  /**
   * Returns colocated DV changes encoded as {@code (status, DeleteFile)} pairs, suitable for
   * computing per-snapshot delete-file deltas:
   *
   * <ul>
   *   <li>{@link ManifestEntry.Status#ADDED} — DVs that became live in this manifest. Surfaced for
   *       ADDED rows (born-with-DV) and MODIFIED rows (DV added or updated).
   *   <li>{@link ManifestEntry.Status#DELETED} — DVs that were superseded. Surfaced for REPLACED
   *       rows that carry the prior DV (preserved by {@code V4Writer.prepareReplaced}).
   * </ul>
   *
   * <p>EXISTING rows (carried-over DVs that did not change) and rows without a {@code
   * deletion_vector} are skipped. The result is intended for {@code SnapshotChanges} and {@code
   * BaseSnapshot} to report DV adds/removes alongside legacy delete-manifest entries.
   */
  CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> colocatedDVChanges() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read deletion vector changes from a delete manifest: %s",
        file.location());
    return readDVChanges();
  }

  /** Returns all entries (including deleted) as delete manifest entries. */
  CloseableIterable<ManifestEntry<DeleteFile>> deleteEntries() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DELETES,
        "Cannot read delete entries from a data manifest: %s",
        file.location());
    @SuppressWarnings({"unchecked", "rawtypes"})
    CloseableIterable<ManifestEntry<DeleteFile>> result =
        (CloseableIterable<ManifestEntry<DeleteFile>>) (CloseableIterable) readEntries();
    return result;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private <F extends ContentFile<F>> CloseableIterable<ManifestEntry<F>> readEntries() {
    PartitionSpec defaultSpec = resolveDefaultSpec();
    Types.StructType statsType =
        StatsUtil.statsReadSchema(
            defaultSpec.schema(), TypeUtil.getProjectedIds(defaultSpec.schema()));
    Schema contentEntrySchema = buildContentEntrySchema(defaultSpec, statsType);

    InternalData.ReadBuilder builder =
        InternalData.read(FileFormat.PARQUET, file)
            .project(contentEntrySchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .setCustomType(TrackedFile.CONTENT_STATS_ID, ContentStatsStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class);
    // Read each per-column stats sub-struct as a FieldStatsStruct so ContentStatsStruct.set can
    // store them directly; unregistered nested structs would default to GenericRecord.
    for (Types.NestedField statsField : statsType.fields()) {
      builder.setCustomType(statsField.fieldId(), FieldStatsStruct.class);
    }

    CloseableIterable<TrackedFileStruct> rows = builder.build();

    addCloseable(rows);

    // toManifestEntry fully materializes each row into a new ManifestEntry (fresh file, metrics,
    // and partition), so the reused row container is safe to pass directly without a defensive copy.
    return (CloseableIterable<ManifestEntry<F>>)
        (CloseableIterable) CloseableIterable.transform(rows, this::toManifestEntry);
  }

  private CloseableIterable<DeleteFile> readDVDeleteFiles() {
    PartitionSpec defaultSpec = resolveDefaultSpec();
    Types.StructType statsType =
        StatsUtil.statsReadSchema(
            defaultSpec.schema(), TypeUtil.getProjectedIds(defaultSpec.schema()));
    Schema contentEntrySchema = buildContentEntrySchema(defaultSpec, statsType);

    InternalData.ReadBuilder builder =
        InternalData.read(FileFormat.PARQUET, file)
            .project(contentEntrySchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .setCustomType(TrackedFile.CONTENT_STATS_ID, ContentStatsStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class);
    // Register per-column stats sub-structs as FieldStatsStruct (same as readEntries) so the
    // projected content_stats column round-trips without defaulting nested structs to GenericRecord.
    for (Types.NestedField statsField : statsType.fields()) {
      builder.setCustomType(statsField.fieldId(), FieldStatsStruct.class);
    }

    CloseableIterable<TrackedFileStruct> rows = builder.build();

    addCloseable(rows);

    CloseableIterable<DeleteFile> dvs =
        CloseableIterable.transform(
            rows,
            row -> {
              TrackedFileStruct copy = (TrackedFileStruct) row.copy();
              if (!isLiveDataRowWithDV(copy)) {
                return null;
              }
              return toDVDeleteFile(copy);
            });

    return CloseableIterable.filter(dvs, dv -> dv != null);
  }

  private CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> readDVChanges() {
    PartitionSpec defaultSpec = resolveDefaultSpec();
    Schema contentEntrySchema = buildContentEntrySchema(defaultSpec);

    CloseableIterable<TrackedFileStruct> rows =
        InternalData.read(FileFormat.PARQUET, file)
            .project(contentEntrySchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .setCustomType(TrackedFile.CONTENT_STATS_ID, ContentStatsReader.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .build();

    addCloseable(rows);

    CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> changes =
        CloseableIterable.transform(
            rows,
            row -> {
              TrackedFileStruct copy = (TrackedFileStruct) row.copy();
              ManifestEntry.Status changeStatus = dvChangeStatus(copy);
              if (changeStatus == null) {
                return null;
              }
              return Pair.of(changeStatus, toDVDeleteFile(copy));
            });

    return CloseableIterable.filter(changes, p -> p != null);
  }

  private CloseableIterable<Pair<EntryStatus, DataFile>> readDataFileChanges() {
    PartitionSpec defaultSpec = resolveDefaultSpec();
    Schema contentEntrySchema = buildContentEntrySchema(defaultSpec);

    CloseableIterable<TrackedFileStruct> rows =
        InternalData.read(FileFormat.PARQUET, file)
            .project(contentEntrySchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .setCustomType(TrackedFile.CONTENT_STATS_ID, ContentStatsReader.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .build();

    addCloseable(rows);

    CloseableIterable<Pair<EntryStatus, DataFile>> changes =
        CloseableIterable.transform(
            rows,
            row -> {
              TrackedFileStruct copy = (TrackedFileStruct) row.copy();
              if (copy.contentType() != FileContent.DATA) {
                return null;
              }

              Tracking tracking = copy.tracking();
              if (tracking == null) {
                return null;
              }

              EntryStatus status = tracking.status();
              if (status != EntryStatus.ADDED && status != EntryStatus.DELETED) {
                return null;
              }

              Integer specId = copy.specId();
              PartitionSpec spec = specById(specId);
              if (spec == null) {
                spec = resolveDefaultSpec();
              }

              DataFile dataFile = toDataFile(copy, spec, tracking);
              // Apply InheritableMetadata so the returned DataFile carries the data/file sequence
              // numbers callers expect (BaseFile fields populated from the parent manifest's
              // sequence number for ADDED entries). Without this step DataFile.dataSequenceNumber()
              // would return null, breaking SnapshotChanges.addedDataFiles() consumers that
              // inspect sequence numbers.
              GenericManifestEntry<DataFile> entry =
                  new GenericManifestEntry<>(spec.partitionType());
              ManifestEntry.Status manifestStatus = toManifestStatus(status);
              setEntry(
                  entry,
                  manifestStatus,
                  tracking.snapshotId(),
                  tracking.dataSequenceNumber(),
                  tracking.fileSequenceNumber(),
                  dataFile);
              inheritableMetadata.apply(entry);
              return Pair.of(status, entry.file());
            });

    return CloseableIterable.filter(changes, p -> p != null);
  }

  // Builds a GenericDeleteFile from a v4+ colocated DV row. Using GenericDeleteFile (a BaseFile)
  // rather than TrackedFileAdapters.asDVDeleteFile lets InheritableMetadata propagate the
  // dataSequenceNumber from the parent manifest to the file — required for DeleteFileIndex's
  // sequence-number checks (DeleteFile.dataSequenceNumber() must be non-null and >= the data
  // file's sequence number).
  private DeleteFile toDVDeleteFile(TrackedFileStruct row) {
    Integer specId = row.specId();
    PartitionSpec spec = specById(specId);
    if (spec == null) {
      spec = resolveDefaultSpec();
    }

    DeletionVector dv = row.deletionVector();
    PartitionData partition = toPartitionData(row, spec);

    GenericDeleteFile dvFile =
        new GenericDeleteFile(
            spec.specId(),
            FileContent.POSITION_DELETES,
            dv.location(),
            FileFormat.PUFFIN,
            partition,
            dv.sizeInBytes(),
            new Metrics(dv.cardinality(), null, null, null, null, null, null),
            null /* no equality field ids */,
            null /* DVs are unsorted per spec */,
            null /* no split offsets */,
            null /* no key metadata */,
            row.location() /* referenced data file */,
            dv.offset(),
            dv.sizeInBytes());

    // The DV's effective data sequence number is the sequence of the snapshot that wrote (or
    // rewrote) this leaf manifest — the same as the manifest's sequenceNumber. Treat the DV row as
    // a freshly ADDED entry so InheritableMetadata.fromManifest assigns the manifest's
    // sequenceNumber to the DV. This matches v3 standalone DV-delete-manifest behavior, where the
    // DV's dataSequenceNumber is inherited from the manifest that introduced it.
    GenericManifestEntry<DeleteFile> entry = new GenericManifestEntry<>(spec.partitionType());
    entry.wrapAppendPreservingFirstRowId(null, null, dvFile);
    inheritableMetadata.apply(entry);
    return entry.file();
  }

  private static boolean isLiveDataRowWithDV(TrackedFileStruct row) {
    if (row.contentType() != FileContent.DATA) {
      return false;
    }

    if (row.deletionVector() == null) {
      return false;
    }

    Tracking tracking = row.tracking();
    if (tracking == null) {
      return false;
    }

    EntryStatus status = tracking.status();
    return status == EntryStatus.ADDED
        || status == EntryStatus.EXISTING
        || status == EntryStatus.MODIFIED;
  }

  // Maps a content_entry row to a per-snapshot DV-change status (ADDED for newly-live DVs,
  // DELETED for superseded DVs), or returns null for rows that do not represent a DV change in
  // this snapshot (no DV, equality-delete rows, or carried-over EXISTING/DELETED rows).
  private static ManifestEntry.Status dvChangeStatus(TrackedFileStruct row) {
    if (row.contentType() != FileContent.DATA) {
      return null;
    }

    if (row.deletionVector() == null) {
      return null;
    }

    Tracking tracking = row.tracking();
    if (tracking == null) {
      return null;
    }

    switch (tracking.status()) {
      case ADDED:
      case MODIFIED:
        // ADDED-with-DV (born-with-DV) and MODIFIED-with-DV (DV added/updated) — both surface the
        // DV as newly live in this snapshot.
        return ManifestEntry.Status.ADDED;
      case REPLACED:
        // REPLACED-with-DV — the prior DV preserved on the REPLACED row (see
        // V4Writer.prepareReplaced). Surfaced as removed in this snapshot.
        return ManifestEntry.Status.DELETED;
      default:
        // EXISTING (DV carried over unchanged) and DELETED (data-file removal; DV removal is not
        // tracked here) are ignored.
        return null;
    }
  }

  private PartitionSpec resolveDefaultSpec() {
    if (specsById != null && !specsById.isEmpty()) {
      PartitionSpec spec = specsById.get(defaultSpecId);
      if (spec != null) {
        return spec;
      }

      return specsById.values().iterator().next();
    }

    return PartitionSpec.unpartitioned();
  }

  private Schema buildContentEntrySchema(PartitionSpec spec, Types.StructType statsType) {
    // v4+ leaf manifests encode partition tuples with the union partition type (a struct covering
    // every live spec's fields). Read with the same union so per-spec subsets land in the correct
    // positions; per-spec projection happens later in toPartitionData. Empty unions fall back to
    // the placeholder used by the writer.
    Types.StructType partitionType =
        specsById != null && !specsById.isEmpty()
            ? Partitioning.partitionType(spec.schema(), specsById.values())
            : spec.partitionType();
    Types.StructType readPartitionType =
        ManifestWriter.V4Writer.emptyPartitionPlaceholderIfNeeded(partitionType);
    return new Schema(
        TrackedFile.schemaWithContentStats(readPartitionType, statsType).fields());
  }

  private ManifestEntry<?> toManifestEntry(TrackedFileStruct row) {
    int formatVersion = row.formatVersion();
    Preconditions.checkArgument(
        formatVersion <= SUPPORTED_FORMAT_VERSION,
        "Unsupported format_version: %s (max supported: %s)",
        formatVersion,
        SUPPORTED_FORMAT_VERSION);

    Tracking tracking = row.tracking();
    Preconditions.checkArgument(
        tracking != null,
        "Invalid content_entry row: missing tracking struct in %s",
        file.location());

    FileContent content = row.contentType();
    Preconditions.checkArgument(
        content != null, "Invalid content_entry row: missing content_type in %s", file.location());

    Integer specId = row.specId();
    PartitionSpec spec = specById(specId);
    if (spec == null) {
      spec = resolveDefaultSpec();
    }

    Long snapshotId = tracking.snapshotId();
    Long dataSequenceNumber = tracking.dataSequenceNumber();
    Long fileSequenceNumber = tracking.fileSequenceNumber();
    ManifestEntry.Status manifestStatus = toManifestStatus(tracking.status());

    if (content == FileContent.DATA) {
      DataFile dataFile = toDataFile(row, spec, tracking);
      GenericManifestEntry<DataFile> entry = new GenericManifestEntry<>(spec.partitionType());
      setEntry(entry, manifestStatus, snapshotId, dataSequenceNumber, fileSequenceNumber, dataFile);
      return inheritableMetadata.apply(entry);
    } else if (content == FileContent.EQUALITY_DELETES) {
      DeleteFile deleteFile = toEqualityDeleteFile(row, spec);
      GenericManifestEntry<DeleteFile> entry = new GenericManifestEntry<>(spec.partitionType());
      setEntry(
          entry, manifestStatus, snapshotId, dataSequenceNumber, fileSequenceNumber, deleteFile);
      return inheritableMetadata.apply(entry);
    } else {
      throw new IllegalArgumentException(
          "Unsupported content_type in leaf manifest: " + content + " in " + file.location());
    }
  }

  private static <F extends ContentFile<F>> void setEntry(
      GenericManifestEntry<F> entry,
      ManifestEntry.Status status,
      Long snapshotId,
      Long dataSequenceNumber,
      Long fileSequenceNumber,
      F file) {
    switch (status) {
      case ADDED:
        // Use wrapAppendPreservingFirstRowId so the firstRowId already set on the file (read from
        // the tracking struct) is not suppressed by Delegates.suppressFirstRowId.
        entry.wrapAppendPreservingFirstRowId(snapshotId, dataSequenceNumber, file);
        break;
      case EXISTING:
        entry.wrapExisting(snapshotId, dataSequenceNumber, fileSequenceNumber, file);
        break;
      case DELETED:
        entry.wrapDelete(snapshotId, dataSequenceNumber, fileSequenceNumber, file);
        break;
      default:
        throw new IllegalArgumentException("Unknown manifest status: " + status);
    }
  }

  private PartitionSpec specById(Integer specId) {
    if (specsById != null && specId != null) {
      return specsById.get(specId);
    }

    return null;
  }

  private DataFile toDataFile(TrackedFileStruct row, PartitionSpec spec, Tracking tracking) {
    Metrics metrics = toMetrics(row);
    PartitionData partition = toPartitionData(row, spec);
    Long firstRowId = tracking.firstRowId();

    return new GenericDataFile(
        spec.specId(),
        row.location(),
        row.fileFormat(),
        partition,
        row.fileSizeInBytes(),
        metrics,
        row.keyMetadata(),
        row.splitOffsets(),
        row.sortOrderId(),
        firstRowId);
  }

  private DeleteFile toEqualityDeleteFile(TrackedFileStruct row, PartitionSpec spec) {
    Metrics metrics = toMetrics(row);
    PartitionData partition = toPartitionData(row, spec);
    List<Integer> equalityIdList = row.equalityIds();
    int[] equalityIds = null;
    if (equalityIdList != null) {
      equalityIds = new int[equalityIdList.size()];
      for (int i = 0; i < equalityIdList.size(); i++) {
        equalityIds[i] = equalityIdList.get(i);
      }
    }

    return new GenericDeleteFile(
        spec.specId(),
        FileContent.EQUALITY_DELETES,
        row.location(),
        row.fileFormat(),
        partition,
        row.fileSizeInBytes(),
        metrics,
        equalityIds,
        row.sortOrderId(),
        row.splitOffsets(),
        row.keyMetadata(),
        null /* no referenced data file */,
        null /* no content offset */,
        null /* no content size */);
  }

  private static Metrics toMetrics(TrackedFileStruct row) {
    ContentStats contentStats = row.contentStats();
    return new Metrics(
        row.recordCount(),
        null /* column sizes not stored in content_stats */,
        MetricsUtil.valueCounts(contentStats),
        MetricsUtil.nullValueCounts(contentStats),
        MetricsUtil.nanValueCounts(contentStats),
        MetricsUtil.lowerBounds(contentStats),
        MetricsUtil.upperBounds(contentStats));
  }

  private static PartitionData toPartitionData(TrackedFileStruct row, PartitionSpec spec) {
    StructLike rowPartition = row.partition();
    Types.StructType specType = spec.partitionType();
    if (rowPartition instanceof PartitionData) {
      PartitionData unionPartition = (PartitionData) rowPartition;
      // The on-disk partition is encoded with the union partition type. Project back to the
      // writer spec's partition type so downstream consumers see a partition struct that matches
      // the file's own spec (and not a wider union shape).
      if (unionPartition.getPartitionType().equals(specType)) {
        return unionPartition.copy();
      }

      PartitionData result = new PartitionData(specType);
      StructProjection projection =
          StructProjection.createAllowMissing(unionPartition.getPartitionType(), specType);
      projection.wrap(unionPartition);
      for (int pos = 0; pos < specType.fields().size(); pos += 1) {
        result.set(pos, projection.get(pos, Object.class));
      }
      return result;
    }

    return new PartitionData(specType);
  }

  private static ManifestEntry.Status toManifestStatus(EntryStatus entryStatus) {
    switch (entryStatus) {
      case ADDED:
        return ManifestEntry.Status.ADDED;
      case EXISTING:
        return ManifestEntry.Status.EXISTING;
      case DELETED:
        return ManifestEntry.Status.DELETED;
      case REPLACED:
        // REPLACED is the prior state of a modified entry — non-live (isLive() == false). Surface
        // as DELETED so isLive() correctly returns false for legacy consumers. Downstream
        // rewrite paths (e.g., MergingSnapshotProducer.rewriteLeafManifestsWithDVs) and scan
        // planning rely on isLive() to drop stale REPLACED rows from prior commits.
        return ManifestEntry.Status.DELETED;
      case MODIFIED:
        // MODIFIED is the live state of a modified entry; surface as EXISTING for legacy consumers
        return ManifestEntry.Status.EXISTING;
      default:
        throw new IllegalArgumentException("Unknown entry status: " + entryStatus);
    }
  }
}
