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
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructProjection;

/**
 * Builds {@link TrackedFile} instances for v4 content_entry rows from legacy {@link ManifestEntry}
 * and {@link ManifestFile} inputs.
 */
class ContentEntryAdapters {
  /**
   * format_version for content_entry rows produced by a v4 writer. Matches the table format version
   * (4).
   */
  static final int V4_FORMAT_VERSION = 4;

  /**
   * format_version for content_entry rows that reference a leaf manifest written by a pre-v4 writer
   * (v1, v2, or v3). Used at the root manifest level when a v4 root carries over legacy leaf
   * manifests during a v3-to-v4 upgrade. Matches a pre-v4 table format version (0 is the sentinel;
   * v1/v2/v3 leaves are not re-encoded as content_entry, so the root just tags the reference as
   * legacy).
   */
  static final int LEGACY_FORMAT_VERSION = 0;

  // Cache of primitive-field types keyed by table schema. The originalTypes map a Metrics
  // instance carries is read-only inside MetricsUtil.fromMetrics, so a single per-schema map
  // can be shared across every adapter call for a writer's lifetime instead of being rebuilt
  // per row. WeakHashMap keys release when callers stop referencing the schema.
  private static final Map<Schema, Map<Integer, Type>> PRIMITIVE_TYPES_BY_SCHEMA =
      Collections.synchronizedMap(new WeakHashMap<>());

  private ContentEntryAdapters() {}

  static TrackedFile fromDataFile(
      ManifestEntry<DataFile> entry,
      Schema tableSchema,
      Types.StructType unionPartitionType,
      EntryStatus statusOverride) {
    Preconditions.checkArgument(entry != null, "Invalid manifest entry: null");
    DataFile file = entry.file();
    Preconditions.checkArgument(file != null, "Invalid data file: null");
    Preconditions.checkArgument(
        file.content() == FileContent.DATA, "Invalid content for data file: %s", file.content());
    return buildContentFileEntry(
        file,
        statusOverride,
        entry.snapshotId(),
        entry.dataSequenceNumber(),
        entry.fileSequenceNumber(),
        tableSchema,
        unionPartitionType);
  }

  static TrackedFile fromDeleteFile(
      ManifestEntry<DeleteFile> entry,
      Schema tableSchema,
      Types.StructType unionPartitionType,
      EntryStatus statusOverride) {
    Preconditions.checkArgument(entry != null, "Invalid manifest entry: null");
    DeleteFile file = entry.file();
    Preconditions.checkArgument(file != null, "Invalid delete file: null");
    // v4 leaf delete manifests must only contain content_type=EQUALITY_DELETES (per spec PR
    // #16025). Reject POSITION_DELETES with a hint that distinguishes the two legacy shapes by
    // file format (the canonical DV check per ContentFileUtil.isDV — both shapes can carry a
    // referencedDataFile, so that field is not a reliable distinguisher):
    //   - v3 delete vector (POSITION_DELETES stored as a Puffin blob) must be colocated on the
    //     data file's content_entry via TrackedFileBuilder.deletionVector(...) — see
    //     MergingSnapshotProducer's row-delta path.
    //   - v2 standalone position delete file (POSITION_DELETES stored in Parquet/Avro/ORC) has no
    //     v4 representation; it can only live in pre-v4 legacy manifests carried over via a
    //     format_version=0 manifest reference.
    if (file.content() == FileContent.POSITION_DELETES) {
      throw new IllegalArgumentException(
          ContentFileUtil.isDV(file)
              ? String.format(
                  Locale.ROOT,
                  "v3 delete vectors must be colocated on the data file's content_entry, not "
                      + "written as a delete manifest entry: %s referencing %s",
                  file.location(),
                  file.referencedDataFile())
              : String.format(
                  Locale.ROOT,
                  "v2 position delete files have no v4 representation; carry them over via a "
                      + "legacy v3 manifest with format_version=0: %s",
                  file.location()));
    }

    Preconditions.checkArgument(
        file.content() == FileContent.EQUALITY_DELETES,
        "Invalid content for delete file: %s",
        file.content());
    return buildContentFileEntry(
        file,
        statusOverride,
        entry.snapshotId(),
        entry.dataSequenceNumber(),
        entry.fileSequenceNumber(),
        tableSchema,
        unionPartitionType);
  }

  /**
   * Builds a manifest reference content_entry for the v4 root manifest.
   *
   * @param manifest the leaf manifest being referenced
   * @param formatVersion {@link #V4_FORMAT_VERSION} (4) for a v4 leaf manifest, or {@link
   *     #LEGACY_FORMAT_VERSION} (0) for a pre-v4 (v1, v2, or v3) leaf manifest carried over during
   *     a v3-to-v4 upgrade. Callers are responsible for resolving the value; this avoids adding a
   *     v4-only accessor to the public ManifestFile interface for a value with no production
   *     consumer at this layer.
   * @param status entry status (typically ADDED for a newly written leaf, EXISTING for a
   *     carried-over reference)
   * @param firstRowId the resolved first-row-id to write for this reference, or null for delete
   *     manifests. Callers are responsible for resolving the value (either carrying over {@link
   *     ManifestFile#firstRowId()} or assigning from a writer-side counter); the adapter does not
   *     decide between the two.
   */
  static TrackedFile fromManifestFile(
      ManifestFile manifest, int formatVersion, EntryStatus status, Long firstRowId) {
    Preconditions.checkArgument(manifest != null, "Invalid manifest file: null");
    Preconditions.checkArgument(
        formatVersion == LEGACY_FORMAT_VERSION || formatVersion >= V4_FORMAT_VERSION,
        "Invalid format_version: %s (must be %s for legacy v1-v3 or >= %s for v4+)",
        formatVersion,
        LEGACY_FORMAT_VERSION,
        V4_FORMAT_VERSION);
    Preconditions.checkArgument(status != null, "Invalid status: null");
    Long manifestSnapshotId = manifest.snapshotId();
    Preconditions.checkArgument(manifestSnapshotId != null, "Invalid manifest snapshot id: null");
    Preconditions.checkArgument(
        firstRowId == null || manifest.content() == ManifestContent.DATA,
        "firstRowId is only valid for DATA manifests, but content is %s",
        manifest.content());

    long manifestSeq = manifest.sequenceNumber();
    Preconditions.checkArgument(
        manifestSeq != ManifestWriter.UNASSIGNED_SEQ,
        "Cannot build content_entry for manifest reference %s: sequence_number is unassigned. "
            + "Resolve via RootManifestWriter.assignSequenceNumber before writing.",
        manifest.path());
    ManifestInfo info = manifestInfo(manifest);

    FileContent contentType =
        manifest.content() == ManifestContent.DATA
            ? FileContent.DATA_MANIFEST
            : FileContent.DELETE_MANIFEST;
    // Prefer the manifest's persisted record count when available (set by V4Writer on write and
    // by RootManifestReader on read). Fall back to summing the per-status file counts when the
    // value is not tracked (legacy v1-v3 manifests). Note that the fall-back sum cannot include
    // MODIFIED entries because manifest_info does not carry that count; v4 writers must set
    // recordCount directly so MODIFIED entries are reflected.
    Long persistedCount = manifest.recordCount();
    long recordCount = persistedCount != null ? persistedCount : manifestEntryCount(manifest);
    TrackedFileBuilder builder =
        TrackedFileBuilder.explicitTracking(
            contentType,
            TrackingBuilder.forContentEntry(
                status, manifestSnapshotId, manifestSeq, manifestSeq, firstRowId));
    builder
        .formatVersion(formatVersion)
        .location(manifest.path())
        .fileFormat(FileFormat.fromFileName(manifest.path()))
        .recordCount(recordCount)
        .fileSizeInBytes(manifest.length())
        .specId(manifest.partitionSpecId())
        .manifestInfo(info);

    if (manifest.keyMetadata() != null) {
      builder.keyMetadata(manifest.keyMetadata());
    }

    return builder.build();
  }

  private static TrackedFile buildContentFileEntry(
      ContentFile<?> file,
      EntryStatus status,
      long snapshotId,
      Long dataSequenceNumber,
      Long fileSequenceNumber,
      Schema tableSchema,
      Types.StructType unionPartitionType) {
    Preconditions.checkArgument(status != null, "Invalid status: null");
    // fromDataFile / fromDeleteFile project legacy ManifestEntry rows, whose status is ADDED,
    // EXISTING, or DELETED. MODIFIED and REPLACED have no legacy representation — they're written
    // directly by V4Writer.prepareWithStatus via TrackedFileBuilder.from(source, sid).
    // deletionVector(dv).build() (MODIFIED) and TrackedFileBuilder.replaced(source, sid)
    // (REPLACED).
    Preconditions.checkArgument(
        status == EntryStatus.ADDED
            || status == EntryStatus.EXISTING
            || status == EntryStatus.DELETED,
        "Unsupported status for content file entry: %s (use V4Writer.prepareWithStatus for "
            + "MODIFIED/REPLACED transitions)",
        status);
    PartitionData partition = toPartitionData(file, unionPartitionType);
    FileFormat format = file.format();
    Preconditions.checkArgument(
        format != null, "Invalid file format: null for %s", file.location());
    ContentStats stats = MetricsUtil.fromMetrics(tableSchema, toMetrics(file, tableSchema));
    boolean isDataFile = file.content() == FileContent.DATA;

    TrackedFileBuilder builder;
    if (status == EntryStatus.ADDED) {
      builder =
          isDataFile
              ? TrackedFileBuilder.data(snapshotId)
              : TrackedFileBuilder.equalityDelete(snapshotId);
    } else {
      Preconditions.checkArgument(
          dataSequenceNumber != null, "Invalid data sequence number: null for non-ADDED entry");
      Preconditions.checkArgument(
          fileSequenceNumber != null, "Invalid file sequence number: null for non-ADDED entry");
      Long firstRowId = isDataFile ? ((DataFile) file).firstRowId() : null;
      FileContent contentType = isDataFile ? FileContent.DATA : FileContent.EQUALITY_DELETES;
      builder =
          TrackedFileBuilder.explicitTracking(
              contentType,
              TrackingBuilder.forContentEntry(
                  status, snapshotId, dataSequenceNumber, fileSequenceNumber, firstRowId));
    }

    builder
        .formatVersion(V4_FORMAT_VERSION)
        .location(file.location())
        .fileFormat(format)
        .partition(partition)
        .recordCount(file.recordCount())
        .fileSizeInBytes(file.fileSizeInBytes())
        .specId(file.specId());

    if (stats != null) {
      builder.contentStats(stats);
    }

    if (file.sortOrderId() != null && isDataFile) {
      builder.sortOrderId(file.sortOrderId());
    }

    if (file.keyMetadata() != null) {
      builder.keyMetadata(file.keyMetadata());
    }

    if (file.splitOffsets() != null) {
      builder.splitOffsets(file.splitOffsets());
    }

    if (!isDataFile && file.equalityFieldIds() != null) {
      builder.equalityIds(file.equalityFieldIds());
    }

    return builder.build();
  }

  /**
   * Returns the number of entries in the referenced leaf manifest — used as the {@code
   * record_count} of a manifest-reference content_entry. Each file the manifest indexes is one row
   * in the manifest file's own encoding, so summing the file counts gives the leaf manifest's
   * record count.
   *
   * <p>Data-row totals (sum of rows across the files the manifest indexes) live in {@code
   * manifest_info}, not in this field — they describe a different file (the data files), not the
   * manifest itself.
   */
  private static long manifestEntryCount(ManifestFile manifest) {
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

  private static ManifestInfo manifestInfo(ManifestFile manifest) {
    long minSeq = manifest.minSequenceNumber();
    Preconditions.checkArgument(
        minSeq != ManifestWriter.UNASSIGNED_SEQ,
        "Cannot build manifest_info for %s: min_sequence_number is unassigned. "
            + "Resolve via RootManifestWriter.assignSequenceNumber before writing.",
        manifest.path());

    return ManifestInfoStruct.builder()
        .addedFilesCount(zeroIfNull(manifest.addedFilesCount()))
        .existingFilesCount(zeroIfNull(manifest.existingFilesCount()))
        .deletedFilesCount(zeroIfNull(manifest.deletedFilesCount()))
        .replacedFilesCount(zeroIfNull(manifest.replacedFilesCount()))
        .addedRowsCount(zeroIfNull(manifest.addedRowsCount()))
        .existingRowsCount(zeroIfNull(manifest.existingRowsCount()))
        .deletedRowsCount(zeroIfNull(manifest.deletedRowsCount()))
        .replacedRowsCount(zeroIfNull(manifest.replacedRowsCount()))
        .minSequenceNumber(minSeq)
        .build();
  }

  private static int zeroIfNull(Integer value) {
    return value != null ? value : 0;
  }

  private static long zeroIfNull(Long value) {
    return value != null ? value : 0L;
  }

  /**
   * Projects the file's per-spec partition tuple into the unified partition schema (union of all
   * live specs) used by every v4 content_entry row. Result fields are sourced by field ID via
   * {@link StructProjection#createAllowMissing}; fields not present in the writer spec land as
   * null.
   */
  private static PartitionData toPartitionData(
      ContentFile<?> file, Types.StructType unionPartitionType) {
    Preconditions.checkArgument(
        unionPartitionType != null, "Invalid union partition type: null for %s", file.location());
    PartitionData result = new PartitionData(unionPartitionType);
    StructLike partition = file.partition();
    if (partition == null) {
      return result;
    }

    Types.StructType sourceType;
    if (partition instanceof PartitionData) {
      sourceType = ((PartitionData) partition).getPartitionType();
    } else {
      // Without a backing PartitionData the partition's element types are unavailable, so an empty
      // struct is the only safe materialization. Reject any non-empty case rather than silently
      // dropping fields.
      Preconditions.checkArgument(
          partition.size() == 0,
          "Cannot convert partition for %s: type information is unavailable for %s",
          file.location(),
          partition);
      return result;
    }

    if (sourceType.fields().isEmpty()) {
      return result;
    }

    // Build a per-call StructProjection (not cached: the writer spec varies across files in
    // multi-spec carry-over flows). Field-ID lookup means partition values land in the correct
    // union-schema position regardless of the writer spec's ordering.
    StructProjection projection =
        StructProjection.createAllowMissing(sourceType, unionPartitionType);
    projection.wrap(partition);
    for (int pos = 0; pos < unionPartitionType.fields().size(); pos += 1) {
      result.set(pos, projection.get(pos, Object.class));
    }
    return result;
  }

  private static Metrics toMetrics(ContentFile<?> file, Schema tableSchema) {
    Map<Integer, ByteBuffer> lowerBounds = file.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = file.upperBounds();
    boolean hasBounds =
        (lowerBounds != null && !lowerBounds.isEmpty())
            || (upperBounds != null && !upperBounds.isEmpty());
    Map<Integer, Type> originalTypes = hasBounds ? primitiveTypesFor(tableSchema) : null;

    return new Metrics(
        file.recordCount(),
        file.columnSizes(),
        file.valueCounts(),
        file.nullValueCounts(),
        file.nanValueCounts(),
        lowerBounds,
        upperBounds,
        originalTypes);
  }

  private static Map<Integer, Type> primitiveTypesFor(Schema schema) {
    if (schema == null) {
      return null;
    }

    Map<Integer, Type> cached = PRIMITIVE_TYPES_BY_SCHEMA.get(schema);
    if (cached != null) {
      return cached;
    }

    Map<Integer, Type> types = Maps.newHashMap();
    for (Types.NestedField field : schema.columns()) {
      collectPrimitiveTypes(field, types);
    }

    Map<Integer, Type> result = Collections.unmodifiableMap(types);
    PRIMITIVE_TYPES_BY_SCHEMA.put(schema, result);
    return result;
  }

  private static void collectPrimitiveTypes(Types.NestedField field, Map<Integer, Type> types) {
    Type type = field.type();
    if (type.isPrimitiveType()) {
      types.put(field.fieldId(), type);
      return;
    }

    if (type.isStructType()) {
      for (Types.NestedField nested : type.asStructType().fields()) {
        collectPrimitiveTypes(nested, types);
      }
      return;
    }

    if (type.isListType()) {
      collectPrimitiveTypes(type.asListType().fields().get(0), types);
      return;
    }

    if (type.isMapType()) {
      for (Types.NestedField nested : type.asMapType().fields()) {
        collectPrimitiveTypes(nested, types);
      }
    }
  }
}
