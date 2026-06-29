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
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

/**
 * Builds {@link TrackedFile} instances for v4+ content_entry rows from legacy {@link ManifestEntry}
 * and {@link ManifestFile} inputs.
 */
class ContentEntryAdapters {
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
    // v4+ leaf delete manifests must only contain content_type=EQUALITY_DELETES (per spec PR
    // #16025). POSITION_DELETES has no v4+ leaf representation:
    //   - v3 delete vectors (POSITION_DELETES stored as a Puffin blob) must be colocated on the
    //     data file's content_entry via TrackedFileBuilder.deletionVector(...) — see
    //     MergingSnapshotProducer's row-delta path.
    //   - v2 standalone position delete files (POSITION_DELETES stored in Parquet/Avro/ORC) have
    //     no v4+ representation; they can only live in pre-v4 legacy manifests carried over via a
    //     format_version=0 manifest reference.
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
   * Builds a manifest reference content_entry for the v4+ root manifest.
   *
   * <p>The on-disk {@code format_version} field is sourced from {@link
   * ManifestFile#formatVersion()} (0 for pre-v4 leaves carried over during a v3-to-v4 upgrade, 4
   * for v4+ leaves). For v4+ leaves the source manifest must carry a non-null {@link
   * ManifestFile#recordCount()} populated by the v4+ writer or root-manifest reader; for pre-v4
   * leaves the record_count is summed from the per-status file counts.
   *
   * @param status entry status (typically ADDED for a newly written leaf, EXISTING for a
   *     carried-over reference)
   * @param firstRowId the resolved first-row-id to write for this reference, or null for delete
   *     manifests. Callers are responsible for resolving the value (either carrying over {@link
   *     ManifestFile#firstRowId()} or assigning from a writer-side counter); the adapter does not
   *     decide between the two.
   */
  static TrackedFile fromManifestFile(ManifestFile manifest, EntryStatus status, Long firstRowId) {
    Preconditions.checkArgument(manifest != null, "Invalid manifest file: null");
    Preconditions.checkArgument(status != null, "Invalid status: null");
    int formatVersion = manifest.formatVersion();
    Preconditions.checkArgument(
        formatVersion == ManifestFile.LEGACY_FORMAT_VERSION
            || formatVersion >= ManifestFile.V4_FORMAT_VERSION,
        "Invalid manifest format_version: %s (must be %s for pre-v4 or >= %s for v4+)",
        formatVersion,
        ManifestFile.LEGACY_FORMAT_VERSION,
        ManifestFile.V4_FORMAT_VERSION);
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
    long recordCount = resolveRecordCount(manifest);

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
      Long snapshotId,
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
      // snapshotId may be null for a staged-write ADDED entry (e.g., FastAppend writing the
      // manifest before the commit snapshot is assigned). The snapshot ID is inherited at read
      // time from the manifest list's added_snapshot_id.
      builder =
          isDataFile
              ? TrackedFileBuilder.data(snapshotId)
              : TrackedFileBuilder.equalityDelete(snapshotId);
    } else {
      Preconditions.checkArgument(
          snapshotId != null, "Invalid snapshot id: null for non-ADDED entry");
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
        .formatVersion(ManifestFile.V4_FORMAT_VERSION)
        .location(file.location())
        .fileFormat(format)
        .partition(partition)
        .recordCount(file.recordCount())
        .fileSizeInBytes(file.fileSizeInBytes())
        .specId(file.specId());

    populateOptionalFields(builder, file, stats, isDataFile);

    return builder.build();
  }

  private static void populateOptionalFields(
      TrackedFileBuilder builder, ContentFile<?> file, ContentStats stats, boolean isDataFile) {
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
  }

  /**
   * Returns the record_count for a manifest-reference content_entry row. For v4+ manifests this is
   * the persisted record_count populated by the writer or reader; for pre-v4 manifests it is summed
   * from the per-status file counts. REPLACED is not summed because it has no pre-v4
   * representation.
   */
  private static long resolveRecordCount(ManifestFile manifest) {
    if (manifest.formatVersion() >= ManifestFile.V4_FORMAT_VERSION) {
      Long persisted = manifest.recordCount();
      Preconditions.checkArgument(
          persisted != null,
          "Invalid v4 manifest reference for %s: record_count must be set by the writer",
          manifest.path());
      return persisted;
    }

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
   * live specs) used by every v4+ content_entry row. Result fields are sourced by field ID via
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
