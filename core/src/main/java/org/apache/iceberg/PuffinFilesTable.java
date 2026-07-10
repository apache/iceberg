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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * A {@link Table} implementation that exposes Puffin files associated with a selected snapshot.
 *
 * <p>Deletion vector files are derived from the selected snapshot's live delete manifests.
 * Statistics files are derived from the statistics files currently registered in table metadata for
 * the selected snapshot.
 *
 * <p>Statistics registration may be updated independently of snapshots. Therefore, time travel
 * queries return the statistics files currently associated with the selected snapshot, rather than
 * the statistics registration state at the time the snapshot was committed.
 *
 * <p>Each row represents a Puffin file associated with the selected snapshot through one metadata
 * source. A physical Puffin file may appear more than once if it is associated through multiple
 * sources.
 */
public class PuffinFilesTable extends BaseMetadataTable {

  private static final String SOURCE_STATISTICS = "statistics";
  private static final String SOURCE_DELETION_VECTOR = "deletion_vector";

  // Delete manifests do not store Puffin blob metadata. Iceberg currently defines Puffin delete
  // files as deletion-vector-v1 blobs over the row-position metadata field.
  private static final List<String> DV_REFERENCED_BLOB_TYPES =
      ImmutableList.of(StandardBlobTypes.DV_V1);
  private static final List<Integer> DV_REFERENCED_FIELD_IDS =
      ImmutableList.of(MetadataColumns.ROW_POSITION.fieldId());

  private static final List<String> DV_SCAN_COLUMNS =
      ImmutableList.of(
          DataFile.FILE_PATH.name(),
          DataFile.FILE_FORMAT.name(),
          DataFile.FILE_SIZE.name(),
          DataFile.CONTENT_OFFSET.name(),
          DataFile.CONTENT_SIZE.name());

  private static final Schema PUFFIN_FILES_SCHEMA =
      new Schema(
          Types.NestedField.required(
              1, "snapshot_id", Types.LongType.get(), "ID of the selected snapshot"),
          Types.NestedField.required(
              2,
              "file_path",
              Types.StringType.get(),
              "Fully qualified location of the Puffin file"),
          Types.NestedField.required(
              3,
              "source",
              Types.StringType.get(),
              "Metadata source that associates the Puffin file with the selected snapshot"),
          Types.NestedField.required(
              4,
              "file_size_in_bytes",
              Types.LongType.get(),
              "Total size of the Puffin file in bytes"),
          Types.NestedField.required(
              5,
              "referenced_blob_count",
              Types.IntegerType.get(),
              "Number of distinct blobs in the Puffin file referenced by the selected snapshot"),
          Types.NestedField.required(
              6,
              "referenced_blob_types",
              Types.ListType.ofRequired(7, Types.StringType.get()),
              "Distinct types of blobs referenced by the selected snapshot"),
          Types.NestedField.required(
              8,
              "referenced_fields",
              Types.ListType.ofRequired(
                  9,
                  Types.StructType.of(
                      Types.NestedField.required(
                          10,
                          "field_id",
                          Types.IntegerType.get(),
                          "Field ID referenced by at least one blob"),
                      Types.NestedField.optional(
                          11,
                          "current_field_name",
                          Types.StringType.get(),
                          "Name currently assigned to the field ID; null if no longer present"))),
              "Distinct fields referenced across the selected blobs"));

  PuffinFilesTable(Table table) {
    this(table, table.name() + ".puffin_files");
  }

  PuffinFilesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new PuffinFilesTableScan(table());
  }

  @Override
  public Schema schema() {
    return PUFFIN_FILES_SCHEMA;
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.PUFFIN_FILES;
  }

  private DataTask task(BaseTableScan scan) {
    Snapshot snapshot = scan.snapshot();
    Preconditions.checkState(snapshot != null, "Cannot plan Puffin files without a snapshot");

    Schema baseTableSchema = table().schema();

    return StaticDataTask.of(
        table().io().newInputFile(taskLocation(snapshot)),
        schema(),
        scan.schema(),
        puffinFiles(snapshot),
        puffinFile -> puffinFileToRow(baseTableSchema, puffinFile));
  }

  private String taskLocation(Snapshot snapshot) {
    if (snapshot.manifestListLocation() != null) {
      return snapshot.manifestListLocation();
    }

    String metadataFileLocation = table().operations().current().metadataFileLocation();
    Preconditions.checkState(
        metadataFileLocation != null,
        "Cannot determine a metadata file location for Puffin files table %s",
        name());

    return metadataFileLocation;
  }

  private List<PuffinFileRow> puffinFiles(Snapshot snapshot) {
    ImmutableList.Builder<PuffinFileRow> rows = ImmutableList.builder();
    long snapshotId = snapshot.snapshotId();

    for (StatisticsFile statisticsFile : table().statisticsFiles()) {
      if (statisticsFile.snapshotId() == snapshotId) {
        rows.add(PuffinFileRow.fromStatisticsFile(snapshotId, statisticsFile));
      }
    }

    rows.addAll(deletionVectorPuffinFiles(snapshot));
    return rows.build();
  }

  private List<PuffinFileRow> deletionVectorPuffinFiles(Snapshot snapshot) {
    Map<String, DeletionVectorFileAccumulator> dvFiles = Maps.newLinkedHashMap();
    long snapshotId = snapshot.snapshotId();

    for (ManifestFile deleteManifest : snapshot.deleteManifests(table().io())) {
      try (ManifestReader<DeleteFile> reader =
              ManifestFiles.readDeleteManifest(deleteManifest, table().io(), table().specs())
                  .select(DV_SCAN_COLUMNS);
          CloseableIterable<ManifestEntry<DeleteFile>> entries = reader.liveEntries()) {

        for (ManifestEntry<DeleteFile> entry : entries) {
          DeleteFile deleteFile = entry.file();
          if (!ContentFileUtil.isDV(deleteFile)) {
            continue;
          }

          String path = deleteFile.location();
          Long contentOffset = deleteFile.contentOffset();
          Long contentSizeInBytes = deleteFile.contentSizeInBytes();

          Preconditions.checkState(
              contentOffset != null,
              "Missing content offset for deletion vector in Puffin file: %s",
              path);
          Preconditions.checkState(
              contentSizeInBytes != null,
              "Missing content size for deletion vector in Puffin file: %s",
              path);

          dvFiles
              .computeIfAbsent(
                  path,
                  ignored ->
                      new DeletionVectorFileAccumulator(
                          snapshotId, path, deleteFile.fileSizeInBytes()))
              .add(deleteFile.fileSizeInBytes(), contentOffset, contentSizeInBytes);
        }

      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to read delete manifest: %s", deleteManifest.path());
      }
    }

    return dvFiles.values().stream()
        .map(DeletionVectorFileAccumulator::toPuffinFileRow)
        .collect(ImmutableList.toImmutableList());
  }

  private class PuffinFilesTableScan extends StaticTableScan {

    PuffinFilesTableScan(Table table) {
      super(
          table, PUFFIN_FILES_SCHEMA, MetadataTableType.PUFFIN_FILES, PuffinFilesTable.this::task);
    }

    PuffinFilesTableScan(Table table, TableScanContext context) {
      super(
          table,
          PUFFIN_FILES_SCHEMA,
          MetadataTableType.PUFFIN_FILES,
          PuffinFilesTable.this::task,
          context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new PuffinFilesTableScan(table, context);
    }
  }

  private static StaticDataTask.Row puffinFileToRow(Schema tableSchema, PuffinFileRow puffinFile) {
    ImmutableList.Builder<StaticDataTask.Row> referencedFields =
        ImmutableList.builderWithExpectedSize(puffinFile.referencedFieldIds().size());

    for (Integer fieldId : puffinFile.referencedFieldIds()) {
      referencedFields.add(StaticDataTask.Row.of(fieldId, currentFieldName(tableSchema, fieldId)));
    }

    return StaticDataTask.Row.of(
        puffinFile.snapshotId(),
        puffinFile.path(),
        puffinFile.source(),
        puffinFile.fileSizeInBytes(),
        puffinFile.referencedBlobCount(),
        puffinFile.referencedBlobTypes(),
        referencedFields.build());
  }

  private static String currentFieldName(Schema tableSchema, int fieldId) {
    if (fieldId == MetadataColumns.ROW_POSITION.fieldId()) {
      return MetadataColumns.ROW_POSITION.name();
    }

    // Field IDs are authoritative. Names are resolved against the current table schema and may be
    // null when a field has been dropped.
    return tableSchema.findColumnName(fieldId);
  }

  private static class PuffinFileRow {
    private final long snapshotId;
    private final String path;
    private final String source;
    private final long fileSizeInBytes;
    private final int referencedBlobCount;
    private final List<String> referencedBlobTypes;
    private final List<Integer> referencedFieldIds;

    private PuffinFileRow(
        long snapshotId,
        String path,
        String source,
        long fileSizeInBytes,
        int referencedBlobCount,
        List<String> referencedBlobTypes,
        List<Integer> referencedFieldIds) {
      this.snapshotId = snapshotId;
      this.path = path;
      this.source = source;
      this.fileSizeInBytes = fileSizeInBytes;
      this.referencedBlobCount = referencedBlobCount;
      this.referencedBlobTypes = referencedBlobTypes;
      this.referencedFieldIds = referencedFieldIds;
    }

    static PuffinFileRow fromStatisticsFile(long snapshotId, StatisticsFile statisticsFile) {
      List<BlobMetadata> blobs = statisticsFile.blobMetadata();

      List<String> referencedBlobTypes =
          blobs.stream()
              .map(BlobMetadata::type)
              .distinct()
              .collect(ImmutableList.toImmutableList());

      List<Integer> referencedFieldIds =
          blobs.stream()
              .flatMap(blob -> blob.fields().stream())
              .distinct()
              .collect(ImmutableList.toImmutableList());

      return new PuffinFileRow(
          snapshotId,
          statisticsFile.path(),
          SOURCE_STATISTICS,
          statisticsFile.fileSizeInBytes(),
          blobs.size(),
          referencedBlobTypes,
          referencedFieldIds);
    }

    static PuffinFileRow fromDeletionVectorFile(
        long snapshotId, String path, long fileSizeInBytes, int referencedBlobCount) {
      return new PuffinFileRow(
          snapshotId,
          path,
          SOURCE_DELETION_VECTOR,
          fileSizeInBytes,
          referencedBlobCount,
          DV_REFERENCED_BLOB_TYPES,
          DV_REFERENCED_FIELD_IDS);
    }

    long snapshotId() {
      return snapshotId;
    }

    String path() {
      return path;
    }

    String source() {
      return source;
    }

    long fileSizeInBytes() {
      return fileSizeInBytes;
    }

    int referencedBlobCount() {
      return referencedBlobCount;
    }

    List<String> referencedBlobTypes() {
      return referencedBlobTypes;
    }

    List<Integer> referencedFieldIds() {
      return referencedFieldIds;
    }
  }

  private static class DeletionVectorFileAccumulator {
    private final long snapshotId;
    private final String path;
    private final long fileSizeInBytes;
    private final Set<DeletionVectorBlobKey> referencedBlobs = Sets.newLinkedHashSet();

    DeletionVectorFileAccumulator(long snapshotId, String path, long fileSizeInBytes) {
      this.snapshotId = snapshotId;
      this.path = path;
      this.fileSizeInBytes = fileSizeInBytes;
    }

    void add(long entryFileSizeInBytes, long contentOffset, long contentSizeInBytes) {
      Preconditions.checkState(
          fileSizeInBytes == entryFileSizeInBytes,
          "Deletion vectors in the same Puffin file have different file sizes: "
              + "path=%s, snapshotId=%s, expected=%s, actual=%s",
          path,
          snapshotId,
          fileSizeInBytes,
          entryFileSizeInBytes);

      referencedBlobs.add(new DeletionVectorBlobKey(contentOffset, contentSizeInBytes));
    }

    PuffinFileRow toPuffinFileRow() {
      return PuffinFileRow.fromDeletionVectorFile(
          snapshotId, path, fileSizeInBytes, referencedBlobs.size());
    }
  }

  private record DeletionVectorBlobKey(long contentOffset, long contentSizeInBytes) {}
}
