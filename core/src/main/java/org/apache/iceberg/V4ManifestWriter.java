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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

abstract class V4ManifestWriter implements Closeable {
  private static final int FORMAT_VERSION = 4;
  static final long UNASSIGNED_SEQ = -1L;

  // TODO:
  // - Relativize paths

  static V4ManifestWriter createRoot(
      EncryptedOutputFile file,
      Map<Integer, PartitionSpec> specsById,
      Types.StructType contentStatsType,
      Map<String, String> writerProperties,
      int tableSchemaId,
      long snapshotId,
      long sequenceNumber,
      long firstRowId) {
    Types.StructType partitionType = Partitioning.unionPartitionTypes(specsById.values());
    return new V4ManifestWriter.RootWriter(
        file,
        specsById,
        partitionType,
        contentStatsType,
        writerProperties,
        tableSchemaId,
        snapshotId,
        sequenceNumber,
        firstRowId);
  }

  static V4ManifestWriter createLeaf(
      EncryptedOutputFile file,
      Map<Integer, PartitionSpec> specsById,
      Types.StructType contentStatsType,
      Map<String, String> writerProperties,
      int tableSchemaId,
      Long snapshotId) {
    Types.StructType partitionType = Partitioning.unionPartitionTypes(specsById.values());
    return new V4ManifestWriter.LeafWriter(
        file,
        specsById,
        partitionType,
        contentStatsType,
        writerProperties,
        tableSchemaId,
        snapshotId);
  }

  private final OutputFile output;
  private final EncryptionKeyMetadata keyMetadata;
  private final Schema manifestSchema;
  private final Long snapshotId;
  private final FileAppender<StructLike> writer;

  private boolean closed = false;
  private int recordCount = 0;
  private int addedFiles = 0;
  private int existingFiles = 0;
  private int deletedFiles = 0;
  private int replacedFiles = 0;
  private int modifiedFiles = 0;
  private long addedRows = 0L;
  private long existingRows = 0L;
  private long deletedRows = 0L;
  private long replacedRows = 0L;
  private long modifiedRows = 0L;
  private long minDataSequenceNumber = UNASSIGNED_SEQ;
  private long unassignedRows = 0L;

  private V4ManifestWriter(
      EncryptedOutputFile output,
      Schema manifestSchema,
      Map<String, String> fileMetadata,
      Map<String, String> writerProperties,
      Long snapshotId) {
    this.output = outputFile(output);
    this.manifestSchema = manifestSchema;
    this.keyMetadata = output.keyMetadata();
    this.snapshotId = snapshotId;
    this.writer = newAppender(this.output, fileMetadata, writerProperties);
  }

  protected abstract StructLike prepare(TrackedFile file, Tracking tracking);

  @VisibleForTesting
  long assignedRowIds() {
    return unassignedRows;
  }

  private FileAppender<StructLike> newAppender(
      OutputFile file, Map<String, String> fileMetadata, Map<String, String> writerProperties) {
    try {
      return InternalData.write(FileFormat.PARQUET, file)
          .schema(manifestSchema)
          .named("tracked_file")
          .meta(fileMetadata)
          .set(writerProperties)
          .overwrite()
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to create manifest writer for location: " + file.location(), e);
    }
  }

  private ManifestInfo summary() {
    return new ManifestInfoStruct(
        addedFiles,
        existingFiles,
        deletedFiles,
        replacedFiles,
        modifiedFiles,
        addedRows,
        existingRows,
        deletedRows,
        replacedRows,
        modifiedRows,
        minDataSequenceNumber,
        null, // no manifest DV
        FORMAT_VERSION);
  }

  public void append(TrackedFile file) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA || file.contentType() == FileContent.DATA_MANIFEST,
        "Cannot append %s file: %s",
        file.contentType(),
        file.location());

    TrackingBuilder added = TrackingBuilder.added(snapshotId);
    if (file.deletionVector() != null) {
      added.dvUpdated();
    }

    write(file, added.build());
  }

  public void appendAtSeq(TrackedFile file, long dataSequenceNumber) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA,
        "Cannot append %s at data sequence number %s: %s",
        file.contentType(),
        dataSequenceNumber,
        file.location());

    TrackingBuilder added = TrackingBuilder.added(snapshotId, dataSequenceNumber);
    if (file.deletionVector() != null) {
      added.dvUpdated();
    }

    write(file, added.build());
  }

  public void modify(TrackedFile file, DeletionVector dv) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA,
        "Cannot modify %s file with a data DV: %s",
        file.contentType(),
        file.location());

    TrackingBuilder modified = TrackingBuilder.from(file.tracking(), snapshotId).dvUpdated();
    TrackedFile withNewDV = Delegates.withDataDV(file, dv);
    write(withNewDV, modified.build());
  }

  public void existing(TrackedFile file) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA
            || file.contentType() == FileContent.DATA_MANIFEST
            || file.contentType() == FileContent.DELETE_MANIFEST,
        "Cannot add existing %s file: %s",
        file.contentType(),
        file.location());

    Tracking existing = TrackingBuilder.from(file.tracking(), snapshotId).build();
    write(file, existing);
  }

  public void replace(TrackedFile file) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA || file.contentType() == FileContent.DATA_MANIFEST,
        "Cannot replace %s file: %s",
        file.contentType(),
        file.location());

    Tracking replaced = TrackingBuilder.replaced(file.tracking(), snapshotId);
    write(file, replaced);
  }

  public void delete(TrackedFile file) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA
            || file.contentType() == FileContent.DATA_MANIFEST
            || file.contentType() == FileContent.DELETE_MANIFEST,
        "Cannot delete %s file: %s",
        file.contentType(),
        file.location());

    Tracking deleted = TrackingBuilder.deleted(file.tracking(), snapshotId);
    write(file, deleted);
  }

  private void write(TrackedFile file, Tracking tracking) {
    writer.add(prepare(file, tracking));

    this.recordCount += 1;
    updateManifestInfo(file, tracking);
  }

  public long length() {
    return writer.length();
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    writer.close();
  }

  private void updateManifestInfo(TrackedFile file, Tracking tracking) {
    if (hasLiveSequenceNumber(file, tracking)) {
      updateMinSequenceNumber(tracking.dataSequenceNumber());
    }

    if (tracking.firstRowId() == null) {
      this.unassignedRows += rowsToAssign(file);
    }

    switch (tracking.status()) {
      case ADDED -> {
        this.addedFiles += 1;
        this.addedRows += file.recordCount();
      }
      case EXISTING -> {
        this.existingFiles += 1;
        this.existingRows += file.recordCount();
      }
      case DELETED -> {
        this.deletedFiles += 1;
        this.deletedRows += file.recordCount();
      }
      case REPLACED -> {
        this.replacedFiles += 1;
        this.replacedRows += file.recordCount();
      }
      case MODIFIED -> {
        this.modifiedFiles += 1;
        this.modifiedRows += file.recordCount();
      }
    }
  }

  private boolean hasLiveSequenceNumber(TrackedFile file, Tracking tracking) {
    return file.contentType() == FileContent.DATA
        && tracking.isLive()
        && tracking.dataSequenceNumber() != null;
  }

  private void updateMinSequenceNumber(long sequenceNumber) {
    if (minDataSequenceNumber < 0 || sequenceNumber < minDataSequenceNumber) {
      this.minDataSequenceNumber = sequenceNumber;
    }
  }

  public TrackedFile toTrackedManifest() {
    Preconditions.checkState(closed, "Cannot build TrackedFile, writer is not closed");
    return new TrackedFileStruct(
        null, // no tracking metadata
        FileContent.DATA_MANIFEST,
        output.location(),
        FileFormat.PARQUET,
        recordCount,
        writer.length(),
        null, // TODO: track spec ID
        null, // manifests do not have a partition
        null, // TODO: aggregate content stats
        false, // stats are not tight for manifests
        null, // sort order is null for manifests
        null, // no deletion vector
        summary(),
        keyMetadata.buffer(),
        writer.splitOffsets(),
        null); // no column files
  }

  private static class LeafWriter extends V4ManifestWriter {
    private final V4Metadata.LeafTrackedFileWrapper wrapper;

    private LeafWriter(
        EncryptedOutputFile output,
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType,
        Map<String, String> writerProperties,
        int tableSchemaId,
        Long snapshotId) {
      super(
          output,
          V4Metadata.leafManifestSchema(partitionType, contentStatsType),
          Map.of(
              "format-version",
              String.valueOf(FORMAT_VERSION),
              "schema-id",
              String.valueOf(tableSchemaId),
              "snapshot-id",
              String.valueOf(snapshotId)),
          writerProperties,
          snapshotId);

      this.wrapper =
          new V4Metadata.LeafTrackedFileWrapper(
              snapshotId, specsById, partitionType, contentStatsType);
    }

    @Override
    protected StructLike prepare(TrackedFile file, Tracking tracking) {
      // TODO: update aggregate metrics in addition to ManifestInfo stats
      return wrapper.wrap(file, tracking);
    }
  }

  private static class RootWriter extends V4ManifestWriter {
    private final V4Metadata.RootTrackedFileWrapper wrapper;
    private final long firstRowId;
    private long nextRowId;

    private RootWriter(
        EncryptedOutputFile output,
        Map<Integer, PartitionSpec> specsById,
        Types.StructType partitionType,
        Types.StructType contentStatsType,
        Map<String, String> writerProperties,
        int tableSchemaId,
        long snapshotId,
        long sequenceNumber,
        long firstRowId) {
      super(
          output,
          V4Metadata.rootManifestSchema(partitionType, contentStatsType),
          Map.of(
              "format-version",
              String.valueOf(FORMAT_VERSION),
              "schema-id",
              String.valueOf(tableSchemaId),
              "snapshot-id",
              String.valueOf(snapshotId),
              "sequence-number",
              String.valueOf(sequenceNumber),
              "first-row-id",
              String.valueOf(firstRowId)),
          writerProperties,
          snapshotId);

      this.wrapper =
          new V4Metadata.RootTrackedFileWrapper(
              snapshotId, sequenceNumber, specsById, partitionType, contentStatsType);
      this.firstRowId = firstRowId;
      this.nextRowId = firstRowId;
    }

    @Override
    protected StructLike prepare(TrackedFile file, Tracking tracking) {
      if (hasFirstRowId(tracking) || isDeletes(file.contentType())) {
        // no first-row-id assignment is needed
        return wrapper.wrap(file, tracking, null);
      } else {
        // assign first-row-id and update the next ID
        wrapper.wrap(file, tracking, nextRowId);
        this.nextRowId += rowsToAssign(file);
        return wrapper;
      }
    }

    @Override
    public long assignedRowIds() {
      long assignedRowIds = nextRowId - firstRowId;
      // sanity check the assigned row IDs
      Preconditions.checkState(
          assignedRowIds == super.assignedRowIds(),
          "Incorrect assigned row IDs: %s != %s",
          assignedRowIds,
          super.assignedRowIds());
      return assignedRowIds;
    }

    private boolean hasFirstRowId(Tracking tracking) {
      return tracking.firstRowId() != null;
    }

    private boolean isDeletes(FileContent type) {
      return type != FileContent.DATA && type != FileContent.DATA_MANIFEST;
    }
  }

  private static long rowsToAssign(TrackedFile file) {
    return switch (file.contentType()) {
      case DATA -> file.recordCount();
      case DATA_MANIFEST -> {
        ManifestInfo summary = file.manifestInfo();
        yield summary.addedRowsCount() + summary.existingRowsCount() + summary.modifiedRowsCount();
      }
      case DELETE_MANIFEST -> 0L;
      default ->
          throw new IllegalArgumentException("Unsupported content type: " + file.contentType());
    };
  }

  private static OutputFile outputFile(EncryptedOutputFile encryptedFile) {
    // pass through native encryption, otherwise delegate to the file to handle encryption
    if (encryptedFile instanceof NativeEncryptionOutputFile nativeFile) {
      return nativeFile;
    }

    return encryptedFile.encryptingOutputFile();
  }
}
