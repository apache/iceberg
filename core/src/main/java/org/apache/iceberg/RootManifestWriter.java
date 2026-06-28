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
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

/**
 * Writes v4 root manifest files (the v4 replacement for the manifest list).
 *
 * <p>Each {@link ManifestFile} is emitted as a {@code content_entry} row with {@code
 * content_type=DATA_MANIFEST} (3) or {@code content_type=DELETE_MANIFEST} (4). The {@code
 * manifest_info} nested struct is populated from the {@link ManifestFile} counts.
 *
 * <p>Direct data-file entries ({@code content_type=DATA}) for the small-write optimization are a
 * future extension and are not emitted by this writer.
 *
 * <p>The partition column on the root manifest content_entry schema is the union of every live
 * partition spec (per {@link Partitioning#partitionType(Schema, Collection)}), matching the leaf
 * manifest partition schema. Manifest-reference rows leave this column null; direct data-file
 * entries (small-write optimization) populate it with the file's partition projected into the union
 * schema.
 */
class RootManifestWriter implements AutoCloseable {
  /**
   * Content stats type for the root manifest. Root manifest entries do not carry column-level
   * stats, so a placeholder struct with a single dummy optional boolean field is used. Parquet
   * cannot encode an empty struct, so this placeholder is always written as null and ignored on
   * read.
   */
  static final Types.StructType ROOT_CONTENT_STATS_TYPE =
      Types.StructType.of(Types.NestedField.optional(99998, "_no_stats", Types.BooleanType.get()));

  /**
   * Returns the partition type used on disk: the input if it has fields, or the leaf-writer's
   * empty-struct placeholder so Parquet can encode the column. Reusing the same placeholder as
   * {@code ManifestWriter.V4Writer} keeps the on-disk schema shape consistent across root and leaf
   * manifests for unpartitioned tables.
   */
  static Types.StructType emptyPartitionPlaceholderIfNeeded(Types.StructType partitionType) {
    return ManifestWriter.V4Writer.emptyPartitionPlaceholderIfNeeded(partitionType);
  }

  private final OutputFile outputFile;
  private final StandardEncryptionManager standardEncryptionManager;
  private final NativeEncryptionKeyMetadata keyMetadata;
  private final FileAppender<StructLike> appender;
  private final long commitSnapshotId;
  private final long commitSequenceNumber;
  // Per-data-manifest first-row-id counter. Initialized from the snapshot's nextRowId at
  // construction
  // and advanced by (existingRowsCount + addedRowsCount) every time a DATA manifest reference
  // without a prior first-row-id is added. Mirrors ManifestListWriter.V3Writer's counter logic.
  private Long nextRowId;
  private boolean closed = false;

  RootManifestWriter(
      OutputFile file,
      EncryptionManager encryptionManager,
      long snapshotId,
      Long parentSnapshotId,
      long sequenceNumber,
      Long snapshotFirstRowId,
      Types.StructType partitionType) {
    if (encryptionManager instanceof StandardEncryptionManager) {
      // ability to encrypt the manifest list key is introduced for standard encryption.
      this.standardEncryptionManager = (StandardEncryptionManager) encryptionManager;
      EncryptedOutputFile encryptedFile = this.standardEncryptionManager.encrypt(file);
      // For Parquet with native encryption, use the NativeEncryptionOutputFile directly so the
      // writer can apply column-level encryption. For all other cases use the encrypting wrapper.
      if (encryptedFile instanceof NativeEncryptionOutputFile) {
        this.outputFile = (NativeEncryptionOutputFile) encryptedFile;
      } else {
        this.outputFile = encryptedFile.encryptingOutputFile();
      }
      this.keyMetadata =
          encryptedFile.keyMetadata() instanceof NativeEncryptionKeyMetadata
              ? (NativeEncryptionKeyMetadata) encryptedFile.keyMetadata()
              : null;
    } else {
      this.standardEncryptionManager = null;
      this.outputFile = file;
      this.keyMetadata = null;
    }

    this.appender =
        newAppender(
            this.outputFile,
            snapshotId,
            parentSnapshotId,
            sequenceNumber,
            emptyPartitionPlaceholderIfNeeded(partitionType));
    this.commitSnapshotId = snapshotId;
    this.commitSequenceNumber = sequenceNumber;
    this.nextRowId = snapshotFirstRowId;
  }

  private static FileAppender<StructLike> newAppender(
      OutputFile file,
      long snapshotId,
      Long parentSnapshotId,
      long sequenceNumber,
      Types.StructType partitionType) {
    Schema contentEntrySchema =
        new Schema(
            TrackedFile.schemaWithContentStats(partitionType, ROOT_CONTENT_STATS_TYPE).fields());
    try {
      return InternalData.write(FileFormat.PARQUET, file)
          .schema(contentEntrySchema)
          .named("content_entry")
          .meta(
              ImmutableMap.of(
                  "snapshot-id", String.valueOf(snapshotId),
                  "parent-snapshot-id", String.valueOf(parentSnapshotId),
                  "sequence-number", String.valueOf(sequenceNumber),
                  "format-version", "4",
                  "content", "root-manifest"))
          // Force ParquetWriter to materialize an empty file when no manifest entries were added,
          // so v4 snapshots referencing an empty manifest set remain readable. Matches v3's
          // empty-Avro-manifest-list behavior. Property is the string form of
          // ParquetWriter.MATERIALIZE_EMPTY_FILE; core has no compile-time dep on iceberg-parquet.
          .set("iceberg.parquet.materialize-empty-file", "true")
          .overwrite()
          .build();
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to create root manifest writer for path: %s", file.location());
    }
  }

  /**
   * Adds a manifest reference entry. The output's {@code format_version} is read from {@link
   * ManifestFile#formatVersion()}: producers of v4 leaf manifests (e.g., {@code V4Writer}) set it
   * to {@code 4}; legacy v1-v3 manifests carried over during a v3-to-v4 upgrade default to {@code
   * 0}.
   */
  void add(ManifestFile manifest) {
    addEntry(manifest, EntryStatus.ADDED);
  }

  /**
   * Adds a manifest reference entry with an explicit entry status. Use {@link EntryStatus#EXISTING}
   * for manifests carried over unchanged from the previous snapshot, and {@link EntryStatus#ADDED}
   * for manifests newly written in this snapshot. The output's {@code format_version} is read from
   * {@link ManifestFile#formatVersion()}.
   */
  void add(ManifestFile manifest, EntryStatus status) {
    addEntry(manifest, status);
  }

  private void addEntry(ManifestFile manifest, EntryStatus status) {
    ManifestFile resolved = assignSequenceNumber(manifest);
    Long firstRowId = resolveFirstRowId(resolved);
    TrackedFile entry =
        ContentEntryAdapters.fromManifestFile(
            resolved, resolved.formatVersion(), status, firstRowId);
    appender.add((StructLike) entry);
  }

  /**
   * Resolves {@code UNASSIGNED_SEQ} on a freshly written leaf manifest so the root manifest entry
   * sees concrete sequence numbers. Mirrors {@code V3Metadata.ManifestFileWrapper}: when {@link
   * ManifestFile#sequenceNumber()} or {@link ManifestFile#minSequenceNumber()} is {@link
   * ManifestWriter#UNASSIGNED_SEQ}, replace it with the commit sequence number. The manifest must
   * have been written by the current commit; this is enforced by checking that {@code
   * manifest.snapshotId()} matches the commit snapshot id.
   */
  private ManifestFile assignSequenceNumber(ManifestFile manifest) {
    long seq = manifest.sequenceNumber();
    long minSeq = manifest.minSequenceNumber();
    if (seq != ManifestWriter.UNASSIGNED_SEQ && minSeq != ManifestWriter.UNASSIGNED_SEQ) {
      return manifest;
    }

    Preconditions.checkState(
        manifest.snapshotId() != null && manifest.snapshotId() == commitSnapshotId,
        "Found unassigned sequence number for a manifest from snapshot: %s",
        manifest.snapshotId());

    long resolvedSeq = seq == ManifestWriter.UNASSIGNED_SEQ ? commitSequenceNumber : seq;
    long resolvedMinSeq = minSeq == ManifestWriter.UNASSIGNED_SEQ ? commitSequenceNumber : minSeq;
    return GenericManifestFile.copyOf(manifest)
        .withSequenceNumbers(resolvedSeq, resolvedMinSeq)
        .build();
  }

  /**
   * Resolves the first-row-id to write for {@code manifest}, mirroring {@code
   * ManifestListWriter.V3Writer.prepare}:
   *
   * <ul>
   *   <li>Non-DATA manifest (DELETE manifest) → null.
   *   <li>DATA manifest with {@code manifest.firstRowId() != null} → carry over the prior value;
   *       counter is not advanced.
   *   <li>DATA manifest with {@code manifest.firstRowId() == null} → assign the current counter
   *       value; advance counter by {@code existingRowsCount + addedRowsCount} (conservative
   *       spacing for pre-v3 manifests whose existing files lacked first-row-id assignments).
   * </ul>
   */
  private Long resolveFirstRowId(ManifestFile manifest) {
    if (manifest.content() != ManifestContent.DATA) {
      return null;
    }

    if (manifest.firstRowId() != null) {
      return manifest.firstRowId();
    }

    Preconditions.checkState(
        nextRowId != null,
        "Cannot assign first-row-id for DATA manifest without a snapshot first-row-id: %s",
        manifest.path());
    long assigned = nextRowId;
    long existingRows = manifest.existingRowsCount() != null ? manifest.existingRowsCount() : 0L;
    long addedRows = manifest.addedRowsCount() != null ? manifest.addedRowsCount() : 0L;
    this.nextRowId = assigned + existingRows + addedRows;
    return assigned;
  }

  /** Convenience method to add all manifests from an iterable (all assumed v4 leaf format). */
  void addAll(Iterable<ManifestFile> manifests) {
    for (ManifestFile manifest : manifests) {
      add(manifest);
    }
  }

  /**
   * Returns metadata about this root manifest file so callers can build a snapshot referring to it.
   */
  ManifestListFile toRootManifestFile() {
    if (keyMetadata != null && keyMetadata.encryptionKey() != null) {
      String keyId =
          standardEncryptionManager.addManifestListKeyMetadata(
              keyMetadata.copyWithLength(appender.length()));
      return new BaseManifestListFile(outputFile.location(), keyId);
    } else {
      return new BaseManifestListFile(outputFile.location(), null);
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      this.closed = true;
      appender.close();
    }
  }
}
