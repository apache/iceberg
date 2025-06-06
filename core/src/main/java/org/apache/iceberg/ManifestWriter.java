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
import java.nio.ByteBuffer;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Writer for manifest files.
 *
 * @param <F> Java class of files written to the manifest, either {@link DataFile} or {@link
 *     DeleteFile}.
 */
public abstract class ManifestWriter<F extends ContentFile<F>> implements FileAppender<F> {
  // stand-in for the current sequence number that will be assigned when the commit is successful
  // this is replaced when writing a manifest list by the ManifestFile wrapper
  static final long UNASSIGNED_SEQ = -1L;

  private final OutputFile file;
  private final ByteBuffer keyMetadataBuffer;
  private final int specId;
  private final FileAppender<ManifestEntry<F>> writer;
  private final Long snapshotId;
  private final GenericManifestEntry<F> reused;
  private final PartitionSummary stats;
  private final Long firstRowId;

  private boolean closed = false;
  private int addedFiles = 0;
  private long addedRows = 0L;
  private int existingFiles = 0;
  private long existingRows = 0L;
  private int deletedFiles = 0;
  private long deletedRows = 0L;
  private Long minDataSequenceNumber = null;

  private ManifestWriter(
      PartitionSpec spec, EncryptedOutputFile file, Long snapshotId, Long firstRowId) {
    this.file = file.encryptingOutputFile();
    this.specId = spec.specId();
    this.writer = newAppender(spec, this.file);
    this.snapshotId = snapshotId;
    this.reused =
        new GenericManifestEntry<>(V1Metadata.entrySchema(spec.partitionType()).asStruct());
    this.stats = new PartitionSummary(spec);
    this.firstRowId = firstRowId;
    this.keyMetadataBuffer = (file.keyMetadata() == null) ? null : file.keyMetadata().buffer();
  }

  protected abstract ManifestEntry<F> prepare(ManifestEntry<F> entry);

  protected abstract FileAppender<ManifestEntry<F>> newAppender(
      PartitionSpec spec, OutputFile outputFile);

  protected ManifestContent content() {
    return ManifestContent.DATA;
  }

  void addEntry(ManifestEntry<F> entry) {
    switch (entry.status()) {
      case ADDED:
        addedFiles += 1;
        addedRows += entry.file().recordCount();
        break;
      case EXISTING:
        existingFiles += 1;
        existingRows += entry.file().recordCount();
        break;
      case DELETED:
        deletedFiles += 1;
        deletedRows += entry.file().recordCount();
        break;
    }

    stats.update(entry.file().partition());

    if (entry.isLive()
        && entry.dataSequenceNumber() != null
        && (minDataSequenceNumber == null || entry.dataSequenceNumber() < minDataSequenceNumber)) {
      this.minDataSequenceNumber = entry.dataSequenceNumber();
    }

    writer.add(prepare(entry));
  }

  /**
   * Add an added entry for a file.
   *
   * <p>The entry's snapshot ID will be this manifest's snapshot ID. The data and file sequence
   * numbers will be assigned at commit.
   *
   * @param addedFile a data file
   */
  @Override
  public void add(F addedFile) {
    addEntry(reused.wrapAppend(snapshotId, addedFile));
  }

  /**
   * Add an added entry for a file with a specific sequence number.
   *
   * <p>The entry's snapshot ID will be this manifest's snapshot ID. The entry's data sequence
   * number will be the provided data sequence number. The entry's file sequence number will be
   * assigned at commit.
   *
   * @param addedFile a data file
   * @param dataSequenceNumber a data sequence number for the file
   */
  public void add(F addedFile, long dataSequenceNumber) {
    addEntry(reused.wrapAppend(snapshotId, dataSequenceNumber, addedFile));
  }

  void add(ManifestEntry<F> entry) {
    if (entry.dataSequenceNumber() != null && entry.dataSequenceNumber() >= 0) {
      addEntry(reused.wrapAppend(snapshotId, entry.dataSequenceNumber(), entry.file()));
    } else {
      addEntry(reused.wrapAppend(snapshotId, entry.file()));
    }
  }

  /**
   * Add an existing entry for a file.
   *
   * <p>The original data and file sequence numbers, snapshot ID, which were assigned at commit,
   * must be preserved when adding an existing entry.
   *
   * @param existingFile a file
   * @param fileSnapshotId snapshot ID when the data file was added to the table
   * @param dataSequenceNumber a data sequence number of the file (assigned when the file was added)
   * @param fileSequenceNumber a file sequence number (assigned when the file was added)
   */
  public void existing(
      F existingFile, long fileSnapshotId, long dataSequenceNumber, Long fileSequenceNumber) {
    reused.wrapExisting(fileSnapshotId, dataSequenceNumber, fileSequenceNumber, existingFile);
    addEntry(reused);
  }

  void existing(ManifestEntry<F> entry) {
    addEntry(reused.wrapExisting(entry));
  }

  /**
   * Add a delete entry for a file.
   *
   * <p>The entry's snapshot ID will be this manifest's snapshot ID. However, the original data and
   * file sequence numbers of the file must be preserved when the file is marked as deleted.
   *
   * @param deletedFile a file
   * @param dataSequenceNumber a data sequence number of the file (assigned when the file was added)
   * @param fileSequenceNumber a file sequence number (assigned when the file was added)
   */
  public void delete(F deletedFile, long dataSequenceNumber, Long fileSequenceNumber) {
    addEntry(reused.wrapDelete(snapshotId, dataSequenceNumber, fileSequenceNumber, deletedFile));
  }

  void delete(ManifestEntry<F> entry) {
    // Use the current Snapshot ID for the delete. It is safe to delete the data file from disk
    // when this Snapshot has been removed or when there are no Snapshots older than this one.
    addEntry(reused.wrapDelete(snapshotId, entry));
  }

  @Override
  public Metrics metrics() {
    return writer.metrics();
  }

  @Override
  public long length() {
    return writer.length();
  }

  public ManifestFile toManifestFile() {
    Preconditions.checkState(closed, "Cannot build ManifestFile, writer is not closed");
    // if the minSequenceNumber is null, then no manifests with a sequence number have been written,
    // so the min data sequence number is the one that will be assigned when this is committed.
    // pass UNASSIGNED_SEQ to inherit it.
    long minSeqNumber = minDataSequenceNumber != null ? minDataSequenceNumber : UNASSIGNED_SEQ;
    return new GenericManifestFile(
        file.location(),
        writer.length(),
        specId,
        content(),
        UNASSIGNED_SEQ,
        minSeqNumber,
        snapshotId,
        stats.summaries(),
        keyMetadataBuffer,
        addedFiles,
        addedRows,
        existingFiles,
        existingRows,
        deletedFiles,
        deletedRows,
        firstRowId);
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    writer.close();
  }

  static class V4Writer extends ManifestWriter<DataFile> {
    private final V4Metadata.ManifestEntryWrapper<DataFile> entryWrapper;

    V4Writer(PartitionSpec spec, EncryptedOutputFile file, Long snapshotId, Long firstRowId) {
      super(spec, file, snapshotId, firstRowId);
      this.entryWrapper = new V4Metadata.ManifestEntryWrapper<>(snapshotId);
    }

    @Override
    protected ManifestEntry<DataFile> prepare(ManifestEntry<DataFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DataFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V4Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "4")
            .meta("content", "data")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }
  }

  static class V4DeleteWriter extends ManifestWriter<DeleteFile> {
    private final V4Metadata.ManifestEntryWrapper<DeleteFile> entryWrapper;

    V4DeleteWriter(PartitionSpec spec, EncryptedOutputFile file, Long snapshotId) {
      super(spec, file, snapshotId, null);
      this.entryWrapper = new V4Metadata.ManifestEntryWrapper<>(snapshotId);
    }

    @Override
    protected ManifestEntry<DeleteFile> prepare(ManifestEntry<DeleteFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DeleteFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V4Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "4")
            .meta("content", "deletes")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }

    @Override
    protected ManifestContent content() {
      return ManifestContent.DELETES;
    }
  }

  static class V3Writer extends ManifestWriter<DataFile> {
    private final V3Metadata.ManifestEntryWrapper<DataFile> entryWrapper;

    V3Writer(PartitionSpec spec, EncryptedOutputFile file, Long snapshotId, Long firstRowId) {
      super(spec, file, snapshotId, firstRowId);
      this.entryWrapper = new V3Metadata.ManifestEntryWrapper<>(snapshotId);
    }

    @Override
    protected ManifestEntry<DataFile> prepare(ManifestEntry<DataFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DataFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V3Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "3")
            .meta("content", "data")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }
  }

  static class V3DeleteWriter extends ManifestWriter<DeleteFile> {
    private final V3Metadata.ManifestEntryWrapper<DeleteFile> entryWrapper;

    V3DeleteWriter(PartitionSpec spec, EncryptedOutputFile file, Long snapshotId) {
      super(spec, file, snapshotId, null);
      this.entryWrapper = new V3Metadata.ManifestEntryWrapper<>(snapshotId);
    }

    @Override
    protected ManifestEntry<DeleteFile> prepare(ManifestEntry<DeleteFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DeleteFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V3Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "3")
            .meta("content", "deletes")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }

    @Override
    protected ManifestContent content() {
      return ManifestContent.DELETES;
    }
  }

  static class V2Writer extends ManifestWriter<DataFile> {
    private final V2Metadata.ManifestEntryWrapper<DataFile> entryWrapper;

    V2Writer(PartitionSpec spec, EncryptedOutputFile file, Long snapshotId) {
      super(spec, file, snapshotId, null);
      this.entryWrapper = new V2Metadata.ManifestEntryWrapper<>(snapshotId);
    }

    @Override
    protected ManifestEntry<DataFile> prepare(ManifestEntry<DataFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DataFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V2Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "2")
            .meta("content", "data")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }
  }

  static class V2DeleteWriter extends ManifestWriter<DeleteFile> {
    private final V2Metadata.ManifestEntryWrapper<DeleteFile> entryWrapper;

    V2DeleteWriter(PartitionSpec spec, EncryptedOutputFile file, Long snapshotId) {
      super(spec, file, snapshotId, null);
      this.entryWrapper = new V2Metadata.ManifestEntryWrapper<>(snapshotId);
    }

    @Override
    protected ManifestEntry<DeleteFile> prepare(ManifestEntry<DeleteFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DeleteFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V2Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "2")
            .meta("content", "deletes")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }

    @Override
    protected ManifestContent content() {
      return ManifestContent.DELETES;
    }
  }

  static class V1Writer extends ManifestWriter<DataFile> {
    private final V1Metadata.ManifestEntryWrapper entryWrapper;

    V1Writer(PartitionSpec spec, EncryptedOutputFile file, Long snapshotId) {
      super(spec, file, snapshotId, null);
      this.entryWrapper = new V1Metadata.ManifestEntryWrapper();
    }

    @Override
    protected ManifestEntry<DataFile> prepare(ManifestEntry<DataFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DataFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V1Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "1")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }
  }
}
