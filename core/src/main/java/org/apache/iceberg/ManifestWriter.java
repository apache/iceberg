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

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

/**
 * Writer for manifest files.
 */
public abstract class ManifestWriter implements FileAppender<DataFile> {
  // stand-in for the current sequence number that will be assigned when the commit is successful
  // this is replaced when writing a manifest list by the ManifestFile wrapper
  static final long UNASSIGNED_SEQ = -1L;

  /**
   * Create a new {@link ManifestWriter}.
   * <p>
   * Manifests created by this writer have all entry snapshot IDs set to null.
   * All entries will inherit the snapshot ID that will be assigned to the manifest on commit.
   *
   * @param spec {@link PartitionSpec} used to produce {@link DataFile} partition tuples
   * @param outputFile the destination file location
   * @return a manifest writer
   * @deprecated will be removed in 0.9.0; use {@link ManifestFiles#write(PartitionSpec, OutputFile)} instead.
   */
  @Deprecated
  public static ManifestWriter write(PartitionSpec spec, OutputFile outputFile) {
    return ManifestFiles.write(spec, outputFile);
  }

  private final OutputFile file;
  private final int specId;
  private final FileAppender<ManifestEntry> writer;
  private final Long snapshotId;
  private final GenericManifestEntry reused;
  private final PartitionSummary stats;

  private boolean closed = false;
  private int addedFiles = 0;
  private long addedRows = 0L;
  private int existingFiles = 0;
  private long existingRows = 0L;
  private int deletedFiles = 0;
  private long deletedRows = 0L;
  private Long minSequenceNumber = null;

  private ManifestWriter(PartitionSpec spec, OutputFile file, Long snapshotId) {
    this.file = file;
    this.specId = spec.specId();
    this.writer = newAppender(spec, file);
    this.snapshotId = snapshotId;
    this.reused = new GenericManifestEntry(spec.partitionType());
    this.stats = new PartitionSummary(spec);
  }

  protected abstract ManifestEntry prepare(ManifestEntry entry);

  protected abstract FileAppender<ManifestEntry> newAppender(PartitionSpec spec, OutputFile outputFile);

  void addEntry(ManifestEntry entry) {
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
    if (entry.sequenceNumber() != null && (minSequenceNumber == null || entry.sequenceNumber() < minSequenceNumber)) {
      this.minSequenceNumber = entry.sequenceNumber();
    }
    writer.add(prepare(entry));
  }

  /**
   * Add an added entry for a data file.
   * <p>
   * The entry's snapshot ID will be this manifest's snapshot ID.
   *
   * @param addedFile a data file
   */
  @Override
  public void add(DataFile addedFile) {
    addEntry(reused.wrapAppend(snapshotId, addedFile));
  }

  void add(ManifestEntry entry) {
    addEntry(reused.wrapAppend(snapshotId, entry.file()));
  }

  /**
   * Add an existing entry for a data file.
   *
   * @param existingFile a data file
   * @param fileSnapshotId snapshot ID when the data file was added to the table
   * @param sequenceNumber sequence number for the data file
   */
  public void existing(DataFile existingFile, long fileSnapshotId, long sequenceNumber) {
    addEntry(reused.wrapExisting(fileSnapshotId, sequenceNumber, existingFile));
  }

  void existing(ManifestEntry entry) {
    addEntry(reused.wrapExisting(entry.snapshotId(), entry.sequenceNumber(), entry.file()));
  }

  /**
   * Add a delete entry for a data file.
   * <p>
   * The entry's snapshot ID will be this manifest's snapshot ID.
   *
   * @param deletedFile a data file
   */
  public void delete(DataFile deletedFile) {
    addEntry(reused.wrapDelete(snapshotId, deletedFile));
  }

  void delete(ManifestEntry entry) {
    // Use the current Snapshot ID for the delete. It is safe to delete the data file from disk
    // when this Snapshot has been removed or when there are no Snapshots older than this one.
    addEntry(reused.wrapDelete(snapshotId, entry.file()));
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
    // if the minSequenceNumber is null, then no manifests with a sequence number have been written, so the min
    // sequence number is the one that will be assigned when this is committed. pass UNASSIGNED_SEQ to inherit it.
    long minSeqNumber = minSequenceNumber != null ? minSequenceNumber : UNASSIGNED_SEQ;
    return new GenericManifestFile(file.location(), writer.length(), specId, UNASSIGNED_SEQ, minSeqNumber, snapshotId,
        addedFiles, addedRows, existingFiles, existingRows, deletedFiles, deletedRows, stats.summaries());
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    writer.close();
  }

  static class V2Writer extends ManifestWriter {
    private V2Metadata.IndexedManifestEntry entryWrapper;

    V2Writer(PartitionSpec spec, OutputFile file, Long snapshotId) {
      super(spec, file, snapshotId);
      this.entryWrapper = new V2Metadata.IndexedManifestEntry(snapshotId, spec.partitionType());
    }

    @Override
    protected ManifestEntry prepare(ManifestEntry entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry> newAppender(PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V2Metadata.entrySchema(spec.partitionType());
      try {
        return Avro.write(file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "2")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create manifest writer for path: " + file);
      }
    }
  }

  static class V1Writer extends ManifestWriter {
    private V1Metadata.IndexedManifestEntry entryWrapper;

    V1Writer(PartitionSpec spec, OutputFile file, Long snapshotId) {
      super(spec, file, snapshotId);
      this.entryWrapper = new V1Metadata.IndexedManifestEntry(spec.partitionType());
    }

    @Override
    protected ManifestEntry prepare(ManifestEntry entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry> newAppender(PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V1Metadata.entrySchema(spec.partitionType());
      try {
        return Avro.write(file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "1")
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create manifest writer for path: " + file);
      }
    }
  }
}
