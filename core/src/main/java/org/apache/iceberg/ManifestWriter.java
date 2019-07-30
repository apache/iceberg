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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer for manifest files.
 */
public class ManifestWriter implements FileAppender<DataFile> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestWriter.class);

  static ManifestFile copyAppendManifest(ManifestReader reader, OutputFile outputFile, long snapshotId,
                                         SnapshotSummary.Builder summaryBuilder) {
    ManifestWriter writer = new ManifestWriter(reader.spec(), outputFile, snapshotId);
    boolean threw = true;
    try {
      for (ManifestEntry entry : reader.entries()) {
        Preconditions.checkArgument(entry.status() == ManifestEntry.Status.ADDED,
            "Cannot append manifest: contains existing files");
        summaryBuilder.addedFile(reader.spec(), entry.file());
        writer.add(entry);
      }

      threw = false;

    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        if (!threw) {
          throw new RuntimeIOException(e, "Failed to close manifest: %s", outputFile);
        }
      }
    }

    return writer.toManifestFile();
  }

  /**
   * Create a new {@link ManifestWriter}.
   * <p>
   * Manifests created by this writer are not part of a snapshot and have all entry snapshot IDs
   * set to -1.
   *
   * @param spec {@link PartitionSpec} used to produce {@link DataFile} partition tuples
   * @param outputFile the destination file location
   * @return a manifest writer
   */
  public static ManifestWriter write(PartitionSpec spec, OutputFile outputFile) {
    return new ManifestWriter(spec, outputFile, -1);
  }

  private final OutputFile file;
  private final int specId;
  private final FileAppender<ManifestEntry> writer;
  private final long snapshotId;
  private final ManifestEntry reused;
  private final PartitionSummary stats;

  private boolean closed = false;
  private int addedFiles = 0;
  private int existingFiles = 0;
  private int deletedFiles = 0;

  ManifestWriter(PartitionSpec spec, OutputFile file, long snapshotId) {
    this.file = file;
    this.specId = spec.specId();
    this.writer = newAppender(FileFormat.AVRO, spec, file);
    this.snapshotId = snapshotId;
    this.reused = new ManifestEntry(spec.partitionType());
    this.stats = new PartitionSummary(spec);
  }

  void addEntry(ManifestEntry entry) {
    switch (entry.status()) {
      case ADDED:
        addedFiles += 1;
        break;
      case EXISTING:
        existingFiles += 1;
        break;
      case DELETED:
        deletedFiles += 1;
        break;
    }
    stats.update(entry.file().partition());
    writer.add(entry);
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
    // TODO: this assumes that file is a GenericDataFile that can be written directly to Avro
    // Eventually, this should check in case there are other DataFile implementations.
    addEntry(reused.wrapAppend(snapshotId, addedFile));
  }

  public void add(ManifestEntry entry) {
    addEntry(reused.wrapAppend(snapshotId, entry.file()));
  }

  /**
   * Add an existing entry for a data file.
   *
   * @param existingFile a data file
   * @param fileSnapshotId snapshot ID when the data file was added to the table
   */
  public void existing(DataFile existingFile, long fileSnapshotId) {
    addEntry(reused.wrapExisting(fileSnapshotId, existingFile));
  }

  void existing(ManifestEntry entry) {
    addEntry(reused.wrapExisting(entry.snapshotId(), entry.file()));
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
    return new GenericManifestFile(file.location(), writer.length(), specId, snapshotId,
        addedFiles, existingFiles, deletedFiles, stats.summaries());
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    writer.close();
  }

  private static <D> FileAppender<D> newAppender(FileFormat format, PartitionSpec spec,
                                                 OutputFile file) {
    Schema manifestSchema = ManifestEntry.getSchema(spec.partitionType());
    try {
      switch (format) {
        case AVRO:
          return Avro.write(file)
              .schema(manifestSchema)
              .named("manifest_entry")
              .meta("schema", SchemaParser.toJson(spec.schema()))
              .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
              .meta("partition-spec-id", String.valueOf(spec.specId()))
              .overwrite()
              .build();
        default:
          throw new IllegalArgumentException("Unsupported format: " + format);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create manifest writer for path: " + file);
    }
  }
}
