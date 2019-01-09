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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

import static org.apache.iceberg.ManifestEntry.Status.DELETED;

/**
 * Writer for manifest files.
 */
class ManifestWriter implements FileAppender<DataFile> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestWriter.class);

  private final String location;
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
    this.location = file.location();
    this.file = file;
    this.specId = spec.specId();
    this.writer = newAppender(FileFormat.AVRO, spec, file);
    this.snapshotId = snapshotId;
    this.reused = new ManifestEntry(spec.partitionType());
    this.stats = new PartitionSummary(spec);
  }

  public void addExisting(Iterable<ManifestEntry> entries) {
    for (ManifestEntry entry : entries) {
      if (entry.status() != DELETED) {
        addExisting(entry);
      }
    }
  }

  public void addExisting(ManifestEntry entry) {
    add(reused.wrapExisting(entry.snapshotId(), entry.file()));
  }

  public void addExisting(long snapshotId, DataFile file) {
    add(reused.wrapExisting(snapshotId, file));
  }

  public void delete(ManifestEntry entry) {
    // Use the current Snapshot ID for the delete. It is safe to delete the data file from disk
    // when this Snapshot has been removed or when there are no Snapshots older than this one.
    add(reused.wrapDelete(snapshotId, entry.file()));
  }

  public void delete(DataFile file) {
    add(reused.wrapDelete(snapshotId, file));
  }

  public void add(ManifestEntry entry) {
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

  public void addEntries(Iterable<ManifestEntry> entries) {
    for (ManifestEntry entry : entries) {
      add(entry);
    }
  }

  @Override
  public void add(DataFile file) {
    // TODO: this assumes that file is a GenericDataFile that can be written directly to Avro
    // Eventually, this should check in case there are other DataFile implementations.
    add(reused.wrapAppend(snapshotId, file));
  }

  @Override
  public Metrics metrics() {
    return writer.metrics();
  }

  public ManifestFile toManifestFile() {
    Preconditions.checkState(closed, "Cannot build ManifestFile, writer is not closed");
    return new GenericManifestFile(location, file.toInputFile().getLength(), specId, snapshotId,
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
              .build();
        default:
          throw new IllegalArgumentException("Unsupported format: " + format);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create manifest writer for path: " + file);
    }
  }
}
