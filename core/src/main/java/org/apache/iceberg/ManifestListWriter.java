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
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

abstract class ManifestListWriter implements FileAppender<ManifestFile> {
  private final FileAppender<ManifestFile> writer;

  private ManifestListWriter(OutputFile file, Map<String, String> meta) {
    this.writer = newAppender(file, meta);
  }

  protected abstract ManifestFile prepare(ManifestFile manifest);

  protected abstract FileAppender<ManifestFile> newAppender(
      OutputFile file, Map<String, String> meta);

  @Override
  public void add(ManifestFile manifest) {
    writer.add(prepare(manifest));
  }

  @Override
  public void addAll(Iterator<ManifestFile> values) {
    values.forEachRemaining(this::add);
  }

  @Override
  public void addAll(Iterable<ManifestFile> values) {
    values.forEach(this::add);
  }

  @Override
  public Metrics metrics() {
    return writer.metrics();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public long length() {
    return writer.length();
  }

  public Long nextRowId() {
    return null;
  }

  static class V4Writer extends ManifestListWriter {
    private final V4Metadata.ManifestFileWrapper wrapper;
    private Long nextRowId;

    V4Writer(
        OutputFile snapshotFile,
        long snapshotId,
        Long parentSnapshotId,
        long sequenceNumber,
        long firstRowId) {
      super(
          snapshotFile,
          ImmutableMap.of(
              "snapshot-id", String.valueOf(snapshotId),
              "parent-snapshot-id", String.valueOf(parentSnapshotId),
              "sequence-number", String.valueOf(sequenceNumber),
              "first-row-id", String.valueOf(firstRowId),
              "format-version", "4"));
      this.wrapper = new V4Metadata.ManifestFileWrapper(snapshotId, sequenceNumber);
      this.nextRowId = firstRowId;
    }

    @Override
    protected ManifestFile prepare(ManifestFile manifest) {
      if (manifest.content() != ManifestContent.DATA || manifest.firstRowId() != null) {
        return wrapper.wrap(manifest, null);
      } else {
        // assign first-row-id and update the next to assign
        wrapper.wrap(manifest, nextRowId);
        // leave space for existing and added rows, in case any of the existing data files do not
        // have an assigned first-row-id (this is the case with manifests from pre-v3 snapshots)
        this.nextRowId += manifest.existingRowsCount() + manifest.addedRowsCount();
        return wrapper;
      }
    }

    @Override
    protected FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta) {
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(V4Metadata.MANIFEST_LIST_SCHEMA)
            .named("manifest_file")
            .meta(meta)
            .overwrite()
            .build();

      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create snapshot list writer for path: %s", file.location());
      }
    }

    @Override
    public Long nextRowId() {
      return nextRowId;
    }
  }

  static class V3Writer extends ManifestListWriter {
    private final V3Metadata.ManifestFileWrapper wrapper;
    private Long nextRowId;

    V3Writer(
        OutputFile snapshotFile,
        long snapshotId,
        Long parentSnapshotId,
        long sequenceNumber,
        long firstRowId) {
      super(
          snapshotFile,
          ImmutableMap.of(
              "snapshot-id", String.valueOf(snapshotId),
              "parent-snapshot-id", String.valueOf(parentSnapshotId),
              "sequence-number", String.valueOf(sequenceNumber),
              "first-row-id", String.valueOf(firstRowId),
              "format-version", "3"));
      this.wrapper = new V3Metadata.ManifestFileWrapper(snapshotId, sequenceNumber);
      this.nextRowId = firstRowId;
    }

    @Override
    protected ManifestFile prepare(ManifestFile manifest) {
      if (manifest.content() != ManifestContent.DATA || manifest.firstRowId() != null) {
        return wrapper.wrap(manifest, null);
      } else {
        // assign first-row-id and update the next to assign
        wrapper.wrap(manifest, nextRowId);
        // leave space for existing and added rows, in case any of the existing data files do not
        // have an assigned first-row-id (this is the case with manifests from pre-v3 snapshots)
        this.nextRowId += manifest.existingRowsCount() + manifest.addedRowsCount();
        return wrapper;
      }
    }

    @Override
    protected FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta) {
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(V3Metadata.MANIFEST_LIST_SCHEMA)
            .named("manifest_file")
            .meta(meta)
            .overwrite()
            .build();

      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create snapshot list writer for path: %s", file.location());
      }
    }

    @Override
    public Long nextRowId() {
      return nextRowId;
    }
  }

  static class V2Writer extends ManifestListWriter {
    private final V2Metadata.ManifestFileWrapper wrapper;

    V2Writer(OutputFile snapshotFile, long snapshotId, Long parentSnapshotId, long sequenceNumber) {
      super(
          snapshotFile,
          ImmutableMap.of(
              "snapshot-id", String.valueOf(snapshotId),
              "parent-snapshot-id", String.valueOf(parentSnapshotId),
              "sequence-number", String.valueOf(sequenceNumber),
              "format-version", "2"));
      this.wrapper = new V2Metadata.ManifestFileWrapper(snapshotId, sequenceNumber);
    }

    @Override
    protected ManifestFile prepare(ManifestFile manifest) {
      return wrapper.wrap(manifest);
    }

    @Override
    protected FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta) {
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(V2Metadata.MANIFEST_LIST_SCHEMA)
            .named("manifest_file")
            .meta(meta)
            .overwrite()
            .build();

      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create snapshot list writer for path: %s", file.location());
      }
    }
  }

  static class V1Writer extends ManifestListWriter {
    private final V1Metadata.ManifestFileWrapper wrapper = new V1Metadata.ManifestFileWrapper();

    V1Writer(OutputFile snapshotFile, long snapshotId, Long parentSnapshotId) {
      super(
          snapshotFile,
          ImmutableMap.of(
              "snapshot-id", String.valueOf(snapshotId),
              "parent-snapshot-id", String.valueOf(parentSnapshotId),
              "format-version", "1"));
    }

    @Override
    protected ManifestFile prepare(ManifestFile manifest) {
      Preconditions.checkArgument(
          manifest.content() == ManifestContent.DATA,
          "Cannot store delete manifests in a v1 table");
      return wrapper.wrap(manifest);
    }

    @Override
    protected FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta) {
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(V1Metadata.MANIFEST_LIST_SCHEMA)
            .named("manifest_file")
            .meta(meta)
            .overwrite()
            .build();

      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create snapshot list writer for path: %s", file.location());
      }
    }
  }
}
