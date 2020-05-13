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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

abstract class ManifestListWriter implements FileAppender<ManifestFile> {
  private final FileAppender<ManifestFile> writer;

  private ManifestListWriter(OutputFile file, Map<String, String> meta) {
    this.writer = newAppender(file, meta);
  }

  protected abstract ManifestFile prepare(ManifestFile manifest);

  protected abstract FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta);

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

  static class V2Writer extends ManifestListWriter {
    private final V2Metadata.IndexedManifestFile wrapper;

    V2Writer(OutputFile snapshotFile, long snapshotId, Long parentSnapshotId, long sequenceNumber) {
      super(snapshotFile, ImmutableMap.of(
          "snapshot-id", String.valueOf(snapshotId),
          "parent-snapshot-id", String.valueOf(parentSnapshotId),
          "sequence-number", String.valueOf(sequenceNumber),
          "format-version", "2"));
      this.wrapper = new V2Metadata.IndexedManifestFile(snapshotId, sequenceNumber);
    }

    @Override
    protected ManifestFile prepare(ManifestFile manifest) {
      return wrapper.wrap(manifest);
    }

    @Override
    protected FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta) {
      try {
        return Avro.write(file)
            .schema(V2Metadata.MANIFEST_LIST_SCHEMA)
            .named("manifest_file")
            .meta(meta)
            .overwrite()
            .build();

      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create snapshot list writer for path: " + file);
      }
    }
  }

  static class V1Writer extends ManifestListWriter {
    private final V1Metadata.IndexedManifestFile wrapper = new V1Metadata.IndexedManifestFile();

    V1Writer(OutputFile snapshotFile, long snapshotId, Long parentSnapshotId) {
      super(snapshotFile, ImmutableMap.of(
          "snapshot-id", String.valueOf(snapshotId),
          "parent-snapshot-id", String.valueOf(parentSnapshotId),
          "format-version", "1"));
    }

    @Override
    protected ManifestFile prepare(ManifestFile manifest) {
      return wrapper.wrap(manifest);
    }

    @Override
    protected FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta) {
      try {
        return Avro.write(file)
            .schema(V1Metadata.MANIFEST_LIST_SCHEMA)
            .named("manifest_file")
            .meta(meta)
            .overwrite()
            .build();

      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create snapshot list writer for path: " + file);
      }
    }
  }
}
