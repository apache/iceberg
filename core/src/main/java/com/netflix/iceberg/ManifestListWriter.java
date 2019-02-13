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

package com.netflix.iceberg;

import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.io.OutputFile;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

class ManifestListWriter implements FileAppender<ManifestFile> {
  private final FileAppender<ManifestFile> writer;

  ManifestListWriter(OutputFile snapshotFile, long snapshotId, Long parentSnapshotId) {
    this.writer = newAppender(snapshotFile, ImmutableMap.of(
        "snapshot-id", String.valueOf(snapshotId),
        "parent-snapshot-id", String.valueOf(parentSnapshotId)));
  }

  @Override
  public void add(ManifestFile file) {
    writer.add(file);
  }

  @Override
  public void addAll(Iterator<ManifestFile> values) {
    writer.addAll(values);
  }

  @Override
  public void addAll(Iterable<ManifestFile> values) {
    writer.addAll(values);
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

  private static FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta) {
    try {
      return Avro.write(file)
          .schema(ManifestFile.schema())
          .named("manifest_file")
          .meta(meta)
          .build();

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create snapshot list writer for path: " + file);
    }
  }
}
