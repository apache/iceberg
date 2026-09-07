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

import static org.apache.iceberg.Files.localInput;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSnapshotStreamParser {
  @TempDir private Path temp;

  @Test
  public void testStreamsAllSnapshotsInOrder() throws IOException {
    List<Snapshot> snapshots =
        Lists.newArrayList(snapshot(1, null), snapshot(2, 1L), snapshot(3, 2L));
    File metadata = writeMetadata(snapshots);

    try (CloseableIterable<Snapshot> stream = SnapshotParser.fromJson(localInput(metadata))) {
      List<Long> ids = Lists.newArrayList();
      for (Snapshot snapshot : stream) {
        ids.add(snapshot.snapshotId());
      }

      assertThat(ids).containsExactly(1L, 2L, 3L);
    }
  }

  @Test
  public void testStreamPreservesSnapshotFields() throws IOException {
    File metadata = writeMetadata(Lists.newArrayList(snapshot(5, 4L)));

    try (CloseableIterable<Snapshot> stream = SnapshotParser.fromJson(localInput(metadata))) {
      Snapshot snapshot = Lists.newArrayList(stream).get(0);

      assertThat(snapshot.snapshotId()).isEqualTo(5L);
      assertThat(snapshot.parentId()).isEqualTo(4L);
      assertThat(snapshot.timestampMillis()).isEqualTo(5000L);
      assertThat(snapshot.manifestListLocation()).isEqualTo("s3://bucket/snap-5.avro");
    }
  }

  @Test
  public void testStreamIsReiterable() throws IOException {
    File metadata = writeMetadata(Lists.newArrayList(snapshot(1, null), snapshot(2, 1L)));

    try (CloseableIterable<Snapshot> stream = SnapshotParser.fromJson(localInput(metadata))) {
      assertThat(collectIds(stream)).containsExactly(1L, 2L);
      assertThat(collectIds(stream)).containsExactly(1L, 2L);
    }
  }

  @Test
  public void testMetadataWithoutSnapshotsArray() throws IOException {
    File metadata = write("{\"format-version\":2,\"table-uuid\":\"uuid\"}");

    try (CloseableIterable<Snapshot> stream = SnapshotParser.fromJson(localInput(metadata))) {
      assertThat(stream).isEmpty();
    }
  }

  @Test
  public void testEmptySnapshotsArray() throws IOException {
    File metadata = writeMetadata(Lists.newArrayList());

    try (CloseableIterable<Snapshot> stream = SnapshotParser.fromJson(localInput(metadata))) {
      assertThat(stream).isEmpty();
    }
  }

  private List<Long> collectIds(CloseableIterable<Snapshot> stream) {
    List<Long> ids = Lists.newArrayList();
    for (Snapshot snapshot : stream) {
      ids.add(snapshot.snapshotId());
    }

    return ids;
  }

  private Snapshot snapshot(long id, Long parentId) {
    return new BaseSnapshot(
        0,
        id,
        parentId,
        id * 1000,
        null,
        null,
        1,
        "s3://bucket/snap-" + id + ".avro",
        null,
        null,
        null);
  }

  /** Writes a metadata document with fields both before and after the {@code snapshots} array. */
  private File writeMetadata(List<Snapshot> snapshots) throws IOException {
    StringBuilder json = new StringBuilder();
    json.append("{\"format-version\":2,\"table-uuid\":\"uuid\",\"properties\":{\"k\":\"v\"},");
    json.append("\"snapshots\":[");
    for (int i = 0; i < snapshots.size(); i += 1) {
      if (i > 0) {
        json.append(",");
      }

      json.append(SnapshotParser.toJson(snapshots.get(i)));
    }

    json.append("],\"current-snapshot-id\":-1}");
    return write(json.toString());
  }

  private File write(String json) throws IOException {
    File file = File.createTempFile("metadata", ".json", temp.toFile());
    Files.write(file.toPath(), json.getBytes(StandardCharsets.UTF_8));
    return file;
  }
}
