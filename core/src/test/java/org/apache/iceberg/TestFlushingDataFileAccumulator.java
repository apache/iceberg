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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

class TestFlushingDataFileAccumulator {

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private final AtomicInteger flushCount = new AtomicInteger(0);
  private final Map<String, List<DataFile>> manifestToDatafiles = Maps.newHashMap();

  private FlushingDataFileAccumulator newAccumulator(int threshold) {
    return new FlushingDataFileAccumulator(threshold, this::stubWrite, this::stubRead);
  }

  private List<ManifestFile> stubWrite(Collection<DataFile> files, int specId) {
    flushCount.incrementAndGet();
    ManifestFile manifest = stubManifest();
    manifestToDatafiles.put(manifest.path(), ImmutableList.copyOf(files));
    return ImmutableList.of(manifest);
  }

  private CloseableIterable<DataFile> stubRead(ManifestFile manifest) {
    List<DataFile> files = manifestToDatafiles.getOrDefault(manifest.path(), ImmutableList.of());
    return CloseableIterable.withNoopClose(files);
  }

  private static ManifestFile stubManifest() {
    return new GenericManifestFile(
        "/path/to/manifest-" + UUID.randomUUID() + ".avro",
        1024,
        0,
        ManifestContent.DATA,
        0,
        0,
        null,
        null,
        null,
        0,
        0L,
        0,
        0L,
        0,
        0L,
        null);
  }

  private static DataFile newFile() {
    return DataFiles.builder(SPEC)
        .withPath("/path/to/data-" + UUID.randomUUID() + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  @Test
  void invalidThresholdZero() {
    assertThatThrownBy(() -> newAccumulator(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Flush threshold must be positive");
  }

  @Test
  void invalidThresholdNegative() {
    assertThatThrownBy(() -> newAccumulator(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Flush threshold must be positive");
  }

  @Test
  void belowThreshold() {
    FlushingDataFileAccumulator accumulator = newAccumulator(5);

    for (int i = 0; i < 4; i++) {
      accumulator.offer(newFile(), SPEC.specId());
    }

    assertThat(accumulator.hasPendingFiles()).isTrue();
    assertThat(accumulator.flushedManifests()).isEmpty();
    assertThat(flushCount.get()).isEqualTo(0);
  }

  @Test
  void exactThreshold() {
    FlushingDataFileAccumulator accumulator = newAccumulator(5);

    for (int i = 0; i < 5; i++) {
      accumulator.offer(newFile(), SPEC.specId());
    }

    assertThat(accumulator.hasPendingFiles()).isFalse();
    assertThat(accumulator.flushedManifests()).hasSize(1);
    assertThat(flushCount.get()).isEqualTo(1);
  }

  @Test
  void aboveThresholdWithRemainder() {
    FlushingDataFileAccumulator accumulator = newAccumulator(3);

    for (int i = 0; i < 7; i++) {
      accumulator.offer(newFile(), SPEC.specId());
    }

    // 2 flushes at 3 and 6, 1 file remaining
    assertThat(accumulator.hasPendingFiles()).isTrue();
    assertThat(accumulator.flushedManifests()).hasSize(2);
    assertThat(flushCount.get()).isEqualTo(2);
  }

  @Test
  void multipleExactBatches() {
    FlushingDataFileAccumulator accumulator = newAccumulator(3);

    for (int i = 0; i < 9; i++) {
      accumulator.offer(newFile(), SPEC.specId());
    }

    assertThat(accumulator.hasPendingFiles()).isFalse();
    assertThat(accumulator.flushedManifests()).hasSize(3);
    assertThat(flushCount.get()).isEqualTo(3);
  }

  @Test
  void specIdsTrackedAcrossFlushes() {
    FlushingDataFileAccumulator accumulator = newAccumulator(2);

    accumulator.offer(newFile(), 0);
    accumulator.offer(newFile(), 1);
    // flush happens here (2 files)
    accumulator.offer(newFile(), 2);

    assertThat(accumulator.specIds()).containsExactlyInAnyOrder(0, 1, 2);
  }

  @Test
  void hasFilesReflectsBothPendingAndFlushed() {
    FlushingDataFileAccumulator accumulator = newAccumulator(2);

    assertThat(accumulator.hasFiles()).isFalse();

    accumulator.offer(newFile(), SPEC.specId());
    assertThat(accumulator.hasFiles()).isTrue();
    assertThat(accumulator.hasPendingFiles()).isTrue();

    accumulator.offer(newFile(), SPEC.specId());
    // flushed — no pending, but hasFiles still true
    assertThat(accumulator.hasFiles()).isTrue();
    assertThat(accumulator.hasPendingFiles()).isFalse();
  }

  @Test
  void deleteUncommittedAlwaysClears() {
    FlushingDataFileAccumulator accumulator = newAccumulator(2);

    accumulator.offer(newFile(), SPEC.specId());
    accumulator.offer(newFile(), SPEC.specId());
    assertThat(accumulator.flushedManifests()).hasSize(1);

    List<String> deleted = Lists.newArrayList();
    accumulator.deleteUncommitted(Collections.emptySet(), deleted::add);
    assertThat(accumulator.flushedManifests()).isEmpty();
    assertThat(deleted).hasSize(1);
  }

  @Test
  void deleteUncommittedClearsEvenWhenAllCommitted() {
    FlushingDataFileAccumulator accumulator = newAccumulator(2);

    accumulator.offer(newFile(), SPEC.specId());
    accumulator.offer(newFile(), SPEC.specId());
    List<ManifestFile> flushed = ImmutableList.copyOf(accumulator.flushedManifests());
    assertThat(flushed).hasSize(1);

    List<String> deleted = Lists.newArrayList();
    accumulator.deleteUncommitted(Sets.newHashSet(flushed), deleted::add);
    assertThat(accumulator.flushedManifests()).isEmpty();
    assertThat(deleted).isEmpty();
  }

  @Test
  void thresholdMaxValueDisablesFlush() {
    FlushingDataFileAccumulator accumulator = newAccumulator(Integer.MAX_VALUE);

    for (int i = 0; i < 100; i++) {
      accumulator.offer(newFile(), SPEC.specId());
    }

    assertThat(accumulator.hasPendingFiles()).isTrue();
    assertThat(accumulator.flushedManifests()).isEmpty();
    assertThat(flushCount.get()).isEqualTo(0);
  }

  @Test
  void pendingBySpecGroupsFilesBySpecId() {
    FlushingDataFileAccumulator accumulator = newAccumulator(10);

    DataFile file0a = newFile();
    DataFile file0b = newFile();
    DataFile file1 = newFile();
    DataFile file2 = newFile();

    accumulator.offer(file0a, 0);
    accumulator.offer(file1, 1);
    accumulator.offer(file0b, 0);
    accumulator.offer(file2, 2);

    assertThat(accumulator.hasPendingFiles()).isTrue();
    assertThat(accumulator.flushedManifests()).isEmpty();
    assertThat(accumulator.pendingBySpec()).containsOnlyKeys(0, 1, 2);
    assertThat(accumulator.pendingBySpec().get(0)).containsExactlyInAnyOrder(file0a, file0b);
    assertThat(accumulator.pendingBySpec().get(1)).containsExactly(file1);
    assertThat(accumulator.pendingBySpec().get(2)).containsExactly(file2);
  }

  @Test
  void allAddedFilesReturnsPendingOnly() throws IOException {
    FlushingDataFileAccumulator accumulator = newAccumulator(10);
    List<DataFile> added = Lists.newArrayList();

    for (int i = 0; i < 3; i++) {
      DataFile file = newFile();
      added.add(file);
      accumulator.offer(file, SPEC.specId());
    }

    try (CloseableIterable<DataFile> result = accumulator.allAddedFiles()) {
      assertThat(result).containsExactlyInAnyOrderElementsOf(added);
    }
  }

  @Test
  void allAddedFilesReturnsFlushedAndPending() throws IOException {
    FlushingDataFileAccumulator accumulator = newAccumulator(3);
    List<DataFile> allFiles = Lists.newArrayList();

    for (int i = 0; i < 5; i++) {
      DataFile file = newFile();
      allFiles.add(file);
      accumulator.offer(file, SPEC.specId());
    }

    assertThat(accumulator.flushedManifests()).hasSize(1);
    assertThat(accumulator.hasPendingFiles()).isTrue();

    try (CloseableIterable<DataFile> result = accumulator.allAddedFiles()) {
      assertThat(result).containsExactlyInAnyOrderElementsOf(allFiles);
    }
  }

  @Test
  void allAddedFilesReturnsFlushedOnly() throws IOException {
    FlushingDataFileAccumulator accumulator = newAccumulator(3);
    List<DataFile> allFiles = Lists.newArrayList();

    for (int i = 0; i < 6; i++) {
      DataFile file = newFile();
      allFiles.add(file);
      accumulator.offer(file, SPEC.specId());
    }

    assertThat(accumulator.flushedManifests()).hasSize(2);
    assertThat(accumulator.hasPendingFiles()).isFalse();

    try (CloseableIterable<DataFile> result = accumulator.allAddedFiles()) {
      assertThat(result).containsExactlyInAnyOrderElementsOf(allFiles);
    }
  }
}
