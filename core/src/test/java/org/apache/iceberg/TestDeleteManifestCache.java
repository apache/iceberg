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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestDeleteManifestCache
    extends ScanTestBase<
        IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>> {

  @Override
  protected IncrementalChangelogScan newScan() {
    return table.newIncrementalChangelogScan();
  }

  @AfterEach
  public void clearCache() {
    // Clear cache after each test to avoid interference
    DeleteManifestCache.instance().invalidateAll();
  }

  @TestTemplate
  public void testCacheReducesManifestReads() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    // Create initial snapshot with data
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Add delete files
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Clear cache stats
    DeleteManifestCache cache = DeleteManifestCache.instance();
    cache.invalidateAll();

    // First scan - cache miss, manifests will be parsed
    IncrementalChangelogScan scan1 =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());
    List<ChangelogScanTask> tasks1 = plan(scan1);

    assertThat(tasks1).as("Must have 1 task").hasSize(1);
    assertThat(cache.stats().hitCount()).as("First scan should have no cache hits").isEqualTo(0);
    long firstRequestCount = cache.stats().requestCount();
    assertThat(firstRequestCount).as("First scan should have cache requests").isGreaterThan(0);

    // Second scan - cache hit, manifests should be reused
    IncrementalChangelogScan scan2 =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());
    List<ChangelogScanTask> tasks2 = plan(scan2);

    assertThat(tasks2).as("Must have 1 task").hasSize(1);
    assertThat(cache.stats().hitCount()).as("Second scan should have cache hits").isGreaterThan(0);
    assertThat(cache.stats().hitRate())
        .as("Cache hit rate should be reasonable")
        .isGreaterThanOrEqualTo(0.4); // At least 40% hit rate demonstrates caching benefit
  }

  @TestTemplate
  public void testCacheHandlesMultipleSnapshots() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    // Create snapshots with deletes
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    table.newFastAppend().appendFile(FILE_C).commit();
    Snapshot snap3 = table.currentSnapshot();

    DeleteFile fileBDeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-b-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=1")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(fileBDeletes).commit();
    Snapshot snap4 = table.currentSnapshot();

    // Clear cache
    DeleteManifestCache cache = DeleteManifestCache.instance();
    cache.invalidateAll();

    // Scan entire range
    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap4.snapshotId());
    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 3 tasks").hasSize(3);

    // Verify cache was populated
    long missCount = cache.stats().missCount();
    assertThat(missCount).as("Cache should have misses for unique manifests").isGreaterThan(0);

    // Scan again - should hit cache
    cache.stats(); // Record stats before second scan
    long initialHits = cache.stats().hitCount();

    IncrementalChangelogScan scan2 =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap4.snapshotId());
    List<ChangelogScanTask> tasks2 = plan(scan2);

    assertThat(tasks2).as("Must have 3 tasks").hasSize(3);
    assertThat(cache.stats().hitCount())
        .as("Second scan should have more cache hits")
        .isGreaterThan(initialHits);
  }

  @TestTemplate
  public void testCacheCorrectnessWithDifferentManifests() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    // Create two separate scenarios with different delete files
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Scan first scenario
    IncrementalChangelogScan scan1 =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());
    List<ChangelogScanTask> tasks1 = plan(scan1);

    assertThat(tasks1).as("Must have 1 task").hasSize(1);
    DeletedRowsScanTask task1 = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks1);
    assertThat(task1.addedDeletes()).hasSize(1);
    assertThat(task1.addedDeletes().get(0).location()).isEqualTo(FILE_A_DELETES.location());

    // Add different delete file
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();
    Snapshot snap3 = table.currentSnapshot();

    // Scan second scenario
    IncrementalChangelogScan scan2 =
        newScan().fromSnapshotExclusive(snap2.snapshotId()).toSnapshot(snap3.snapshotId());
    List<ChangelogScanTask> tasks2 = plan(scan2);

    assertThat(tasks2).as("Must have 1 task").hasSize(1);
    DeletedRowsScanTask task2 = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks2);
    assertThat(task2.addedDeletes()).hasSize(1);
    assertThat(task2.addedDeletes().get(0).location()).isEqualTo(FILE_A2_DELETES.location());

    // Verify different delete files are returned even with caching
    assertThat(task1.addedDeletes().get(0).location())
        .isNotEqualTo(task2.addedDeletes().get(0).location());
  }

  private List<ChangelogScanTask> plan(IncrementalChangelogScan scan) {
    try (CloseableIterable<ChangelogScanTask> tasks = scan.planFiles()) {
      return Lists.newArrayList(tasks);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
