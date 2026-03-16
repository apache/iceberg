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

import static org.apache.iceberg.TestBase.FILE_A;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSnapshotProducer {

  @Test
  public void testManifestFileGroupSize() {
    assertManifestWriterCount(
        4 /* worker pool size */,
        100 /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer if file count is small");

    assertManifestWriterCount(
        4 /* worker pool size */,
        SnapshotProducer.MIN_FILE_GROUP_SIZE /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer if file count matches min group size");

    assertManifestWriterCount(
        4 /* worker pool size */,
        SnapshotProducer.MIN_FILE_GROUP_SIZE + 1 /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer if file count is slightly above min group size");

    assertManifestWriterCount(
        4 /* worker pool size */,
        (int) (1.25 * SnapshotProducer.MIN_FILE_GROUP_SIZE) /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer when file count is < 1.5 * min group size");

    assertManifestWriterCount(
        4 /* worker pool size */,
        (int) (1.5 * SnapshotProducer.MIN_FILE_GROUP_SIZE) /* file count */,
        2 /* manifest writer count */,
        "Must use 2 writers when file count is >= 1.5 * min group size");

    assertManifestWriterCount(
        3 /* worker pool size */,
        100 * SnapshotProducer.MIN_FILE_GROUP_SIZE /* file count */,
        3 /* manifest writer count */,
        "Must limit parallelism to worker pool size when file count is large");

    assertManifestWriterCount(
        32 /* worker pool size */,
        5 * SnapshotProducer.MIN_FILE_GROUP_SIZE /* file count */,
        5 /* manifest writer count */,
        "Must limit parallelism to avoid tiny manifests");
  }

  private void assertManifestWriterCount(
      int workerPoolSize, int fileCount, int expectedManifestWriterCount, String errMsg) {
    int writerCount = SnapshotProducer.manifestWriterCount(workerPoolSize, fileCount);
    assertThat(writerCount).as(errMsg).isEqualTo(expectedManifestWriterCount);
  }

  @Test
  public void manifestNotCleanedUpWhenSnapshotNotLoadableAfterCommit(@TempDir File tableDir) {
    // Uses a custom TableOps that returns stale metadata (without the new snapshot) on the
    // first refresh() after commit, simulating eventual consistency. Verifies that commit succeeds
    // and that the committed data is visible once the table is refreshed again
    String tableName = "stale-table-on-first-refresh";
    TestTables.TestTableOperations ops = opsWithStaleRefreshAfterCommit(tableName, tableDir);
    TestTables.TestTable tableWithStaleRefresh =
        TestTables.create(tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), 2, ops);

    // the first refresh() after the commit will return stale metadata (without this snapshot), so
    // SnapshotProducer will skip cleanup to avoid accidentally deleting files that are part of the
    // committed snapshot but commit still succeeds
    tableWithStaleRefresh.newAppend().appendFile(FILE_A).commit();

    // Refresh again to get the real metadata; the snapshot must be visible now
    tableWithStaleRefresh.ops().refresh();
    Snapshot snapshot = tableWithStaleRefresh.currentSnapshot();
    assertThat(snapshot)
        .as("Committed snapshot must be visible after refresh (eventual consistency resolved)")
        .isNotNull();

    File metadata = Paths.get(tableDir.getPath(), "metadata").toFile();
    assertThat(snapshot.allManifests(tableWithStaleRefresh.io()))
        .isNotEmpty()
        .allSatisfy(
            manifest -> assertThat(metadata.listFiles()).contains(new File(manifest.path())));
  }

  /**
   * Creates a TableOperations that returns stale metadata (without the newly committed snapshot) on
   * the first refresh() after a commit. This simulates eventual consistency where the committed
   * snapshot is not yet visible. Used to verify that when the snapshot cannot be loaded after
   * commit, cleanup is skipped to avoid accidentally deleting files that are part of the committed
   * snapshot.
   */
  private static TestTables.TestTableOperations opsWithStaleRefreshAfterCommit(
      String name, File location) {
    return new TestTables.TestTableOperations(name, location) {
      private TableMetadata metadataToReturnOnNextRefresh;

      @Override
      public void commit(TableMetadata base, TableMetadata updatedMetadata) {
        super.commit(base, updatedMetadata);
        if (base != null) {
          // return stale metadata on the first refresh() call
          this.metadataToReturnOnNextRefresh = base;
        }
      }

      @Override
      public TableMetadata refresh() {
        if (metadataToReturnOnNextRefresh != null) {
          this.current = metadataToReturnOnNextRefresh;
          this.metadataToReturnOnNextRefresh = null;
          return current;
        }

        return super.refresh();
      }
    };
  }
}
