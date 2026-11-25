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

import java.io.File;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestMetadataTableUtils {
  private static final long ROOT_MANIFEST_SNAPSHOT_ID = 42L;

  @TempDir private File tableDir;

  @AfterEach
  void clearTables() {
    TestTables.clearTables();
  }

  @Test
  void rejectsMetadataTableForRootManifestSnapshot() {
    Table table = withRootManifestSnapshot(upgradedV4Table(), SnapshotRef.MAIN_BRANCH);

    assertThatThrownBy(
            () -> MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(
            "Metadata tables are not supported for tables with v4 root manifests");
  }

  @Test
  void rejectsNoncurrentRootManifestSnapshot() {
    Table table = withRootManifestSnapshot(upgradedV4Table(), "b1");

    assertThatThrownBy(
            () ->
                MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.ALL_FILES))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(
            "Metadata tables are not supported for tables with v4 root manifests");
  }

  @Test
  void allowsMetadataTableWithoutRootManifestSnapshot() {
    Table filesTable =
        MetadataTableUtils.createMetadataTableInstance(upgradedV4Table(), MetadataTableType.FILES);

    assertThat(filesTable).isInstanceOf(FilesTable.class);
  }

  @Test
  void allowsManifestIndependentTableForRootManifestSnapshot() {
    Table table = withRootManifestSnapshot(upgradedV4Table(), SnapshotRef.MAIN_BRANCH);

    Table snapshotsTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.SNAPSHOTS);

    assertThat(snapshotsTable).isInstanceOf(SnapshotsTable.class);
  }

  private Table upgradedV4Table() {
    Table table =
        TestTables.create(
            tableDir, "test", TestBase.SCHEMA, TestBase.SPEC, SortOrder.unsorted(), 3);
    table.newAppend().appendFile(TestBase.FILE_A).commit();

    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata current = ops.current();
    ops.commit(current, TableMetadata.buildFrom(current).upgradeFormatVersion(4).build());
    table.refresh();
    return table;
  }

  private static Table withRootManifestSnapshot(Table table, String branch) {
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata base = ops.current();
    Snapshot parent = base.currentSnapshot();
    Snapshot rootManifestSnapshot =
        new BaseSnapshot(
            base.lastSequenceNumber() + 1,
            ROOT_MANIFEST_SNAPSHOT_ID,
            parent.snapshotId(),
            parent.timestampMillis() + 1,
            DataOperations.APPEND,
            ImmutableMap.of(),
            base.currentSchemaId(),
            null,
            "file:/nonexistent/root-manifest.parquet",
            base.nextRowId(),
            0L,
            null);
    ops.commit(
        base,
        TableMetadata.buildFrom(base).setBranchSnapshot(rootManifestSnapshot, branch).build());
    table.refresh();
    return table;
  }
}
