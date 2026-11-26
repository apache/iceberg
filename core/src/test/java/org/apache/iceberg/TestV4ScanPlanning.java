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

import static org.apache.iceberg.V4TestHelpers.dataFile;
import static org.apache.iceberg.V4TestHelpers.writeTrackedFiles;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestV4ScanPlanning {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final Types.StructType EMPTY_PARTITION = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_PARTITION);
  private static final int UNPARTITIONED_SPEC_ID = PartitionSpec.unpartitioned().specId();
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsWriteSchema(SCHEMA, MetricsConfig.from(ImmutableMap.of(), SCHEMA, null));
  private static final String FILE_A = "s3://bucket/db/table/data/file-a.parquet";
  private static final String FILE_B = "s3://bucket/db/table/data/file-b.parquet";

  @TempDir private File tableDir;

  @AfterEach
  void cleanup() {
    TestTables.clearTables();
  }

  @Test
  void plansRootManifestSnapshot() throws IOException {
    TestTables.TestTable table =
        TestTables.create(tableDir, "v4_root", SCHEMA, PartitionSpec.unpartitioned(), 4);

    commitRootManifestSnapshot(
        table,
        dataFile(FILE_A, UNPARTITIONED_SPEC_ID, EMPTY_PARTITION_DATA),
        dataFile(FILE_B, UNPARTITIONED_SPEC_ID, EMPTY_PARTITION_DATA));

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      assertThat(tasks)
          .extracting(task -> task.file().location())
          .as("a root manifest snapshot should plan its data files")
          .containsExactlyInAnyOrder(FILE_A, FILE_B);
    }
  }

  @Test
  void readsManifestListSnapshotAfterUpgradeToV4() throws IOException {
    TestTables.TestTable table =
        TestTables.create(tableDir, "v4_upgraded", SCHEMA, PartitionSpec.unpartitioned(), 3);
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(FILE_A)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(file).commit();

    TableMetadata upgraded =
        TableMetadata.buildFrom(table.ops().current()).upgradeFormatVersion(4).build();
    table.ops().commit(table.ops().current(), upgraded);
    table.refresh();

    assertThat(table.ops().current().formatVersion()).isEqualTo(4);
    assertThat(table.currentSnapshot().rootManifestLocation())
        .as("a snapshot retained across upgrade should not have a root manifest")
        .isNull();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      assertThat(tasks).extracting(task -> task.file().location()).containsExactly(FILE_A);
    }
  }

  private void commitRootManifestSnapshot(TestTables.TestTable table, TrackedFile... entries)
      throws IOException {
    String rootLocation =
        new File(tableDir, "metadata/root-" + System.nanoTime() + ".parquet").toURI().toString();
    OutputFile out = table.io().newOutputFile(rootLocation);
    writeTrackedFiles(
        out, FileFormat.PARQUET, EMPTY_PARTITION, STATS_TYPE, ImmutableList.copyOf(entries));

    long addedRows = entries.length;
    BaseSnapshot snapshot =
        new BaseSnapshot(
            1L,
            1L,
            null,
            System.currentTimeMillis(),
            "append",
            ImmutableMap.of("operation", "append"),
            table.schema().schemaId(),
            /* manifestList= */ null,
            rootLocation,
            0L,
            addedRows,
            null);

    TableMetadata updated =
        TableMetadata.buildFrom(table.ops().current())
            .setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
            .build();
    table.ops().commit(table.ops().current(), updated);
    table.refresh();
  }
}
