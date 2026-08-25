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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.source.TestTables.TestTable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Focused unit test for {@link FileScanTaskFilteringScan}'s own filtering logic, independent of
 * Spark's {@code Dataset} API. {@code Dataset#inputFiles()} turned out not to reflect this
 * decorator's effect for Iceberg's DataSourceV2 scans, and the {@code resultDataFiles} SQL metric
 * is recorded inside the wrapped scan's own {@code planFiles()} -- before this decorator's filter
 * ever runs -- so neither is a reliable signal for verifying this class at the Spark integration
 * level. This tests {@link FileScanTaskFilteringScan#planFiles()} directly against a real (if
 * minimal) Iceberg table instead.
 */
public class TestFileScanTaskFilteringScan {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;

  private TestTable table;
  private DataFile fileA;
  private DataFile fileB;
  private DataFile fileC;

  @BeforeEach
  public void createTable() {
    this.table = TestTables.create(tableDir, "test", SCHEMA, PartitionSpec.unpartitioned());
    this.fileA = dataFile("file-a.parquet");
    this.fileB = dataFile("file-b.parquet");
    this.fileC = dataFile("file-c.parquet");
    table.newAppend().appendFile(fileA).appendFile(fileB).appendFile(fileC).commit();
  }

  @AfterEach
  public void dropTable() {
    TestTables.clearTables();
  }

  private DataFile dataFile(String fileName) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(new File(tableDir, fileName).toString())
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  @Test
  public void planFilesReturnsOnlyAllowedPaths() {
    BatchScan scan = table.newBatchScan();
    FileScanTaskFilteringScan filtered =
        new FileScanTaskFilteringScan(scan, ImmutableSet.of(fileB.location()));

    try (CloseableIterable<ScanTask> tasks = filtered.planFiles()) {
      ScanTask onlyTask = Iterables.getOnlyElement(tasks);
      assertThat(onlyTask).isInstanceOf(FileScanTask.class);
      assertThat(((FileScanTask) onlyTask).file().location()).isEqualTo(fileB.location());
    }
  }

  @Test
  public void planFilesMatchesMultipleAllowedPaths() {
    BatchScan scan = table.newBatchScan();
    FileScanTaskFilteringScan filtered =
        new FileScanTaskFilteringScan(
            scan, ImmutableSet.of(fileA.location(), fileC.location()));

    try (CloseableIterable<ScanTask> tasks = filtered.planFiles()) {
      assertThat(tasks)
          .extracting(task -> ((FileScanTask) task).file().location())
          .containsExactlyInAnyOrder(fileA.location(), fileC.location());
    }
  }

  @Test
  public void planFilesFallsBackToUnfilteredSetWhenNoPathsMatch() {
    // Simulates a path-format mismatch between how the index recorded a path and how Iceberg
    // reports it here: none of the "allowed" paths correspond to any real candidate file, which
    // must never be interpreted as "prune everything to zero rows."
    BatchScan scan = table.newBatchScan();
    FileScanTaskFilteringScan filtered =
        new FileScanTaskFilteringScan(scan, ImmutableSet.of("/some/path/that/does-not-exist"));

    try (CloseableIterable<ScanTask> tasks = filtered.planFiles()) {
      assertThat(tasks)
          .extracting(task -> ((FileScanTask) task).file().location())
          .containsExactlyInAnyOrder(fileA.location(), fileB.location(), fileC.location());
    }
  }

  @Test
  public void otherMethodsDelegateUnchanged() {
    BatchScan scan = table.newBatchScan();
    FileScanTaskFilteringScan filtered =
        new FileScanTaskFilteringScan(scan, ImmutableSet.of(fileB.location()));

    assertThat(filtered.schema()).isEqualTo(scan.schema());
    assertThat(filtered.table()).isEqualTo(scan.table());
    assertThat(filtered.isCaseSensitive()).isEqualTo(scan.isCaseSensitive());
  }
}
