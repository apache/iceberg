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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Scan-planning tests for v4 colocated deletion vectors.
 *
 * <p>Verifies that {@link Table#newScan()} resolves v4 colocated DVs into the {@link FileScanTask}
 * delete list, mirroring v3 scan-planning behavior despite the DV living inside a data manifest
 * (rather than a separate position-delete manifest).
 */
public class TestV4ScanPlanning {

  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(100)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(5)
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(100)
          .withPartitionPath("data_bucket=1")
          .withRecordCount(5)
          .build();

  @TempDir File tableDir;

  private TestTables.TestTable table;

  @BeforeEach
  public void before() {
    table = TestTables.create(tableDir, tableDir.getName(), SCHEMA, SPEC, SortOrder.unsorted(), 4);
  }

  /**
   * Scanning a v4 table where FILE_A has a colocated DV must:
   *
   * <ul>
   *   <li>Produce exactly one {@link FileScanTask} for FILE_A.
   *   <li>Attach exactly one {@link DeleteFile} (the DV's Puffin file) to that task.
   *   <li>Report the DV using {@link FileContent#POSITION_DELETES} and {@link FileFormat#PUFFIN}.
   * </ul>
   */
  @Test
  public void testScanResolvesColocatedDV() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();

    List<FileScanTask> tasks = planTasks(table);

    assertThat(tasks).as("must plan exactly one FileScanTask for FILE_A").hasSize(1);
    FileScanTask task = tasks.get(0);
    assertThat(task.file().location()).isEqualTo(FILE_A.location());

    List<DeleteFile> deletes = task.deletes();
    assertThat(deletes).as("FILE_A's task must carry the colocated DV").hasSize(1);

    DeleteFile attached = deletes.get(0);
    assertThat(attached.location()).isEqualTo(dv.location());
    assertThat(attached.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(attached.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(ContentFileUtil.isDV(attached)).isTrue();
    assertThat(attached.referencedDataFile()).isEqualTo(FILE_A.location());
  }

  /**
   * A born-with-DV file (data file and DV committed in the same {@code RowDelta}) must surface the
   * DV through scan planning just like an after-the-fact DV.
   */
  @Test
  public void testScanResolvesBornWithDV() throws IOException {
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addRows(FILE_A).addDeletes(dv).commit();

    List<FileScanTask> tasks = planTasks(table);

    assertThat(tasks).hasSize(1);
    FileScanTask task = tasks.get(0);
    assertThat(task.file().location()).isEqualTo(FILE_A.location());
    assertThat(task.deletes()).hasSize(1);
    assertThat(task.deletes().get(0).location()).isEqualTo(dv.location());
  }

  /**
   * Only the file with a colocated DV gets the DV attached; an unrelated data file in the same
   * manifest scans with no deletes.
   */
  @Test
  public void testScanAttachesDVOnlyToReferencedFile() throws IOException {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    DeleteFile dvForA = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dvForA).commit();

    List<FileScanTask> tasks = planTasks(table);

    assertThat(tasks).as("must plan one task per live data file").hasSize(2);

    FileScanTask taskA = findTaskFor(tasks, FILE_A);
    FileScanTask taskB = findTaskFor(tasks, FILE_B);

    assertThat(taskA.deletes()).as("FILE_A must carry its DV").hasSize(1);
    assertThat(taskA.deletes().get(0).location()).isEqualTo(dvForA.location());
    assertThat(taskB.deletes()).as("FILE_B has no DV").isEmpty();
  }

  /**
   * After replacing a DV (DV1 → DV2), scan planning must attach only the live DV (DV2). The prior
   * DV (DV1) must not appear in the scan task's delete list.
   */
  @Test
  public void testScanReflectsReplacedDV() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv1 = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv1).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Replace DV1 with DV2 — scope validation from snap2 so the v4 concurrent-DV detector does
    // not flag DV1 (already in the parent chain) as a conflict with DV2.
    DeleteFile dv2 = FileGenerationUtil.generateDV(table, FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv1)
        .addDeletes(dv2)
        .validateFromSnapshot(snap2.snapshotId())
        .commit();

    List<FileScanTask> tasks = planTasks(table);

    assertThat(tasks).hasSize(1);
    FileScanTask task = tasks.get(0);
    assertThat(task.deletes()).hasSize(1);
    assertThat(task.deletes().get(0).location())
        .as("scan must attach DV2, not DV1")
        .isEqualTo(dv2.location());
  }

  private static List<FileScanTask> planTasks(Table table) throws IOException {
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> iter = table.newScan().planFiles()) {
      for (FileScanTask task : iter) {
        tasks.add(task);
      }
    }
    return tasks;
  }

  private static FileScanTask findTaskFor(List<FileScanTask> tasks, DataFile file) {
    for (FileScanTask task : tasks) {
      if (task.file().location().equals(file.location())) {
        return task;
      }
    }
    throw new AssertionError("No FileScanTask for " + file.location());
  }
}
