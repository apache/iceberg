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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class ScanTestBase<
        ScanT extends Scan<ScanT, T, G>, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends TestBase {

  protected abstract ScanT newScan();

  @TestTemplate
  public void testTableScanHonorsSelect() {
    ScanT scan = newScan().select(Collections.singletonList("id"));

    Schema expectedSchema = new Schema(required(1, "id", Types.IntegerType.get()));

    assertThat(scan.schema().asStruct())
        .as("A tableScan.select() should prune the schema")
        .isEqualTo(expectedSchema.asStruct());
  }

  @TestTemplate
  public void testTableBothProjectAndSelect() {
    assertThatThrownBy(
            () -> newScan().select(Collections.singletonList("id")).project(SCHEMA.select("data")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set projection schema when columns are selected");
    assertThatThrownBy(
            () -> newScan().project(SCHEMA.select("data")).select(Collections.singletonList("id")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot select columns when projection schema is set");
  }

  @TestTemplate
  public void testTableScanHonorsSelectWithoutCaseSensitivity() {
    ScanT scan1 = newScan().caseSensitive(false).select(Collections.singletonList("ID"));
    // order of refinements shouldn't matter
    ScanT scan2 = newScan().select(Collections.singletonList("ID")).caseSensitive(false);

    Schema expectedSchema = new Schema(required(1, "id", Types.IntegerType.get()));

    assertThat(scan1.schema().asStruct())
        .as("A tableScan.select() should prune the schema without case sensitivity")
        .isEqualTo(expectedSchema.asStruct());

    assertThat(scan2.schema().asStruct())
        .as("A tableScan.select() should prune the schema regardless of scan refinement order")
        .isEqualTo(expectedSchema.asStruct());
  }

  @TestTemplate
  public void testTableScanHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    ScanT scan1 = newScan().filter(Expressions.equal("id", 5));

    try (CloseableIterable<G> groups = scan1.planTasks()) {
      assertThat(groups).as("Tasks should not be empty").isNotEmpty();
      for (G group : groups) {
        for (T task : group.tasks()) {
          Expression residual = ((ContentScanTask<?>) task).residual();
          assertThat(residual)
              .as("Residuals must be preserved")
              .isNotEqualTo(Expressions.alwaysTrue());
        }
      }
    }

    ScanT scan2 = newScan().filter(Expressions.equal("id", 5)).ignoreResiduals();

    try (CloseableIterable<G> groups = scan2.planTasks()) {
      assertThat(groups).as("Tasks should not be empty").isNotEmpty();
      for (G group : groups) {
        for (T task : group.tasks()) {
          Expression residual = ((ContentScanTask<?>) task).residual();
          assertThat(residual)
              .as("Residuals must be preserved")
              .isEqualTo(Expressions.alwaysTrue());
        }
      }
    }
  }

  @TestTemplate
  public void testTableScanWithPlanExecutor() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    AtomicInteger planThreadsIndex = new AtomicInteger(0);
    ScanT scan =
        newScan()
            .planWith(
                Executors.newFixedThreadPool(
                    1,
                    runnable -> {
                      Thread thread = new Thread(runnable);
                      thread.setName("plan-" + planThreadsIndex.getAndIncrement());
                      thread.setDaemon(
                          true); // daemon threads will be terminated abruptly when the JVM exits
                      return thread;
                    }));
    assertThat(scan.planFiles()).hasSize(2);
    assertThat(planThreadsIndex.get())
        .as("Thread should be created in provided pool")
        .isGreaterThan(0);
  }

  @TestTemplate
  public void testReAddingPartitionField() throws Exception {
    assumeThat(formatVersion).isEqualTo(2);
    Schema schema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(2, "b", Types.StringType.get()),
            required(3, "data", Types.IntegerType.get()));
    PartitionSpec initialSpec = PartitionSpec.builderFor(schema).identity("a").build();
    File dir = Files.createTempDirectory(temp, "junit").toFile();
    dir.delete();
    this.table = TestTables.create(dir, "test_part_evolution", schema, initialSpec, formatVersion);
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(initialSpec)
                .withPath("/path/to/data/a.parquet")
                .withFileSizeInBytes(10)
                .withPartitionPath("a=1")
                .withRecordCount(1)
                .build())
        .commit();

    table.updateSpec().addField("b").removeField("a").commit();
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath("/path/to/data/b.parquet")
                .withFileSizeInBytes(10)
                .withPartitionPath("b=1")
                .withRecordCount(1)
                .build())
        .commit();

    table.updateSpec().addField("a").commit();
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath("/path/to/data/ab.parquet")
                .withFileSizeInBytes(10)
                .withPartitionPath("b=1/a=1")
                .withRecordCount(1)
                .build())
        .commit();

    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath("/path/to/data/a2b.parquet")
                .withFileSizeInBytes(10)
                .withPartitionPath("b=1/a=2")
                .withRecordCount(1)
                .build())
        .commit();

    TableScan scan1 = table.newScan().filter(Expressions.equal("b", "1"));
    try (CloseableIterable<CombinedScanTask> tasks = scan1.planTasks()) {
      assertThat(tasks).as("There should be 1 combined task").hasSize(1);
      for (CombinedScanTask combinedScanTask : tasks) {
        assertThat(combinedScanTask.files()).as("All 4 files should match b=1 filter").hasSize(4);
      }
    }

    TableScan scan2 = table.newScan().filter(Expressions.equal("a", 2));
    try (CloseableIterable<CombinedScanTask> tasks = scan2.planTasks()) {
      assertThat(tasks).as("There should be 1 combined task").hasSize(1);
      for (CombinedScanTask combinedScanTask : tasks) {
        assertThat(combinedScanTask.files())
            .as("a=2 and file without a in spec should match")
            .hasSize(2);
      }
    }
  }

  @TestTemplate
  public void testDataFileSorted() throws Exception {
    Schema schema =
        new Schema(
            required(1, "a", Types.IntegerType.get()), required(2, "b", Types.StringType.get()));
    File dir = Files.createTempDirectory(temp, "junit").toFile();
    dir.delete();
    this.table =
        TestTables.create(
            dir, "test_data_file_sorted", schema, PartitionSpec.unpartitioned(), formatVersion);
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data/a.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withSortOrder(
                    SortOrder.builderFor(table.schema()).asc("a", NullOrder.NULLS_FIRST).build())
                .build())
        .commit();

    TableScan scan = table.newScan();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask fileScanTask : tasks) {
        assertThat(fileScanTask.file().sortOrderId()).isEqualTo(1);
      }
    }
  }
}
