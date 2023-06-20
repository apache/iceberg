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
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class ScanTestBase<
        ScanT extends Scan<ScanT, T, G>, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public ScanTestBase(int formatVersion) {
    super(formatVersion);
  }

  protected abstract ScanT newScan();

  @Test
  public void testTableScanHonorsSelect() {
    ScanT scan = newScan().select(Arrays.asList("id"));

    Schema expectedSchema = new Schema(required(1, "id", Types.IntegerType.get()));

    assertEquals(
        "A tableScan.select() should prune the schema",
        expectedSchema.asStruct(),
        scan.schema().asStruct());
  }

  @Test
  public void testTableBothProjectAndSelect() {
    Assertions.assertThatThrownBy(
            () -> newScan().select(Arrays.asList("id")).project(SCHEMA.select("data")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set projection schema when columns are selected");
    Assertions.assertThatThrownBy(
            () -> newScan().project(SCHEMA.select("data")).select(Arrays.asList("id")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot select columns when projection schema is set");
  }

  @Test
  public void testTableScanHonorsSelectWithoutCaseSensitivity() {
    ScanT scan1 = newScan().caseSensitive(false).select(Arrays.asList("ID"));
    // order of refinements shouldn't matter
    ScanT scan2 = newScan().select(Arrays.asList("ID")).caseSensitive(false);

    Schema expectedSchema = new Schema(required(1, "id", Types.IntegerType.get()));

    assertEquals(
        "A tableScan.select() should prune the schema without case sensitivity",
        expectedSchema.asStruct(),
        scan1.schema().asStruct());

    assertEquals(
        "A tableScan.select() should prune the schema regardless of scan refinement order",
        expectedSchema.asStruct(),
        scan2.schema().asStruct());
  }

  @Test
  public void testTableScanHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    ScanT scan1 = newScan().filter(Expressions.equal("id", 5));

    try (CloseableIterable<G> groups = scan1.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(groups) > 0);
      for (G group : groups) {
        for (T task : group.tasks()) {
          Expression residual = ((ContentScanTask<?>) task).residual();
          Assert.assertNotEquals("Residuals must be preserved", Expressions.alwaysTrue(), residual);
        }
      }
    }

    ScanT scan2 = newScan().filter(Expressions.equal("id", 5)).ignoreResiduals();

    try (CloseableIterable<G> groups = scan2.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(groups) > 0);
      for (G group : groups) {
        for (T task : group.tasks()) {
          Expression residual = ((ContentScanTask<?>) task).residual();
          Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), residual);
        }
      }
    }
  }

  @Test
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
    Assert.assertEquals(2, Iterables.size(scan.planFiles()));
    Assert.assertTrue("Thread should be created in provided pool", planThreadsIndex.get() > 0);
  }

  @Test
  public void testReAddingPartitionField() throws Exception {
    Assume.assumeTrue(formatVersion == 2);
    Schema schema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(2, "b", Types.StringType.get()),
            required(3, "data", Types.IntegerType.get()));
    PartitionSpec initialSpec = PartitionSpec.builderFor(schema).identity("a").build();
    File dir = temp.newFolder();
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
      Assert.assertTrue("There should be 1 combined task", Iterables.size(tasks) == 1);
      for (CombinedScanTask combinedScanTask : tasks) {
        Assert.assertEquals(
            "All 4 files should match b=1 filter", 4, combinedScanTask.files().size());
      }
    }

    TableScan scan2 = table.newScan().filter(Expressions.equal("a", 2));
    try (CloseableIterable<CombinedScanTask> tasks = scan2.planTasks()) {
      Assert.assertTrue("There should be 1 combined task", Iterables.size(tasks) == 1);
      for (CombinedScanTask combinedScanTask : tasks) {
        Assert.assertEquals(
            "a=2 and file without a in spec should match", 2, combinedScanTask.files().size());
      }
    }
  }
}
