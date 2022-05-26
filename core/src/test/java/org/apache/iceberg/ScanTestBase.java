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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public abstract class ScanTestBase<T extends Scan<T>> extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public ScanTestBase(int formatVersion) {
    super(formatVersion);
  }

  protected abstract T newScan();

  @Test
  public void testTableScanHonorsSelect() {
    T scan = newScan().select(Arrays.asList("id"));

    Schema expectedSchema = new Schema(required(1, "id", Types.IntegerType.get()));

    assertEquals("A tableScan.select() should prune the schema",
        expectedSchema.asStruct(),
        scan.schema().asStruct());
  }

  @Test
  public void testTableBothProjectAndSelect() {
    AssertHelpers.assertThrows("Cannot set projection schema when columns are selected",
        IllegalStateException.class, () -> newScan().select(Arrays.asList("id")).project(SCHEMA.select("data")));
    AssertHelpers.assertThrows("Cannot select columns when projection schema is set",
        IllegalStateException.class, () -> newScan().project(SCHEMA.select("data")).select(Arrays.asList("id")));
  }

  @Test
  public void testTableScanHonorsSelectWithoutCaseSensitivity() {
    T scan1 = newScan().caseSensitive(false).select(Arrays.asList("ID"));
    // order of refinements shouldn't matter
    T scan2 = newScan().select(Arrays.asList("ID")).caseSensitive(false);

    Schema expectedSchema = new Schema(required(1, "id", Types.IntegerType.get()));

    assertEquals("A tableScan.select() should prune the schema without case sensitivity",
        expectedSchema.asStruct(),
        scan1.schema().asStruct());

    assertEquals("A tableScan.select() should prune the schema regardless of scan refinement order",
        expectedSchema.asStruct(),
        scan2.schema().asStruct());
  }

  @Test
  public void testTableScanHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    T scan1 = newScan()
        .filter(Expressions.equal("id", 5));

    try (CloseableIterable<CombinedScanTask> tasks = scan1.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          Assert.assertNotEquals("Residuals must be preserved", Expressions.alwaysTrue(), fileScanTask.residual());
        }
      }
    }

    T scan2 = newScan()
        .filter(Expressions.equal("id", 5))
        .ignoreResiduals();

    try (CloseableIterable<CombinedScanTask> tasks = scan2.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), fileScanTask.residual());
        }
      }
    }
  }

  @Test
  public void testTableScanWithPlanExecutor() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    AtomicInteger planThreadsIndex = new AtomicInteger(0);
    T scan = newScan()
        .planWith(Executors.newFixedThreadPool(1, runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("plan-" + planThreadsIndex.getAndIncrement());
          thread.setDaemon(true); // daemon threads will be terminated abruptly when the JVM exits
          return thread;
        }));
    Assert.assertEquals(2, Iterables.size(scan.planFiles()));
    Assert.assertTrue("Thread should be created in provided pool", planThreadsIndex.get() > 0);
  }
}
