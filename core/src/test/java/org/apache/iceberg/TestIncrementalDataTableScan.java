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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIncrementalDataTableScan extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @BeforeEach
  public void setupTableProperties() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "3").commit();
  }

  @TestTemplate
  public void testInvalidScans() {
    add(table.newAppend(), files("A"));
    assertThatThrownBy(() -> appendsBetweenScan(1, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("from and to snapshot ids cannot be the same");

    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C"));
    add(table.newAppend(), files("D"));
    add(table.newAppend(), files("E"));
    assertThatThrownBy(() -> table.newScan().appendsBetween(2, 5).appendsBetween(1, 4))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("from snapshot id 1 not in existing snapshot ids range (2, 4]");
    assertThatThrownBy(() -> table.newScan().appendsBetween(1, 2).appendsBetween(1, 3))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("to snapshot id 3 not in existing snapshot ids range (1, 2]");
  }

  @TestTemplate
  public void testAppends() {
    add(table.newAppend(), files("A")); // 1
    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C"));
    add(table.newAppend(), files("D"));
    add(table.newAppend(), files("E")); // 5

    class MyListener implements Listener<IncrementalScanEvent> {

      IncrementalScanEvent lastEvent = null;

      @Override
      public void notify(IncrementalScanEvent event) {
        this.lastEvent = event;
      }

      public IncrementalScanEvent event() {
        return lastEvent;
      }
    }

    MyListener listener1 = new MyListener();
    Listeners.register(listener1, IncrementalScanEvent.class);
    filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(1, 5));
    assertThat(listener1.event().fromSnapshotId()).isEqualTo(1);
    assertThat(listener1.event().toSnapshotId()).isEqualTo(5);
    filesMatch(Lists.newArrayList("C", "D", "E"), appendsBetweenScan(2, 5));
    assertThat(listener1.event().fromSnapshotId()).isEqualTo(2);
    assertThat(listener1.event().toSnapshotId()).isEqualTo(5);
    assertThat(listener1.event().projection()).isEqualTo(table.schema());
    assertThat(listener1.event().filter()).isEqualTo(Expressions.alwaysTrue());
    assertThat(listener1.event().tableName()).isEqualTo("test");
    assertThat(listener1.event().isFromSnapshotInclusive()).isFalse();
  }

  @TestTemplate
  public void testReplaceOverwritesDeletes() {
    add(table.newAppend(), files("A")); // 1
    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C"));
    add(table.newAppend(), files("D"));
    add(table.newAppend(), files("E")); // 5
    filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(1, 5));

    replace(table.newRewrite(), files("A", "B", "C"), files("F", "G")); // 6
    // Replace commits are ignored
    filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(1, 6));
    filesMatch(Lists.newArrayList("E"), appendsBetweenScan(4, 6));
    // 6th snapshot is a replace. No new content is added
    assertThat(appendsBetweenScan(5, 6)).as("Replace commits are ignored").isEmpty();
    delete(table.newDelete(), files("D")); // 7
    // 7th snapshot is a delete.
    assertThat(appendsBetweenScan(5, 7)).as("Replace and delete commits are ignored").isEmpty();
    assertThat(appendsBetweenScan(6, 7)).as("Delete commits are ignored").isEmpty();
    add(table.newAppend(), files("I")); // 8
    // snapshots 6 and 7 are ignored
    filesMatch(Lists.newArrayList("B", "C", "D", "E", "I"), appendsBetweenScan(1, 8));
    filesMatch(Lists.newArrayList("I"), appendsBetweenScan(6, 8));
    filesMatch(Lists.newArrayList("I"), appendsBetweenScan(7, 8));

    overwrite(table.newOverwrite(), files("H"), files("E")); // 9
    assertThatThrownBy(() -> appendsBetweenScan(8, 9))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Found overwrite operation, cannot support incremental data in snapshots (8, 9]");
  }

  @TestTemplate
  public void testTransactions() {
    Transaction transaction = table.newTransaction();

    add(transaction.newAppend(), files("A")); // 1
    add(transaction.newAppend(), files("B"));
    add(transaction.newAppend(), files("C"));
    add(transaction.newAppend(), files("D"));
    add(transaction.newAppend(), files("E")); // 5
    transaction.commitTransaction();
    filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(1, 5));

    transaction = table.newTransaction();
    replace(transaction.newRewrite(), files("A", "B", "C"), files("F", "G")); // 6
    transaction.commitTransaction();
    // Replace commits are ignored
    filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(1, 6));
    filesMatch(Lists.newArrayList("E"), appendsBetweenScan(4, 6));
    // 6th snapshot is a replace. No new content is added
    assertThat(appendsBetweenScan(5, 6)).as("Replace commits are ignored").isEmpty();

    transaction = table.newTransaction();
    delete(transaction.newDelete(), files("D")); // 7
    transaction.commitTransaction();
    // 7th snapshot is a delete.
    assertThat(appendsBetweenScan(5, 7)).as("Replace and delete commits are ignored").isEmpty();
    assertThat(appendsBetweenScan(6, 7)).as("Delete commits are ignored").isEmpty();

    transaction = table.newTransaction();
    add(transaction.newAppend(), files("I")); // 8
    transaction.commitTransaction();
    // snapshots 6, 7 and 8 are ignored
    filesMatch(Lists.newArrayList("B", "C", "D", "E", "I"), appendsBetweenScan(1, 8));
    filesMatch(Lists.newArrayList("I"), appendsBetweenScan(6, 8));
    filesMatch(Lists.newArrayList("I"), appendsBetweenScan(7, 8));
  }

  @TestTemplate
  public void testRollbacks() {
    add(table.newAppend(), files("A")); // 1
    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C")); // 3
    // Go back to snapshot "B"
    table.manageSnapshots().rollbackTo(2).commit(); // 2
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(2);
    filesMatch(Lists.newArrayList("B"), appendsBetweenScan(1, 2));
    filesMatch(Lists.newArrayList("B"), appendsAfterScan(1));

    Transaction transaction = table.newTransaction();
    add(transaction.newAppend(), files("D")); // 4
    add(transaction.newAppend(), files("E")); // 5
    add(transaction.newAppend(), files("F"));
    transaction.commitTransaction();
    // Go back to snapshot "E"
    table.manageSnapshots().rollbackTo(5).commit();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(5);
    filesMatch(Lists.newArrayList("B", "D", "E"), appendsBetweenScan(1, 5));
    filesMatch(Lists.newArrayList("B", "D", "E"), appendsAfterScan(1));
  }

  @TestTemplate
  public void testIgnoreResiduals() throws IOException {
    add(table.newAppend(), files("A"));
    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C"));

    TableScan scan1 = table.newScan().filter(Expressions.equal("id", 5)).appendsBetween(1, 3);

    try (CloseableIterable<CombinedScanTask> tasks = scan1.planTasks()) {
      assertThat(tasks).as("Tasks should not be empty").hasSizeGreaterThan(0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          assertThat(fileScanTask.residual())
              .as("Residuals must be preserved")
              .isNotEqualTo(Expressions.alwaysTrue());
        }
      }
    }

    TableScan scan2 =
        table.newScan().filter(Expressions.equal("id", 5)).appendsBetween(1, 3).ignoreResiduals();

    try (CloseableIterable<CombinedScanTask> tasks = scan2.planTasks()) {
      assertThat(tasks).as("Tasks should not be empty").hasSizeGreaterThan(0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          assertThat(fileScanTask.residual())
              .as("Residuals must be ignored")
              .isEqualTo(Expressions.alwaysTrue());
        }
      }
    }
  }

  @TestTemplate
  public void testPlanWithExecutor() {
    add(table.newAppend(), files("A"));
    add(table.newAppend(), files("B"));
    add(table.newAppend(), files("C"));

    AtomicInteger planThreadsIndex = new AtomicInteger(0);
    TableScan scan =
        table
            .newScan()
            .appendsAfter(1)
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
        .isGreaterThanOrEqualTo(0);
  }

  private static DataFile file(String name) {
    return DataFiles.builder(SPEC)
        .withPath(name + ".parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(1)
        .build();
  }

  private static void add(AppendFiles appendFiles, List<DataFile> adds) {
    for (DataFile f : adds) {
      appendFiles.appendFile(f);
    }
    appendFiles.commit();
  }

  private static void delete(DeleteFiles deleteFiles, List<DataFile> deletes) {
    for (DataFile f : deletes) {
      deleteFiles.deleteFile(f);
    }
    deleteFiles.commit();
  }

  private static void replace(
      RewriteFiles rewriteFiles, List<DataFile> deletes, List<DataFile> adds) {
    rewriteFiles.rewriteFiles(Sets.newHashSet(deletes), Sets.newHashSet(adds));
    rewriteFiles.commit();
  }

  private static void overwrite(
      OverwriteFiles overwriteFiles, List<DataFile> adds, List<DataFile> deletes) {
    for (DataFile f : adds) {
      overwriteFiles.addFile(f);
    }
    for (DataFile f : deletes) {
      overwriteFiles.deleteFile(f);
    }
    overwriteFiles.commit();
  }

  private static List<DataFile> files(String... names) {
    return Lists.transform(Lists.newArrayList(names), TestIncrementalDataTableScan::file);
  }

  private List<String> appendsAfterScan(long fromSnapshotId) {
    final TableScan appendsAfter = table.newScan().appendsAfter(fromSnapshotId);
    return filesToScan(appendsAfter);
  }

  private List<String> appendsBetweenScan(long fromSnapshotId, long toSnapshotId) {
    Snapshot s1 = table.snapshot(fromSnapshotId);
    Snapshot s2 = table.snapshot(toSnapshotId);
    TableScan appendsBetween = table.newScan().appendsBetween(s1.snapshotId(), s2.snapshotId());
    return filesToScan(appendsBetween);
  }

  private static List<String> filesToScan(TableScan tableScan) {
    Iterable<String> filesToRead =
        Iterables.transform(
            tableScan.planFiles(),
            t -> {
              String path = t.file().path().toString();
              return path.split("\\.")[0];
            });
    return Lists.newArrayList(filesToRead);
  }

  private static void filesMatch(List<String> expected, List<String> actual) {
    Collections.sort(expected);
    Collections.sort(actual);
    assertThat(actual).isEqualTo(expected);
  }
}
