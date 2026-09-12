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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

/** Regression test for the copy-on-write runtime-filter concurrency bug (issue #18004). */
public class TestSparkCopyOnWriteScanConcurrency extends TestBaseWithCatalog {

  private static final int FILE_COUNT = 10;
  private static final long TIMEOUT_SECONDS = 30;
  private static final long JOIN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS);

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  // A COW UPDATE with a subquery becomes a UNION whose branches share one scan, which Spark can
  // filter() concurrently under AQE. Without synchronization a branch reads the full task set and
  // rewrites every file, duplicating rows (#18004).
  @TestTemplate
  public void testConcurrentRuntimeFilteringNarrowsSharedScan() throws Exception {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('write.update.mode'='copy-on-write')",
        tableName);

    // one single-row file per INSERT, so the narrowed set (1) is clearly distinct from the full set
    for (int i = 0; i < FILE_COUNT; i++) {
      sql("INSERT INTO %s VALUES (%d, 'data-%d')", tableName, i, i);
    }

    Table table = validationCatalog.loadTable(tableIdent);

    CountDownLatch enteredResetTasks = new CountDownLatch(1);
    CountDownLatch releaseWinner = new CountDownLatch(1);
    InstrumentedCopyOnWriteScan scan = newInstrumentedScan(table, enteredResetTasks, releaseWinner);

    // memoize the full task set and groups exactly as query planning would
    List<FileScanTask> plannedTasks = scan.tasks();
    scan.taskGroups();
    assertThat(plannedTasks).as("expected one task per single-row file").hasSize(FILE_COUNT);

    // `_file IN (<one location>)` runtime predicate, matching what Spark passes for a COW filter
    String targetLocation = plannedTasks.get(0).file().location();
    Predicate[] predicates = {filePathInPredicate(targetLocation)};

    AtomicReference<Throwable> winnerFailure = new AtomicReference<>();
    AtomicReference<Throwable> loserFailure = new AtomicReference<>();
    AtomicInteger observedTaskCount = new AtomicInteger(-1);
    AtomicInteger observedTaskGroupFileCount = new AtomicInteger(-1);

    // winner: narrows the scan, then parks in resetTasks while holding the scan monitor
    Thread winner =
        new Thread(
            () -> {
              try {
                scan.filter(predicates);
              } catch (Throwable t) {
                winnerFailure.set(t);
              }
            },
            "cow-filter-winner");

    // loser: runs filter() (short-circuits) then reads the shared task set, which must be narrowed
    Thread loser =
        new Thread(
            () -> {
              try {
                scan.filter(predicates);
                observedTaskCount.set(scan.tasks().size());
                observedTaskGroupFileCount.set(countFiles(scan.taskGroups()));
              } catch (Throwable t) {
                loserFailure.set(t);
              }
            },
            "cow-filter-loser");

    winner.start();
    assertThat(enteredResetTasks.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
        .as("winner should have entered resetTasks")
        .isTrue();

    loser.start();
    // release the winner once the loser has blocked on the monitor or finished its stale read
    awaitLoserSettled(loser, observedTaskCount);
    releaseWinner.countDown();

    winner.join(JOIN_TIMEOUT_MILLIS);
    loser.join(JOIN_TIMEOUT_MILLIS);

    if (winnerFailure.get() != null) {
      throw new AssertionError("winner branch failed", winnerFailure.get());
    }
    if (loserFailure.get() != null) {
      throw new AssertionError("loser branch failed", loserFailure.get());
    }

    assertThat(observedTaskCount.get())
        .as(
            "the losing UNION branch must observe the narrowed single-file task set, never the "
                + "full %s-file set (which would rewrite every file and duplicate rows, issue "
                + "#18004)",
            FILE_COUNT)
        .isEqualTo(1);
    assertThat(observedTaskGroupFileCount.get())
        .as("task groups must reflect the narrowed single-file set")
        .isEqualTo(1);
  }

  // waits until the loser can progress no further before releasing the winner: either blocked on
  // the scan monitor (synchronized filter()) or already done recording its task count
  private static void awaitLoserSettled(Thread loser, AtomicInteger observedTaskCount)
      throws InterruptedException {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (System.nanoTime() < deadlineNanos) {
      Thread.State state = loser.getState();
      if (state == Thread.State.BLOCKED
          || state == Thread.State.TERMINATED
          || observedTaskCount.get() != -1) {
        return;
      }
      Thread.sleep(1);
    }
  }

  // mirrors SparkScanBuilder#buildCopyOnWriteScan but returns an instrumented subclass
  private InstrumentedCopyOnWriteScan newInstrumentedScan(
      Table table, CountDownLatch enteredResetTasks, CountDownLatch releaseWinner) {
    SparkReadConf readConf = new SparkReadConf(spark, table);
    Schema schema = table.schema();
    Snapshot snapshot = table.currentSnapshot();

    BatchScan batchScan =
        table
            .newBatchScan()
            .useSnapshot(snapshot.snapshotId())
            .ignoreResiduals()
            .caseSensitive(readConf.caseSensitive())
            .filter(Expressions.alwaysTrue())
            .project(schema);

    return new InstrumentedCopyOnWriteScan(
        spark,
        table,
        schema,
        snapshot,
        null /* branch */,
        batchScan,
        readConf,
        schema /* projection */,
        Lists.newArrayList(),
        () -> null,
        enteredResetTasks,
        releaseWinner);
  }

  private static Predicate filePathInPredicate(String location) {
    NamedReference fileRef = FieldReference.apply(MetadataColumns.FILE_PATH.name());
    LiteralValue<UTF8String> literal =
        new LiteralValue<>(UTF8String.fromString(location), DataTypes.StringType);
    return new Predicate(
        "IN", new org.apache.spark.sql.connector.expressions.Expression[] {fileRef, literal});
  }

  private static int countFiles(List<ScanTaskGroup<FileScanTask>> taskGroups) {
    int count = 0;
    for (ScanTaskGroup<FileScanTask> group : taskGroups) {
      count += group.tasks().size();
    }
    return count;
  }

  // parks inside resetTasks to hold the filter() window open for another thread. Intentionally NOT
  // synchronized so the only serialization under test comes from SparkCopyOnWriteScan#filter.
  private static class InstrumentedCopyOnWriteScan extends SparkCopyOnWriteScan {
    private final CountDownLatch enteredResetTasks;
    private final CountDownLatch releaseWinner;

    InstrumentedCopyOnWriteScan(
        SparkSession spark,
        Table table,
        Schema schema,
        Snapshot snapshot,
        String branch,
        BatchScan scan,
        SparkReadConf readConf,
        Schema projection,
        List<Expression> filters,
        Supplier<ScanReport> scanReportSupplier,
        CountDownLatch enteredResetTasks,
        CountDownLatch releaseWinner) {
      super(
          spark,
          table,
          schema,
          snapshot,
          branch,
          scan,
          readConf,
          projection,
          filters,
          scanReportSupplier);
      this.enteredResetTasks = enteredResetTasks;
      this.releaseWinner = releaseWinner;
    }

    @Override
    protected void resetTasks(List<FileScanTask> filteredTasks) {
      enteredResetTasks.countDown();
      try {
        releaseWinner.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      super.resetTasks(filteredTasks);
    }
  }
}
