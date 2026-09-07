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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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

/**
 * Reproduces the copy-on-write runtime-filter concurrency bug (issue #18004).
 *
 * <p>A COW {@code UPDATE ... WHERE EXISTS (<subquery>)} is rewritten as {@code Union(Filter(cond,
 * S), Filter(NOT cond, S))} where BOTH branches share the same {@link SparkCopyOnWriteScan} {@code
 * S}. Under AQE the two branch stages are prepared concurrently, so the {@link
 * org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering#filter(Predicate[])} callback runs
 * concurrently on the one shared scan. {@code filter()} is a non-atomic check-then-act: it
 * publishes {@code filteredLocations} first, then narrows {@code tasks()}, and only at the very end
 * calls {@code resetTasks(...)}. A losing branch that observes {@code filteredLocations} already
 * set (so it skips narrowing) while the winner has not yet reached {@code resetTasks} would read
 * the planning-time memoized FULL task set and scan every file, duplicating rows on commit.
 *
 * <p>This drives the race deterministically: a winner thread narrows the scan but parks inside an
 * un-synchronized {@code resetTasks} override, while a loser thread runs {@code filter()} and reads
 * the shared task set. With {@code filter()} synchronized the loser is serialized behind the winner
 * and observes the narrowed single-file set; revert the {@code synchronized} keyword and it
 * observes the full set.
 */
public class TestSparkCopyOnWriteScanConcurrency extends TestBaseWithCatalog {

  private static final int FILE_COUNT = 10;
  private static final long PARK_MILLIS = TimeUnit.SECONDS.toMillis(2);
  private static final long LATCH_TIMEOUT_SECONDS = 30;
  private static final long JOIN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testConcurrentRuntimeFilteringNarrowsSharedScan() throws Exception {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('write.update.mode'='copy-on-write')",
        tableName);

    // one single-row data file per INSERT, so the full task set is clearly distinguishable from
    // the single-file set the runtime filter should produce
    for (int i = 0; i < FILE_COUNT; i++) {
      sql("INSERT INTO %s VALUES (%d, 'data-%d')", tableName, i, i);
    }

    Table table = validationCatalog.loadTable(tableIdent);

    CountDownLatch enteredResetTasks = new CountDownLatch(1);
    InstrumentedCopyOnWriteScan scan = newInstrumentedScan(table, enteredResetTasks);

    // memoize the full planning-time task set and task groups exactly as query planning would
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

    // winner: narrows the scan but parks inside resetTasks. It holds the scan monitor while it
    // sleeps ONLY if filter() is synchronized; that is exactly the production behavior under test.
    Thread winner =
        new Thread(
            () -> {
              try {
                scan.filter(predicates);
              } catch (Throwable t) {
                winnerFailure.set(t);
                enteredResetTasks.countDown();
              }
            },
            "cow-filter-winner");

    // loser: proceeds only once the winner has published filteredLocations and entered resetTasks.
    // Its own filter() short-circuits (the guard fails for the same single location), then it reads
    // the shared task set - which must already be narrowed.
    Thread loser =
        new Thread(
            () -> {
              try {
                assertThat(enteredResetTasks.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .as("winner should have entered resetTasks")
                    .isTrue();
                scan.filter(predicates);
                observedTaskCount.set(scan.tasks().size());
                observedTaskGroupFileCount.set(countFiles(scan.taskGroups()));
              } catch (Throwable t) {
                loserFailure.set(t);
              }
            },
            "cow-filter-loser");

    winner.start();
    loser.start();
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

  // mirrors SparkScanBuilder#buildCopyOnWriteScan (non-null snapshot path) but returns an
  // instrumented subclass so the race window inside resetTasks can be controlled deterministically
  private InstrumentedCopyOnWriteScan newInstrumentedScan(
      Table table, CountDownLatch enteredResetTasks) {
    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    Schema expectedSchema = table.schema();
    Snapshot snapshot = table.currentSnapshot();

    BatchScan batchScan =
        table
            .newBatchScan()
            .useSnapshot(snapshot.snapshotId())
            .ignoreResiduals()
            .caseSensitive(readConf.caseSensitive())
            .filter(Expressions.alwaysTrue())
            .project(expectedSchema);

    return new InstrumentedCopyOnWriteScan(
        spark,
        table,
        batchScan,
        snapshot,
        readConf,
        expectedSchema,
        Lists.newArrayList(),
        () -> null,
        enteredResetTasks);
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

  /**
   * A {@link SparkCopyOnWriteScan} that parks inside {@code resetTasks} (before delegating to the
   * real implementation) so the check-then-act window in {@code filter()} can be observed by
   * another thread. The override is intentionally NOT synchronized: the only serialization under
   * test must come from {@code SparkCopyOnWriteScan#filter}, so reverting that fix reliably
   * reproduces the race instead of being masked here.
   */
  private static class InstrumentedCopyOnWriteScan extends SparkCopyOnWriteScan {
    private final CountDownLatch enteredResetTasks;

    InstrumentedCopyOnWriteScan(
        SparkSession spark,
        Table table,
        BatchScan scan,
        Snapshot snapshot,
        SparkReadConf readConf,
        Schema expectedSchema,
        List<Expression> filters,
        Supplier<ScanReport> scanReportSupplier,
        CountDownLatch enteredResetTasks) {
      super(spark, table, scan, snapshot, readConf, expectedSchema, filters, scanReportSupplier);
      this.enteredResetTasks = enteredResetTasks;
    }

    @Override
    protected void resetTasks(List<FileScanTask> filteredTasks) {
      enteredResetTasks.countDown();
      try {
        Thread.sleep(PARK_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      super.resetTasks(filteredTasks);
    }
  }
}
