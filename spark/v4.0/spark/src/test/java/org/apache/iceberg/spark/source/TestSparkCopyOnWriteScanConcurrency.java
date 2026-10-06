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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;

public class TestSparkCopyOnWriteScanConcurrency extends TestBaseWithCatalog {

  private static final long TIMEOUT_SECONDS = 10;

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void concurrentRuntimeFilteringNarrowsSharedScan() throws Exception {
    sql("CREATE TABLE %s (id BIGINT, data STRING) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    CountDownLatch enteredResetTasks = new CountDownLatch(1);
    CountDownLatch releaseFilter = new CountDownLatch(1);
    InstrumentedScan scan = newScan(table, enteredResetTasks, releaseFilter);

    assertThat(scan.tasks()).hasSize(2);
    scan.taskGroups();
    String location = scan.tasks().get(0).file().location();
    NamedReference fileRef = FieldReference.apply(MetadataColumns.FILE_PATH.name());
    LiteralValue<UTF8String> literal =
        new LiteralValue<>(UTF8String.fromString(location), DataTypes.StringType);
    Predicate[] predicates = {
      new Predicate(
          "IN", new org.apache.spark.sql.connector.expressions.Expression[] {fileRef, literal})
    };

    FutureTask<Void> winnerTask =
        new FutureTask<>(
            () -> {
              scan.filter(predicates);
              return null;
            });
    FutureTask<int[]> loserTask =
        new FutureTask<>(
            () -> {
              scan.filter(predicates);
              int tasks = scan.tasks().size();
              int taskGroups =
                  scan.taskGroups().stream().mapToInt(group -> group.tasks().size()).sum();
              return new int[] {tasks, taskGroups};
            });
    Thread winner = new Thread(winnerTask);
    Thread loser = new Thread(loserTask);

    winner.start();
    assertThat(enteredResetTasks.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    loser.start();
    try {
      Awaitility.await()
          .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .until(() -> loser.getState() == Thread.State.BLOCKED || loserTask.isDone());
    } finally {
      releaseFilter.countDown();
    }

    winner.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    loser.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    assertThat(winner.isAlive()).isFalse();
    assertThat(loser.isAlive()).isFalse();
    assertThat(winnerTask.get()).isNull();
    assertThat(loserTask.get()).containsExactly(1, 1);
  }

  private InstrumentedScan newScan(
      Table table, CountDownLatch enteredResetTasks, CountDownLatch releaseFilter) {
    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
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
    return new InstrumentedScan(
        spark,
        table,
        batchScan,
        snapshot,
        readConf,
        schema,
        Collections.emptyList(),
        () -> null,
        enteredResetTasks,
        releaseFilter);
  }

  private static class InstrumentedScan extends SparkCopyOnWriteScan {
    private final CountDownLatch enteredResetTasks;
    private final CountDownLatch releaseFilter;

    InstrumentedScan(
        SparkSession spark,
        Table table,
        BatchScan scan,
        Snapshot snapshot,
        SparkReadConf readConf,
        Schema schema,
        List<Expression> filters,
        Supplier<ScanReport> scanReportSupplier,
        CountDownLatch enteredResetTasks,
        CountDownLatch releaseFilter) {
      super(spark, table, scan, snapshot, readConf, schema, filters, scanReportSupplier);
      this.enteredResetTasks = enteredResetTasks;
      this.releaseFilter = releaseFilter;
    }

    @Override
    protected void resetTasks(List<FileScanTask> filteredTasks) {
      enteredResetTasks.countDown();
      try {
        assertThat(releaseFilter.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
      super.resetTasks(filteredTasks);
    }
  }
}
