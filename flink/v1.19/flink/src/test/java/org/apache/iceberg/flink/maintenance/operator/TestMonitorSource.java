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
package org.apache.iceberg.flink.maintenance.operator;

import static org.apache.iceberg.flink.maintenance.operator.FlinkStreamingTestUtils.closeJobClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestMonitorSource extends OperatorTestBase {
  private static final TableChange EMPTY_EVENT = TableChange.empty();
  private static final RateLimiterStrategy HIGH_RATE = RateLimiterStrategy.perSecond(100.0);
  private static final RateLimiterStrategy LOW_RATE = RateLimiterStrategy.perSecond(1.0 / 10000.0);

  @TempDir private File checkpointDir;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testChangeReaderIterator(boolean withDelete) {
    if (withDelete) {
      sql.exec(
          "CREATE TABLE %s (id int, data varchar, PRIMARY KEY(`id`) NOT ENFORCED) WITH ('format-version'='2', 'write.upsert.enabled'='true')",
          TABLE_NAME);
    } else {
      sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    }

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();
    Table table = tableLoader.loadTable();

    MonitorSource.TableChangeIterator iterator =
        new MonitorSource.TableChangeIterator(tableLoader, null, Long.MAX_VALUE);

    // For an empty table we get an empty result
    assertThat(iterator.next()).isEqualTo(EMPTY_EVENT);

    // Add a single commit and get back the commit data in the event
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    table.refresh();
    TableChange expected = tableChangeWithLastSnapshot(table, TableChange.empty());
    assertThat(iterator.next()).isEqualTo(expected);
    // Make sure that consecutive calls do not return the data again
    assertThat(iterator.next()).isEqualTo(EMPTY_EVENT);

    // Add two more commits, but fetch the data in one loop
    sql.exec("INSERT INTO %s VALUES (2, 'b')", TABLE_NAME);
    table.refresh();
    expected = tableChangeWithLastSnapshot(table, TableChange.empty());

    sql.exec("INSERT INTO %s VALUES (3, 'c')", TABLE_NAME);
    table.refresh();
    expected = tableChangeWithLastSnapshot(table, expected);

    assertThat(iterator.next()).isEqualTo(expected);
    // Make sure that consecutive calls do not return the data again
    assertThat(iterator.next()).isEqualTo(EMPTY_EVENT);
  }

  /**
   * Create a table and check that the source returns the data as new commits arrive to the table.
   */
  @Test
  void testSource() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar) "
            + "WITH ('flink.max-continuous-empty-commits'='100000')",
        TABLE_NAME);
    Configuration config = new Configuration();
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + checkpointDir.getPath());
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.enableCheckpointing(1000);
    env.setParallelism(1);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();
    Table table = tableLoader.loadTable();
    DataStream<TableChange> events =
        env.fromSource(
                new MonitorSource(tableLoader, HIGH_RATE, Long.MAX_VALUE),
                WatermarkStrategy.noWatermarks(),
                "TableChangeSource")
            .forceNonParallel();

    // Creating a stream for inserting data into the table concurrently
    ManualSource<RowData> insertSource =
        new ManualSource<>(env, InternalTypeInfo.of(FlinkSchemaUtil.convert(table.schema())));
    FlinkSink.forRowData(insertSource.dataStream())
        .tableLoader(tableLoader)
        .uidPrefix("iceberg-sink")
        .append();

    // Sink to collect the results
    CollectingSink<TableChange> result = new CollectingSink<>();
    events.sinkTo(result);

    // First result is an empty event
    env.executeAsync("Table Change Source Test");
    assertThat(result.poll(Duration.ofSeconds(5L))).isEqualTo(EMPTY_EVENT);

    // Insert some data
    insertSource.sendRecord(GenericRowData.of(1, StringData.fromString("a")));
    // Wait until the changes are committed
    Awaitility.await()
        .until(
            () -> {
              table.refresh();
              return table.currentSnapshot() != null;
            });

    table.refresh();
    long size = firstFileLength(table);

    // Wait until the first non-empty event has arrived, and check the expected result
    Awaitility.await()
        .until(
            () -> {
              TableChange newEvent = result.poll(Duration.ofSeconds(5L));
              // Fetch every empty event from the beginning
              while (newEvent.equals(EMPTY_EVENT)) {
                newEvent = result.poll(Duration.ofSeconds(5L));
              }

              // The first non-empty event should contain the expected value
              return newEvent.equals(new TableChange(1, 0, size, 0L, 1));
            });
  }

  /** Check that the {@link MonitorSource} operator state is restored correctly. */
  @Test
  void testStateRestore(@TempDir File savepointDir) throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    Configuration config = new Configuration();
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + checkpointDir.getPath());
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.enableCheckpointing(1000);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();
    DataStream<TableChange> events =
        env.fromSource(
                new MonitorSource(tableLoader, HIGH_RATE, Long.MAX_VALUE),
                WatermarkStrategy.noWatermarks(),
                "TableChangeSource")
            .forceNonParallel();

    // Sink to collect the results
    CollectingSink<TableChange> result = new CollectingSink<>();
    events.sinkTo(result);

    // Start the job
    Configuration conf;
    JobClient jobClient = null;
    AtomicReference<TableChange> firstNonEmptyEvent = new AtomicReference<>();
    try {
      jobClient = env.executeAsync("Table Change Source Test");

      Awaitility.await()
          .until(
              () -> {
                TableChange newEvent = result.poll(Duration.ofSeconds(5L));
                // Fetch every empty event from the beginning
                while (newEvent.equals(EMPTY_EVENT)) {
                  newEvent = result.poll(Duration.ofSeconds(5L));
                }

                // The first non-empty event should contain the expected value
                firstNonEmptyEvent.set(newEvent);
                return true;
              });
    } finally {
      // Stop with savepoint
      conf = closeJobClient(jobClient, savepointDir);
    }

    // Restore from savepoint, create the same topology with a different env
    env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    events =
        env.fromSource(
                new MonitorSource(tableLoader, LOW_RATE, Long.MAX_VALUE),
                WatermarkStrategy.noWatermarks(),
                "TableChangeSource")
            .forceNonParallel();
    CollectingSink<TableChange> resultWithSavepoint = new CollectingSink<>();
    events.sinkTo(resultWithSavepoint);

    // Make sure that the job with restored source does not read new records from the table
    JobClient clientWithSavepoint = null;
    try {
      clientWithSavepoint = env.executeAsync("Table Change Source test with savepoint");

      assertThat(resultWithSavepoint.poll(Duration.ofSeconds(5L))).isEqualTo(EMPTY_EVENT);
    } finally {
      closeJobClient(clientWithSavepoint, null);
    }

    // Restore without savepoint
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    events =
        env.fromSource(
                new MonitorSource(tableLoader, LOW_RATE, Long.MAX_VALUE),
                WatermarkStrategy.noWatermarks(),
                "TableChangeSource")
            .forceNonParallel();
    CollectingSink<TableChange> resultWithoutSavepoint = new CollectingSink<>();
    events.sinkTo(resultWithoutSavepoint);

    // Make sure that a new job without state reads the event as expected
    JobClient clientWithoutSavepoint = null;
    try {
      clientWithoutSavepoint = env.executeAsync("Table Change Source Test without savepoint");
      assertThat(resultWithoutSavepoint.poll(Duration.ofSeconds(5L)))
          .isEqualTo(firstNonEmptyEvent.get());
    } finally {
      closeJobClient(clientWithoutSavepoint);
    }
  }

  @Test
  void testNotOneParallelismThrows() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    env.fromSource(
            new MonitorSource(tableLoader, HIGH_RATE, Long.MAX_VALUE),
            WatermarkStrategy.noWatermarks(),
            "TableChangeSource")
        .setParallelism(2)
        .print();

    assertThatThrownBy(env::execute)
        .isInstanceOf(JobExecutionException.class)
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Parallelism should be set to 1");
  }

  @Test
  void testMaxReadBack() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    MonitorSource.TableChangeIterator iterator =
        new MonitorSource.TableChangeIterator(tableLoader, null, 1);

    // For a single maxReadBack we only get a single change
    assertThat(iterator.next().commitNum()).isEqualTo(1);

    iterator = new MonitorSource.TableChangeIterator(tableLoader, null, 2);

    // Expecting 2 commits/snapshots for maxReadBack=2
    assertThat(iterator.next().commitNum()).isEqualTo(2);

    iterator = new MonitorSource.TableChangeIterator(tableLoader, null, Long.MAX_VALUE);

    // For maxReadBack Long.MAX_VALUE we get every change
    assertThat(iterator.next().commitNum()).isEqualTo(3);
  }

  @Test
  void testSkipReplace() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    MonitorSource.TableChangeIterator iterator =
        new MonitorSource.TableChangeIterator(tableLoader, null, Long.MAX_VALUE);

    // Read the current snapshot
    assertThat(iterator.next().commitNum()).isEqualTo(1);

    // Create a DataOperations.REPLACE snapshot
    Table table = tableLoader.loadTable();
    DataFile dataFile =
        table.snapshots().iterator().next().addedDataFiles(table.io()).iterator().next();
    RewriteFiles rewrite = tableLoader.loadTable().newRewrite();
    // Replace the file with itself for testing purposes
    rewrite.deleteFile(dataFile);
    rewrite.addFile(dataFile);
    rewrite.commit();

    // Check that the rewrite is ignored
    assertThat(iterator.next()).isEqualTo(EMPTY_EVENT);
  }

  private static long firstFileLength(Table table) {
    return table.currentSnapshot().addedDataFiles(table.io()).iterator().next().fileSizeInBytes();
  }

  private static TableChange tableChangeWithLastSnapshot(Table table, TableChange previous) {
    List<DataFile> dataFiles =
        Lists.newArrayList(table.currentSnapshot().addedDataFiles(table.io()).iterator());
    List<DeleteFile> deleteFiles =
        Lists.newArrayList(table.currentSnapshot().addedDeleteFiles(table.io()).iterator());

    long dataSize = dataFiles.stream().mapToLong(d -> d.fileSizeInBytes()).sum();
    long deleteSize = deleteFiles.stream().mapToLong(d -> d.fileSizeInBytes()).sum();
    boolean hasDelete = table.currentSnapshot().addedDeleteFiles(table.io()).iterator().hasNext();

    return new TableChange(
        previous.dataFileNum() + dataFiles.size(),
        previous.deleteFileNum() + deleteFiles.size(),
        previous.dataFileSize() + dataSize,
        previous.deleteFileSize() + deleteSize,
        previous.commitNum() + 1);
  }
}
