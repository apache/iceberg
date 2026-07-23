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

import static org.apache.iceberg.expressions.Expressions.ref;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public final class TestStructuredStreamingRead3 extends CatalogTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, async = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        true
      },
      {
        SparkCatalogConfig.REST.catalogName(),
        SparkCatalogConfig.REST.implementation(),
        ImmutableMap.builder()
            .putAll(SparkCatalogConfig.REST.properties())
            .put(CatalogProperties.URI, restCatalog.properties().get(CatalogProperties.URI))
            .build(),
        false
      }
    };
  }

  private Table table;

  private final AtomicInteger microBatches = new AtomicInteger();

  @Parameter(index = 3)
  private Boolean async;

  /**
   * test data to be used by multiple writes each write creates a snapshot and writes a list of
   * records
   */
  private static final List<List<SimpleRecord>> TEST_DATA_MULTIPLE_SNAPSHOTS =
      Lists.newArrayList(
          Lists.newArrayList(
              new SimpleRecord(1, "one"), new SimpleRecord(2, "two"), new SimpleRecord(3, "three")),
          Lists.newArrayList(new SimpleRecord(4, "four"), new SimpleRecord(5, "five")),
          Lists.newArrayList(new SimpleRecord(6, "six"), new SimpleRecord(7, "seven")));

  /**
   * test data - to be used for multiple write batches each batch inturn will have multiple
   * snapshots
   */
  private static final List<List<List<SimpleRecord>>> TEST_DATA_MULTIPLE_WRITES_MULTIPLE_SNAPSHOTS =
      Lists.newArrayList(
          Lists.newArrayList(
              Lists.newArrayList(
                  new SimpleRecord(1, "one"),
                  new SimpleRecord(2, "two"),
                  new SimpleRecord(3, "three")),
              Lists.newArrayList(new SimpleRecord(4, "four"), new SimpleRecord(5, "five"))),
          Lists.newArrayList(
              Lists.newArrayList(new SimpleRecord(6, "six"), new SimpleRecord(7, "seven")),
              Lists.newArrayList(new SimpleRecord(8, "eight"), new SimpleRecord(9, "nine"))),
          Lists.newArrayList(
              Lists.newArrayList(
                  new SimpleRecord(10, "ten"),
                  new SimpleRecord(11, "eleven"),
                  new SimpleRecord(12, "twelve")),
              Lists.newArrayList(
                  new SimpleRecord(13, "thirteen"), new SimpleRecord(14, "fourteen")),
              Lists.newArrayList(
                  new SimpleRecord(15, "fifteen"), new SimpleRecord(16, "sixteen"))));

  @BeforeAll
  public static void setupSpark() {
    // disable AQE as tests assume that writes generate a particular number of files
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @BeforeEach
  public void setupTable() {
    sql(
        "CREATE TABLE %s "
            + "(id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(3, id)) "
            + "TBLPROPERTIES ('commit.manifest.min-count-to-merge'='3', 'commit.manifest-merge.enabled'='true')",
        tableName);
    this.table = validationCatalog.loadTable(tableIdent);
    microBatches.set(0);
  }

  @AfterEach
  public void stopStreams() throws TimeoutException {
    for (StreamingQuery query : spark.streams().active()) {
      query.stop();
    }
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testReadStreamOnIcebergTableWithMultipleSnapshots() throws Exception {
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    StreamingQuery query = startStream();

    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadStreamWithMaxFiles1() throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);
    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1"),
        List.of(1L, 2L, 1L, 1L, 1L, 1L));

    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1"),
        List.of(1L, 2L, 1L, 1L, 1L, 1L),
        Trigger.AvailableNow());
  }

  @TestTemplate
  public void testReadStreamWithMaxFiles2() throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);
    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "2"),
        List.of(3L, 2L, 2L));

    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "2"),
        List.of(3L, 2L, 2L),
        Trigger.AvailableNow());
  }

  @TestTemplate
  public void testReadStreamWithMaxRows1() throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);
    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "1"),
        List.of(1L, 2L, 1L, 1L, 1L, 1L));

    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "1"),
        List.of(1L, 2L, 1L, 1L, 1L, 1L),
        Trigger.AvailableNow());

    // soft limit of 1 is being enforced, the stream is not blocked.
    StreamingQuery query = startStream(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "1");

    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual)
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(TEST_DATA_MULTIPLE_SNAPSHOTS));
  }

  @TestTemplate
  public void testReadStreamWithMaxRows2() throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);
    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "2"),
        List.of(3L, 2L, 2L));

    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "2"),
        List.of(3L, 2L, 2L),
        Trigger.AvailableNow());

    StreamingQuery query =
        startStream(ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "2"));

    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual)
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(TEST_DATA_MULTIPLE_SNAPSHOTS));
  }

  @TestTemplate
  public void testReadStreamWithMaxRows4() throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);
    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "4"), List.of(4L, 3L));

    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "4"),
        List.of(4L, 3L),
        Trigger.AvailableNow());
  }

  @TestTemplate
  public void testReadStreamWithCompositeReadLimit() throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);
    assertMicroBatchRecordSizes(
        ImmutableMap.of(
            SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1",
            SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "2"),
        List.of(1L, 2L, 1L, 1L, 1L, 1L));

    assertMicroBatchRecordSizes(
        ImmutableMap.of(
            SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1",
            SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "2"),
        List.of(1L, 2L, 1L, 1L, 1L, 1L),
        Trigger.AvailableNow());
  }

  @TestTemplate
  public void testReadStreamWithLowAsyncQueuePreload() throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);
    // Set low preload limits to test async queue behavior - background thread should load
    // remaining data

    StreamingQuery query =
        startStream(
            ImmutableMap.of(
                SparkReadOptions.ASYNC_QUEUE_PRELOAD_ROW_LIMIT,
                "5",
                SparkReadOptions.ASYNC_QUEUE_PRELOAD_FILE_LIMIT,
                "5"));

    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual)
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(TEST_DATA_MULTIPLE_SNAPSHOTS));
  }

  @TestTemplate
  public void testAvailableNowStreamReadShouldNotHangOrReprocessData() throws Exception {
    File writerCheckpointFolder = temp.resolve("writer-checkpoint-folder").toFile();
    File writerCheckpoint = new File(writerCheckpointFolder, "writer-checkpoint");
    File output = temp.resolve("junit").toFile();

    Map<String, String> options = Maps.newHashMap();
    options.put(SparkReadOptions.ASYNC_MICRO_BATCH_PLANNING_ENABLED, async.toString());
    if (async) {
      options.put(SparkReadOptions.STREAMING_SNAPSHOT_POLLING_INTERVAL_MS, "1");
    }

    DataStreamWriter querySource =
        spark
            .readStream()
            .options(options)
            .format("iceberg")
            .load(tableName)
            .writeStream()
            .option("checkpointLocation", writerCheckpoint.toString())
            .format("parquet")
            .trigger(Trigger.AvailableNow())
            .option("path", output.getPath());

    List<SimpleRecord> expected = Lists.newArrayList();
    for (List<List<SimpleRecord>> expectedCheckpoint :
        TEST_DATA_MULTIPLE_WRITES_MULTIPLE_SNAPSHOTS) {

      // New data was added while the stream was not running
      appendDataAsMultipleSnapshots(expectedCheckpoint);
      expected.addAll(Lists.newArrayList(Iterables.concat(Iterables.concat(expectedCheckpoint))));

      try {
        StreamingQuery query = querySource.start();

        // Query should terminate on its own after processing all available data
        assertThat(query.awaitTermination(60000)).isTrue();

        // Check output
        List<SimpleRecord> actual =
            spark
                .read()
                .load(output.getPath())
                .as(Encoders.bean(SimpleRecord.class))
                .collectAsList();
        assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));

        // Restarting immediately should not reprocess data
        query = querySource.start();
        assertThat(query.awaitTermination(60000)).isTrue();
        assertThat(query.recentProgress().length).isEqualTo(1);
        assertThat(query.recentProgress()[0].sources()[0].startOffset())
            .isEqualTo(query.recentProgress()[0].sources()[0].endOffset());
      } finally {
        stopStreams();
      }
    }
  }

  @TestTemplate
  public void testTriggerAvailableNowDoesNotProcessNewDataWhileRunning() throws Exception {
    List<List<SimpleRecord>> expectedData = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expectedData);

    long expectedRecordCount = expectedData.stream().mapToLong(List::size).sum();

    table.refresh();
    long expectedSnapshotId = table.currentSnapshot().snapshotId();

    String sinkTable = "availablenow_sink";
    Map<String, String> options = Maps.newHashMap();
    options.put(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1");
    options.put(SparkReadOptions.ASYNC_MICRO_BATCH_PLANNING_ENABLED, async.toString());
    if (async) {
      options.put(SparkReadOptions.STREAMING_SNAPSHOT_POLLING_INTERVAL_MS, "1");
    }

    StreamingQuery query =
        spark
            .readStream()
            .options(options)
            .format("iceberg")
            .load(tableName)
            .writeStream()
            .format("memory")
            .queryName(sinkTable)
            .trigger(Trigger.AvailableNow())
            .start();

    assertThat(query.isActive()).isTrue();

    // Add new data while the stream is running
    List<SimpleRecord> newDataDuringStreamSnap1 =
        Lists.newArrayList(
            new SimpleRecord(100, "hundred"),
            new SimpleRecord(101, "hundred-one"),
            new SimpleRecord(102, "hundred-two"));
    List<SimpleRecord> newDataDuringStreamSnap2 =
        Lists.newArrayList(
            new SimpleRecord(200, "two-hundred"), new SimpleRecord(201, "two-hundred-one"));
    appendData(newDataDuringStreamSnap1);
    appendData(newDataDuringStreamSnap2);

    // Query should terminate on its own after processing all available data till expectedSnapshotId
    assertThat(query.awaitTermination(60000)).isTrue();

    List<SimpleRecord> actualResults =
        spark
            .sql("SELECT * FROM " + sinkTable)
            .as(Encoders.bean(SimpleRecord.class))
            .collectAsList();
    long endOffsetSnapshotId =
        StreamingOffset.fromJson(query.lastProgress().sources()[0].endOffset()).snapshotId();

    // Verify the stream processed only up to the snapshot present when started
    assertThat(expectedSnapshotId).isEqualTo(endOffsetSnapshotId);

    // Verify only the initial data was processed
    assertThat(actualResults.size()).isEqualTo(expectedRecordCount);
    assertThat(actualResults).containsExactlyInAnyOrderElementsOf(Iterables.concat(expectedData));
  }

  @TestTemplate
  public void testTriggerAvailableNowCapsAsyncPreloadAfterPrepare() {
    List<List<SimpleRecord>> initialData =
        List.of(List.of(new SimpleRecord(1, "one")), List.of(new SimpleRecord(2, "two")));
    appendDataAsMultipleSnapshots(initialData);

    table.refresh();
    long expectedCapSnapshotId = table.currentSnapshot().snapshotId();

    SparkMicroBatchStream stream =
        new SparkMicroBatchStream(
            JavaSparkContext.fromSparkContext(spark.sparkContext()),
            table,
            table::io,
            new SparkReadConf(
                spark,
                table,
                ImmutableMap.of(
                    SparkReadOptions.ASYNC_MICRO_BATCH_PLANNING_ENABLED,
                    async.toString(),
                    SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH,
                    "1",
                    SparkReadOptions.STREAMING_SNAPSHOT_POLLING_INTERVAL_MS,
                    "1",
                    SparkReadOptions.ASYNC_QUEUE_PRELOAD_FILE_LIMIT,
                    "10",
                    SparkReadOptions.ASYNC_QUEUE_PRELOAD_ROW_LIMIT,
                    "10")),
            table.schema(),
            temp.resolve("available-now-cap-checkpoint").toString());

    try {
      stream.prepareForTriggerAvailableNow();

      appendData(List.of(new SimpleRecord(3, "three")));

      Offset startOffset = stream.initialOffset();
      Offset firstEndOffset = stream.latestOffset(startOffset, stream.getDefaultReadLimit());
      assertThat(firstEndOffset).isNotNull();
      stream.planInputPartitions(startOffset, firstEndOffset);

      Offset secondEndOffset = stream.latestOffset(firstEndOffset, stream.getDefaultReadLimit());
      assertThat(secondEndOffset).isNotNull();
      stream.planInputPartitions(firstEndOffset, secondEndOffset);

      assertThat(stream.latestOffset(secondEndOffset, stream.getDefaultReadLimit())).isNull();
      assertThat(((StreamingOffset) secondEndOffset).snapshotId()).isEqualTo(expectedCapSnapshotId);
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  public void testLatestOffsetReturnsNullAfterFinalBatchIsConsumed() throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);

    table.refresh();
    int expectedBatchCount;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      expectedBatchCount = Iterables.size(tasks);
    }

    SparkMicroBatchStream stream =
        newMicroBatchStream(
            ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1"),
            "drain-to-null-checkpoint");

    try {
      int plannedBatchCount = 0;
      Offset startOffset = stream.initialOffset();
      Offset endOffset = stream.latestOffset(startOffset, stream.getDefaultReadLimit());
      while (endOffset != null) {
        InputPartition[] partitions = stream.planInputPartitions(startOffset, endOffset);
        assertThat(partitions).isNotEmpty();
        plannedBatchCount += 1;
        startOffset = endOffset;
        endOffset = stream.latestOffset(startOffset, stream.getDefaultReadLimit());
      }

      assertThat(endOffset).isNull();
      assertThat(plannedBatchCount).isEqualTo(expectedBatchCount);
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  public void testPlanInputPartitionsIsIdempotentForSameOffsets() {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);

    SparkMicroBatchStream stream =
        newMicroBatchStream(
            ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1"),
            "idempotent-plan-files-checkpoint");

    try {
      Offset startOffset = stream.initialOffset();
      Offset endOffset = stream.latestOffset(startOffset, stream.getDefaultReadLimit());

      assertThat(endOffset).isNotNull();

      InputPartition[] firstPartitions = stream.planInputPartitions(startOffset, endOffset);
      InputPartition[] secondPartitions = stream.planInputPartitions(startOffset, endOffset);

      List<String> firstFileLocations = Lists.newArrayList();
      for (InputPartition partition : firstPartitions) {
        SparkInputPartition sparkInputPartition = (SparkInputPartition) partition;
        for (FileScanTask task : sparkInputPartition.<FileScanTask>taskGroup().tasks()) {
          firstFileLocations.add(task.file().location());
        }
      }

      List<String> secondFileLocations = Lists.newArrayList();
      for (InputPartition partition : secondPartitions) {
        SparkInputPartition sparkInputPartition = (SparkInputPartition) partition;
        for (FileScanTask task : sparkInputPartition.<FileScanTask>taskGroup().tasks()) {
          secondFileLocations.add(task.file().location());
        }
      }

      assertThat(firstFileLocations).containsExactlyInAnyOrderElementsOf(secondFileLocations);

      startOffset = endOffset;
      endOffset = stream.latestOffset(startOffset, stream.getDefaultReadLimit());
      while (endOffset != null) {
        assertThat(stream.planInputPartitions(startOffset, endOffset)).isNotEmpty();
        startOffset = endOffset;
        endOffset = stream.latestOffset(startOffset, stream.getDefaultReadLimit());
      }

      assertThat(endOffset).isNull();
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  public void testReadStreamOnIcebergThenAddData() throws Exception {
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;

    StreamingQuery query = startStream();

    appendDataAsMultipleSnapshots(expected);

    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadingStreamFromTimestamp() throws Exception {
    List<SimpleRecord> dataBeforeTimestamp =
        Lists.newArrayList(
            new SimpleRecord(-2, "minustwo"),
            new SimpleRecord(-1, "minusone"),
            new SimpleRecord(0, "zero"));

    appendData(dataBeforeTimestamp);

    table.refresh();
    long streamStartTimestamp = table.currentSnapshot().timestampMillis() + 1;

    StreamingQuery query =
        startStream(SparkReadOptions.STREAM_FROM_TIMESTAMP, Long.toString(streamStartTimestamp));

    List<SimpleRecord> empty = rowsAvailable(query);
    assertThat(empty).isEmpty();

    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    List<SimpleRecord> actual = rowsAvailable(query);

    assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadingStreamFromFutureTimetsamp() throws Exception {
    long futureTimestamp = System.currentTimeMillis() + 10000;

    StreamingQuery query =
        startStream(SparkReadOptions.STREAM_FROM_TIMESTAMP, Long.toString(futureTimestamp));

    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).isEmpty();

    List<SimpleRecord> data =
        Lists.newArrayList(
            new SimpleRecord(-2, "minustwo"),
            new SimpleRecord(-1, "minusone"),
            new SimpleRecord(0, "zero"));

    // Perform several inserts that should not show up because the fromTimestamp has not elapsed
    IntStream.range(0, 3)
        .forEach(
            x -> {
              appendData(data);
              assertThat(rowsAvailable(query)).isEmpty();
            });

    waitUntilAfter(futureTimestamp);

    // Data appended after the timestamp should appear
    appendData(data);
    // Allow async background thread to refresh, else test sometimes fails
    Thread.sleep(50);
    actual = rowsAvailable(query);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(data);
  }

  @TestTemplate
  public void testReadingStreamFromTimestampFutureWithExistingSnapshots() throws Exception {
    List<SimpleRecord> dataBeforeTimestamp =
        Lists.newArrayList(
            new SimpleRecord(1, "one"), new SimpleRecord(2, "two"), new SimpleRecord(3, "three"));
    appendData(dataBeforeTimestamp);

    long streamStartTimestamp = System.currentTimeMillis() + 2000;

    // Start the stream with a future timestamp after the current snapshot
    StreamingQuery query =
        startStream(SparkReadOptions.STREAM_FROM_TIMESTAMP, Long.toString(streamStartTimestamp));
    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).isEmpty();

    // Stream should contain data added after the timestamp elapses
    waitUntilAfter(streamStartTimestamp);
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);
    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadingStreamFromTimestampOfExistingSnapshot() throws Exception {
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;

    // Create an existing snapshot with some data
    appendData(expected.get(0));
    table.refresh();
    long firstSnapshotTime = table.currentSnapshot().timestampMillis();

    // Start stream giving the first Snapshot's time as the start point
    StreamingQuery stream =
        startStream(SparkReadOptions.STREAM_FROM_TIMESTAMP, Long.toString(firstSnapshotTime));

    // Append rest of expected data
    for (int i = 1; i < expected.size(); i++) {
      appendData(expected.get(i));
    }

    List<SimpleRecord> actual = rowsAvailable(stream);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadingStreamWithExpiredSnapshotFromTimestamp() throws TimeoutException {
    List<SimpleRecord> firstSnapshotRecordList = Lists.newArrayList(new SimpleRecord(1, "one"));

    List<SimpleRecord> secondSnapshotRecordList = Lists.newArrayList(new SimpleRecord(2, "two"));

    List<SimpleRecord> thirdSnapshotRecordList = Lists.newArrayList(new SimpleRecord(3, "three"));

    List<SimpleRecord> expectedRecordList = Lists.newArrayList();
    expectedRecordList.addAll(secondSnapshotRecordList);
    expectedRecordList.addAll(thirdSnapshotRecordList);

    appendData(firstSnapshotRecordList);
    table.refresh();
    long firstSnapshotid = table.currentSnapshot().snapshotId();
    long firstSnapshotCommitTime = table.currentSnapshot().timestampMillis();

    appendData(secondSnapshotRecordList);
    appendData(thirdSnapshotRecordList);

    table.expireSnapshots().expireSnapshotId(firstSnapshotid).commit();

    StreamingQuery query =
        startStream(
            SparkReadOptions.STREAM_FROM_TIMESTAMP, String.valueOf(firstSnapshotCommitTime));
    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRecordList);
  }

  @TestTemplate
  public void testResumingStreamReadFromCheckpoint() throws Exception {
    File writerCheckpointFolder = temp.resolve("writer-checkpoint-folder").toFile();
    File writerCheckpoint = new File(writerCheckpointFolder, "writer-checkpoint");
    File output = temp.resolve("junit").toFile();

    DataStreamWriter querySource =
        spark
            .readStream()
            .format("iceberg")
            .load(tableName)
            .writeStream()
            .option("checkpointLocation", writerCheckpoint.toString())
            .format("parquet")
            .queryName("checkpoint_test")
            .option("path", output.getPath());

    StreamingQuery startQuery = querySource.start();
    startQuery.processAllAvailable();
    startQuery.stop();

    List<SimpleRecord> expected = Lists.newArrayList();
    for (List<List<SimpleRecord>> expectedCheckpoint :
        TEST_DATA_MULTIPLE_WRITES_MULTIPLE_SNAPSHOTS) {
      // New data was added while the stream was down
      appendDataAsMultipleSnapshots(expectedCheckpoint);
      expected.addAll(Lists.newArrayList(Iterables.concat(Iterables.concat(expectedCheckpoint))));

      // Stream starts up again from checkpoint read the newly added data and shut down
      StreamingQuery restartedQuery = querySource.start();
      restartedQuery.processAllAvailable();
      restartedQuery.stop();

      // Read data added by the stream
      List<SimpleRecord> actual =
          spark.read().load(output.getPath()).as(Encoders.bean(SimpleRecord.class)).collectAsList();
      assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
    }
  }

  @TestTemplate
  public void testFailReadingCheckpointInvalidSnapshot() throws IOException, TimeoutException {
    File writerCheckpointFolder = temp.resolve("writer-checkpoint-folder").toFile();
    File writerCheckpoint = new File(writerCheckpointFolder, "writer-checkpoint");
    File output = temp.resolve("junit").toFile();

    DataStreamWriter querySource =
        spark
            .readStream()
            .format("iceberg")
            .load(tableName)
            .writeStream()
            .option("checkpointLocation", writerCheckpoint.toString())
            .format("parquet")
            .queryName("checkpoint_test")
            .option("path", output.getPath());

    List<SimpleRecord> firstSnapshotRecordList = Lists.newArrayList(new SimpleRecord(1, "one"));
    List<SimpleRecord> secondSnapshotRecordList = Lists.newArrayList(new SimpleRecord(2, "two"));
    StreamingQuery startQuery = querySource.start();

    appendData(firstSnapshotRecordList);
    table.refresh();
    long firstSnapshotid = table.currentSnapshot().snapshotId();
    startQuery.processAllAvailable();
    startQuery.stop();

    appendData(secondSnapshotRecordList);

    table.expireSnapshots().expireSnapshotId(firstSnapshotid).commit();

    StreamingQuery restartedQuery = querySource.start();
    assertThatThrownBy(restartedQuery::processAllAvailable)
        .hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            String.format(
                "Cannot load current offset at snapshot %d, the snapshot was expired or removed",
                firstSnapshotid));
  }

  @TestTemplate
  public void testParquetOrcAvroDataInOneTable() throws Exception {
    List<SimpleRecord> parquetFileRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "one"), new SimpleRecord(2, "two"), new SimpleRecord(3, "three"));

    List<SimpleRecord> orcFileRecords =
        Lists.newArrayList(new SimpleRecord(4, "four"), new SimpleRecord(5, "five"));

    List<SimpleRecord> avroFileRecords =
        Lists.newArrayList(new SimpleRecord(6, "six"), new SimpleRecord(7, "seven"));

    appendData(parquetFileRecords);
    appendData(orcFileRecords, "orc");
    appendData(avroFileRecords, "avro");

    StreamingQuery query = startStream();
    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(
            Iterables.concat(parquetFileRecords, orcFileRecords, avroFileRecords));
  }

  @TestTemplate
  public void testReadStreamFromEmptyTable() throws Exception {
    StreamingQuery stream = startStream();
    List<SimpleRecord> actual = rowsAvailable(stream);
    assertThat(actual).isEmpty();
  }

  @TestTemplate
  public void testDefaultModeReadsCurrentSnapshotInFullThenIncremental() throws Exception {
    List<List<SimpleRecord>> existing = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(existing);

    StreamingQuery query = startStream();

    List<SimpleRecord> afterInitial = rowsAvailable(query);
    assertThat(afterInitial).containsExactlyInAnyOrderElementsOf(Iterables.concat(existing));

    List<SimpleRecord> newRecords =
        Lists.newArrayList(new SimpleRecord(100, "hundred"), new SimpleRecord(101, "hundred-one"));
    appendData(newRecords);

    List<SimpleRecord> total = Lists.newArrayList(Iterables.concat(existing));
    total.addAll(newRecords);

    List<SimpleRecord> afterIncremental = rowsAvailable(query);
    assertThat(afterIncremental).containsExactlyInAnyOrderElementsOf(total);
  }

  @TestTemplate
  public void testStreamFromSnapshotLatestSkipsBacklog() throws Exception {
    List<List<SimpleRecord>> existing = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(existing);

    StreamingQuery query =
        startStream(
            SparkReadOptions.STREAM_FROM_SNAPSHOT, SparkReadOptions.STREAM_FROM_SNAPSHOT_LATEST);

    // With "latest", the backlog must not be visible
    assertThat(rowsAvailable(query)).isEmpty();

    List<SimpleRecord> newRecords =
        Lists.newArrayList(new SimpleRecord(100, "hundred"), new SimpleRecord(101, "hundred-one"));
    appendData(newRecords);

    // New appends after the stream starts should be picked up
    assertThat(rowsAvailable(query)).containsExactlyInAnyOrderElementsOf(newRecords);
  }

  @TestTemplate
  public void testStreamFromSnapshotSpecificIdReadsAfterSnapshot() throws Exception {
    List<SimpleRecord> ignored =
        Lists.newArrayList(new SimpleRecord(-1, "minus-one"), new SimpleRecord(0, "zero"));
    appendData(ignored);
    table.refresh();
    long ignoredSnapshotId = table.currentSnapshot().snapshotId();

    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    StreamingQuery query =
        startStream(SparkReadOptions.STREAM_FROM_SNAPSHOT, Long.toString(ignoredSnapshotId));

    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testStreamFromSnapshotEarliestReplaysHistory() throws Exception {
    // historical default: stream snapshots from the oldest ancestor
    List<List<SimpleRecord>> existing = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(existing);

    StreamingQuery query =
        startStream(
            SparkReadOptions.STREAM_FROM_SNAPSHOT, SparkReadOptions.STREAM_FROM_SNAPSHOT_EARLIEST);

    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(existing));
  }

  @TestTemplate
  public void testDefaultModeReadsOverwriteSnapshotAsInitialState() throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);

    List<SimpleRecord> postOverwrite =
        Lists.newArrayList(
            new SimpleRecord(100, "hundred"),
            new SimpleRecord(101, "hundred-one"),
            new SimpleRecord(102, "hundred-two"));
    spark
        .createDataFrame(postOverwrite, SimpleRecord.class)
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("overwrite")
        .save(tableName);

    table.refresh();
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.OVERWRITE);

    // Default mode must read the post-overwrite state only
    StreamingQuery query = startStream();
    assertThat(rowsAvailable(query)).containsExactlyInAnyOrderElementsOf(postOverwrite);
  }

  @TestTemplate
  public void testDefaultModeReadsDeleteSnapshotAsInitialState() throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);

    table.refresh();
    table.updateSpec().removeField("id_bucket").addField(ref("id")).commit();
    table.refresh();
    table.newDelete().deleteFromRowFilter(Expressions.equal("id", 4)).commit();

    table.refresh();
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);

    // Default mode must read the post-delete state only
    StreamingQuery query = startStream();
    List<SimpleRecord> actual = rowsAvailable(query);
    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "one"),
            new SimpleRecord(2, "two"),
            new SimpleRecord(3, "three"),
            new SimpleRecord(5, "five"),
            new SimpleRecord(6, "six"),
            new SimpleRecord(7, "seven"));
    assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
  }

  @TestTemplate
  public void testStreamFromSnapshotAndStreamFromTimestampAreMutuallyExclusive() throws Exception {
    appendData(Lists.newArrayList(new SimpleRecord(1, "one")));

    Map<String, String> options =
        ImmutableMap.of(
            SparkReadOptions.STREAM_FROM_SNAPSHOT,
            SparkReadOptions.STREAM_FROM_SNAPSHOT_LATEST,
            SparkReadOptions.STREAM_FROM_TIMESTAMP,
            Long.toString(System.currentTimeMillis()));

    assertThatThrownBy(() -> newMicroBatchStream(options, "mutex-checkpoint"))
        .hasMessageContaining(SparkReadOptions.STREAM_FROM_SNAPSHOT)
        .hasMessageContaining(SparkReadOptions.STREAM_FROM_TIMESTAMP);
  }

  @TestTemplate
  public void testStreamFromSnapshotInvalidValue() throws Exception {
    appendData(Lists.newArrayList(new SimpleRecord(1, "one")));

    assertThatThrownBy(
            () ->
                newMicroBatchStream(
                    ImmutableMap.of(SparkReadOptions.STREAM_FROM_SNAPSHOT, "not-a-snapshot"),
                    "invalid-value-checkpoint"))
        .hasMessageContaining(SparkReadOptions.STREAM_FROM_SNAPSHOT)
        .hasMessageContaining("not-a-snapshot");
  }

  @TestTemplate
  public void testStreamFromSnapshotIdNotFound() throws Exception {
    appendData(Lists.newArrayList(new SimpleRecord(1, "one")));

    assertThatThrownBy(
            () ->
                newMicroBatchStream(
                    ImmutableMap.of(SparkReadOptions.STREAM_FROM_SNAPSHOT, "12345"),
                    "not-found-checkpoint"))
        .hasMessageContaining("Cannot find snapshot")
        .hasMessageContaining("12345");
  }

  @TestTemplate
  public void testStreamFromSnapshotEqualToCurrentBehavesLikeLatest() throws Exception {
    // stream-from-snapshot=<current_id> must position past the current snapshot's end
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);
    table.refresh();
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    long expectedPosition = MicroBatchUtils.addedFilesCount(table, table.currentSnapshot());

    SparkMicroBatchStream stream =
        newMicroBatchStream(
            ImmutableMap.of(
                SparkReadOptions.STREAM_FROM_SNAPSHOT, Long.toString(currentSnapshotId)),
            "stream-from-current-snapshot-id-checkpoint");
    try {
      StreamingOffset initial = (StreamingOffset) stream.initialOffset();
      assertThat(initial.snapshotId()).isEqualTo(currentSnapshotId);
      assertThat(initial.position()).isEqualTo(expectedPosition);
      assertThat(initial.shouldScanAllFiles()).isFalse();
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  public void testDefaultModeAppliesPositionalDeletesInInitialSnapshot() throws Exception {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "  'format-version' = '2',"
            + "  'write.delete.mode' = 'merge-on-read',"
            + "  'write.update.mode' = 'merge-on-read',"
            + "  'write.merge.mode' = 'merge-on-read'"
            + ")",
        tableName);
    this.table = validationCatalog.loadTable(tableIdent);

    // five rows in one file so Spark's V2 DELETE produces a positional delete rather than rewriting
    List<SimpleRecord> initial =
        Lists.newArrayList(
            new SimpleRecord(1, "one"),
            new SimpleRecord(2, "two"),
            new SimpleRecord(3, "three"),
            new SimpleRecord(4, "four"),
            new SimpleRecord(5, "five"));
    appendData(initial);

    sql("DELETE FROM %s WHERE data = 'one'", tableName);
    table.refresh();
    assertThat(table.currentSnapshot().deleteManifests(table.io())).isNotEmpty();

    List<SimpleRecord> expected = Lists.newArrayList();
    for (SimpleRecord r : initial) {
      if (!"one".equals(r.getData())) {
        expected.add(r);
      }
    }

    StreamingQuery query = startStream();
    assertThat(rowsAvailable(query)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @TestTemplate
  public void testDefaultModeAppliesEqualityDeletesInInitialSnapshot() throws Exception {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "  'format-version' = '2',"
            + "  'write.delete.mode' = 'merge-on-read'"
            + ")",
        tableName);
    this.table = validationCatalog.loadTable(tableIdent);

    List<SimpleRecord> initial =
        Lists.newArrayList(
            new SimpleRecord(1, "one"),
            new SimpleRecord(2, "two"),
            new SimpleRecord(3, "three"),
            new SimpleRecord(4, "four"),
            new SimpleRecord(5, "five"));
    appendData(initial);
    table.refresh();

    // Spark's V2 writer only emits positional deletes; write an equality delete file by hand
    Schema deleteRowSchema = table.schema().select("data");
    Record deleteRecord = GenericRecord.create(deleteRowSchema);
    List<Record> deletes = Lists.newArrayList(deleteRecord.copy("data", "one"));
    File deleteOut = File.createTempFile("eq-delete", ".parquet");
    deleteOut.delete();
    deleteOut.deleteOnExit();
    DeleteFile equalityDelete =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(deleteOut), null, deletes, deleteRowSchema);
    table.newRowDelta().addDeletes(equalityDelete).commit();
    table.refresh();
    assertThat(table.currentSnapshot().deleteManifests(table.io())).isNotEmpty();

    List<SimpleRecord> expected = Lists.newArrayList();
    for (SimpleRecord r : initial) {
      if (!"one".equals(r.getData())) {
        expected.add(r);
      }
    }

    StreamingQuery query = startStream();
    assertThat(rowsAvailable(query)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @TestTemplate
  public void testDefaultModeAppliesDeletionVectorsInInitialSnapshot() throws Exception {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "  'format-version' = '3',"
            + "  'write.delete.mode' = 'merge-on-read',"
            + "  'write.update.mode' = 'merge-on-read',"
            + "  'write.merge.mode' = 'merge-on-read'"
            + ")",
        tableName);
    this.table = validationCatalog.loadTable(tableIdent);

    List<SimpleRecord> initial =
        Lists.newArrayList(
            new SimpleRecord(1, "one"),
            new SimpleRecord(2, "two"),
            new SimpleRecord(3, "three"),
            new SimpleRecord(4, "four"),
            new SimpleRecord(5, "five"));
    appendData(initial);

    sql("DELETE FROM %s WHERE data = 'one'", tableName);
    table.refresh();
    assertThat(table.currentSnapshot().deleteManifests(table.io())).isNotEmpty();

    List<SimpleRecord> expected = Lists.newArrayList();
    for (SimpleRecord r : initial) {
      if (!"one".equals(r.getData())) {
        expected.add(r);
      }
    }

    StreamingQuery query = startStream();
    assertThat(rowsAvailable(query)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @TestTemplate
  public void testDefaultModeAppliesCarriedForwardDeletesOnAppendCurrentSnapshot()
      throws Exception {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "  'format-version' = '2',"
            + "  'write.delete.mode' = 'merge-on-read',"
            + "  'write.update.mode' = 'merge-on-read',"
            + "  'write.merge.mode' = 'merge-on-read'"
            + ")",
        tableName);
    this.table = validationCatalog.loadTable(tableIdent);

    List<SimpleRecord> initial =
        Lists.newArrayList(
            new SimpleRecord(1, "one"),
            new SimpleRecord(2, "two"),
            new SimpleRecord(3, "three"),
            new SimpleRecord(4, "four"),
            new SimpleRecord(5, "five"));
    appendData(initial);

    sql("DELETE FROM %s WHERE data = 'one'", tableName);

    appendData(Lists.newArrayList(new SimpleRecord(100, "hundred")));
    table.refresh();
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.APPEND);
    assertThat(table.currentSnapshot().deleteManifests(table.io())).isNotEmpty();

    List<SimpleRecord> expected = Lists.newArrayList();
    for (SimpleRecord r : initial) {
      if (!"one".equals(r.getData())) {
        expected.add(r);
      }
    }
    expected.add(new SimpleRecord(100, "hundred"));

    StreamingQuery query = startStream();
    assertThat(rowsAvailable(query)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @TestTemplate
  public void testDefaultModeAppliesDeletesAcrossRateLimitedInitialSnapshotBatches()
      throws Exception {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "  'format-version' = '2',"
            + "  'write.delete.mode' = 'merge-on-read',"
            + "  'write.update.mode' = 'merge-on-read',"
            + "  'write.merge.mode' = 'merge-on-read'"
            + ")",
        tableName);
    this.table = validationCatalog.loadTable(tableIdent);

    // three appends → three data files in the initial snapshot; each holds 5 rows so the DELETE
    // produces a row-level delete rather than a file rewrite
    List<SimpleRecord> file1 =
        Lists.newArrayList(
            new SimpleRecord(1, "one"),
            new SimpleRecord(2, "two"),
            new SimpleRecord(3, "three"),
            new SimpleRecord(4, "four"),
            new SimpleRecord(5, "five"));
    List<SimpleRecord> file2 =
        Lists.newArrayList(
            new SimpleRecord(6, "six"),
            new SimpleRecord(7, "seven"),
            new SimpleRecord(8, "eight"),
            new SimpleRecord(9, "nine"),
            new SimpleRecord(10, "ten"));
    List<SimpleRecord> file3 =
        Lists.newArrayList(
            new SimpleRecord(11, "eleven"),
            new SimpleRecord(12, "twelve"),
            new SimpleRecord(13, "thirteen"),
            new SimpleRecord(14, "fourteen"),
            new SimpleRecord(15, "fifteen"));
    appendData(file1);
    appendData(file2);
    appendData(file3);

    sql("DELETE FROM %s WHERE data = 'seven'", tableName);
    table.refresh();
    assertThat(table.currentSnapshot().deleteManifests(table.io())).isNotEmpty();

    List<SimpleRecord> expected = Lists.newArrayList();
    expected.addAll(file1);
    for (SimpleRecord r : file2) {
      if (!"seven".equals(r.getData())) {
        expected.add(r);
      }
    }
    expected.addAll(file3);

    List<SimpleRecord> captured = Collections.synchronizedList(Lists.newArrayList());
    java.util.concurrent.atomic.AtomicInteger batchCount =
        new java.util.concurrent.atomic.AtomicInteger();

    Map<String, String> options = Maps.newHashMap();
    options.put(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1");
    options.put(SparkReadOptions.ASYNC_MICRO_BATCH_PLANNING_ENABLED, async.toString());
    if (async) {
      options.put(SparkReadOptions.STREAMING_SNAPSHOT_POLLING_INTERVAL_MS, "1");
    }

    StreamingQuery query =
        spark
            .readStream()
            .options(options)
            .format("iceberg")
            .load(tableName)
            .writeStream()
            .options(options)
            .trigger(Trigger.AvailableNow())
            .foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>)
                    (dataset, batchId) -> {
                      batchCount.incrementAndGet();
                      captured.addAll(
                          dataset.as(Encoders.bean(SimpleRecord.class)).collectAsList());
                    })
            .start();
    query.processAllAvailable();
    query.awaitTermination(60000);

    assertThat(captured).containsExactlyInAnyOrderElementsOf(expected);
    // rate limiter must split the initial snapshot across batches — proves the delete file
    // survives across multiple planFiles() calls against the cached BatchScan plan
    assertThat(batchCount.get()).isGreaterThan(1);
    stopStreams();
  }

  @TestTemplate
  public void testDefaultModeWithMaxFilesPerBatchSplitsInitialSnapshot() throws Exception {
    // initial data
    appendData(
        Lists.newArrayList(
            new SimpleRecord(1, "one"), new SimpleRecord(2, "two"), new SimpleRecord(3, "three")));
    appendData(Lists.newArrayList(new SimpleRecord(4, "four"), new SimpleRecord(5, "five")));

    // overwrite initial data - rows 1-5 must not appear in the stream
    List<SimpleRecord> postOverwrite =
        Lists.newArrayList(
            new SimpleRecord(100, "hundred"),
            new SimpleRecord(101, "hundred-one"),
            new SimpleRecord(102, "hundred-two"),
            new SimpleRecord(103, "hundred-three"));
    spark
        .createDataFrame(postOverwrite, SimpleRecord.class)
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("overwrite")
        .save(tableName);

    // make current snapshot append with the manifest list still referencing the overwrite's data
    // files
    List<SimpleRecord> postOverwriteAppend =
        Lists.newArrayList(new SimpleRecord(200, "two-hundred"));
    appendData(postOverwriteAppend);

    table.refresh();
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.APPEND);

    List<SimpleRecord> expected = Lists.newArrayList();
    expected.addAll(postOverwrite);
    expected.addAll(postOverwriteAppend);

    List<SimpleRecord> captured = Collections.synchronizedList(Lists.newArrayList());
    java.util.concurrent.atomic.AtomicInteger batchCount =
        new java.util.concurrent.atomic.AtomicInteger();

    Map<String, String> options = Maps.newHashMap();
    options.put(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1");
    options.put(SparkReadOptions.ASYNC_MICRO_BATCH_PLANNING_ENABLED, async.toString());
    if (async) {
      options.put(SparkReadOptions.STREAMING_SNAPSHOT_POLLING_INTERVAL_MS, "1");
    }

    StreamingQuery query =
        spark
            .readStream()
            .options(options)
            .format("iceberg")
            .load(tableName)
            .writeStream()
            .options(options)
            .trigger(Trigger.AvailableNow())
            .foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>)
                    (dataset, batchId) -> {
                      batchCount.incrementAndGet();
                      captured.addAll(
                          dataset.as(Encoders.bean(SimpleRecord.class)).collectAsList());
                    })
            .start();
    query.processAllAvailable();
    query.awaitTermination(60000);

    // only latest state of live rows arrive, nothing from before the overwrite
    assertThat(captured).containsExactlyInAnyOrderElementsOf(expected);
    // rate-limiting should split the initial snapshot read across multiple batches
    assertThat(batchCount.get()).isGreaterThan(1);

    long totalInputRows = 0L;
    for (org.apache.spark.sql.streaming.StreamingQueryProgress progress : query.recentProgress()) {
      totalInputRows += progress.numInputRows();
    }
    assertThat(totalInputRows).isEqualTo((long) expected.size());
    stopStreams();
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeOverwriteErrorsOut() throws Exception {
    // upgrade table to version 2 - to facilitate creation of Snapshot of type OVERWRITE.
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    // fill table with some initial data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots);

    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "one") // id = 1
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteRowSchema);

    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withPath(File.createTempFile("junit", null, temp.toFile()).getPath())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withFormat(FileFormat.PARQUET)
            .build();

    table.newRowDelta().addRows(dataFile).addDeletes(eqDeletes).commit();

    // check pre-condition - that the above Delete file write - actually resulted in snapshot of
    // type OVERWRITE
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.OVERWRITE);

    StreamingQuery query =
        startStream(
            ImmutableMap.of(
                SparkReadOptions.STREAM_FROM_SNAPSHOT,
                SparkReadOptions.STREAM_FROM_SNAPSHOT_EARLIEST));

    assertThatThrownBy(query::processAllAvailable)
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Cannot process overwrite snapshot");
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeRewriteDataFilesIgnoresReplace() throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    makeRewriteDataFiles();
    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1"),
        List.of(1L, 2L, 1L, 1L, 1L, 1L));
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeRewriteDataFilesIgnoresReplaceMaxRows()
      throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    makeRewriteDataFiles();
    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "4"), List.of(4L, 3L));
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeRewriteDataFilesIgnoresReplaceMaxFilesAndRows()
      throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    makeRewriteDataFiles();
    assertMicroBatchRecordSizes(
        ImmutableMap.of(
            SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH,
            "4",
            SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH,
            "1"),
        List.of(1L, 2L, 1L, 1L, 1L, 1L));
  }

  @TestTemplate
  public void testReadStreamWithSnapshotType2RewriteDataFilesIgnoresReplace() throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    makeRewriteDataFiles();
    makeRewriteDataFiles();
    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1"),
        List.of(1L, 2L, 1L, 1L, 1L, 1L));
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeRewriteDataFilesIgnoresReplaceFollowedByAppend()
      throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    makeRewriteDataFiles();

    appendDataAsMultipleSnapshots(expected);
    assertMicroBatchRecordSizes(
        ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1"),
        List.of(1L, 2L, 1L, 1L, 1L, 1L, 1L, 2L, 1L, 1L, 1L, 1L));
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeReplaceIgnoresReplace() throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    // this should create a snapshot with type Replace.
    table.rewriteManifests().clusterBy(f -> 1).commit();

    // check pre-condition
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.REPLACE);

    StreamingQuery query = startStream();
    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeDeleteErrorsOut() throws Exception {
    table.updateSpec().removeField("id_bucket").addField(ref("id")).commit();

    // fill table with some data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots);

    // this should create a snapshot with type delete.
    table.newDelete().deleteFromRowFilter(Expressions.equal("id", 4)).commit();

    // check pre-condition - that the above delete operation on table resulted in Snapshot of Type
    // DELETE.
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);

    StreamingQuery query =
        startStream(
            ImmutableMap.of(
                SparkReadOptions.STREAM_FROM_SNAPSHOT,
                SparkReadOptions.STREAM_FROM_SNAPSHOT_EARLIEST));

    assertThatThrownBy(query::processAllAvailable)
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Cannot process delete snapshot");
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeDeleteAndSkipDeleteOption() throws Exception {
    table.updateSpec().removeField("id_bucket").addField(ref("id")).commit();

    // fill table with some data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots);

    // this should create a snapshot with type delete.
    table.newDelete().deleteFromRowFilter(Expressions.equal("id", 4)).commit();

    // check pre-condition - that the above delete operation on table resulted in Snapshot of Type
    // DELETE.
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);

    StreamingQuery query =
        startStream(
            ImmutableMap.of(
                SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS,
                "true",
                SparkReadOptions.STREAM_FROM_SNAPSHOT,
                SparkReadOptions.STREAM_FROM_SNAPSHOT_EARLIEST));
    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(dataAcrossSnapshots));
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeDeleteAndSkipOverwriteOption() throws Exception {
    table.updateSpec().removeField("id_bucket").addField(ref("id")).commit();

    // fill table with some data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots);

    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withPath(File.createTempFile("junit", null, temp.toFile()).getPath())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withFormat(FileFormat.PARQUET)
            .build();

    // this should create a snapshot with type overwrite.
    table
        .newOverwrite()
        .addFile(dataFile)
        .overwriteByRowFilter(Expressions.greaterThan("id", 4))
        .commit();

    // check pre-condition - that the above delete operation on table resulted in Snapshot of Type
    // OVERWRITE.
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.OVERWRITE);

    StreamingQuery query =
        startStream(
            ImmutableMap.of(
                SparkReadOptions.STREAMING_SKIP_OVERWRITE_SNAPSHOTS,
                "true",
                SparkReadOptions.STREAM_FROM_SNAPSHOT,
                SparkReadOptions.STREAM_FROM_SNAPSHOT_EARLIEST));
    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(dataAcrossSnapshots));
  }

  /**
   * We are testing that all the files in a rewrite snapshot are skipped Create a rewrite data files
   * snapshot using existing files.
   */
  public void makeRewriteDataFiles() {
    table.refresh();

    // we are testing that all the files in a rewrite snapshot are skipped
    // create a rewrite data files snapshot using existing files
    RewriteFiles rewrite = table.newRewrite();
    Iterable<Snapshot> it = table.snapshots();
    for (Snapshot snapshot : it) {
      if (snapshot.operation().equals(DataOperations.APPEND)) {
        Iterable<DataFile> datafiles =
            SnapshotChanges.builderFor(table).snapshot(snapshot).build().addedDataFiles();
        for (DataFile datafile : datafiles) {
          rewrite.addFile(datafile);
          rewrite.deleteFile(datafile);
        }
      }
    }
    rewrite.commit();
  }

  /**
   * appends each list as a Snapshot on the iceberg table at the given location. accepts a list of
   * lists - each list representing data per snapshot.
   */
  private void appendDataAsMultipleSnapshots(List<List<SimpleRecord>> data) {
    for (List<SimpleRecord> l : data) {
      appendData(l);
    }
  }

  private void appendData(List<SimpleRecord> data) {
    appendData(data, "parquet");
  }

  private void appendData(List<SimpleRecord> data, String format) {
    Dataset<Row> df = spark.createDataFrame(data, SimpleRecord.class);
    df.select("id", "data")
        .write()
        .format("iceberg")
        .option("write-format", format)
        .mode("append")
        .save(tableName);
  }

  private static final String MEMORY_TABLE = "_stream_view_mem";

  private StreamingQuery startStream(Map<String, String> options) throws TimeoutException {
    Map<String, String> allOptions = Maps.newHashMap(options);
    allOptions.put(SparkReadOptions.ASYNC_MICRO_BATCH_PLANNING_ENABLED, async.toString());
    if (async) {
      allOptions.putIfAbsent(SparkReadOptions.STREAMING_SNAPSHOT_POLLING_INTERVAL_MS, "1");
    }
    return spark
        .readStream()
        .options(allOptions)
        .format("iceberg")
        .load(tableName)
        .writeStream()
        .options(allOptions)
        .format("memory")
        .queryName(MEMORY_TABLE)
        .outputMode(OutputMode.Append())
        .start();
  }

  private StreamingQuery startStream() throws TimeoutException {
    return startStream(Collections.emptyMap());
  }

  private StreamingQuery startStream(String key, String value) throws TimeoutException {
    return startStream(
        ImmutableMap.of(key, value, SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1"));
  }

  private void assertMicroBatchRecordSizes(
      Map<String, String> options, List<Long> expectedMicroBatchRecordSize)
      throws TimeoutException {
    assertMicroBatchRecordSizes(options, expectedMicroBatchRecordSize, Trigger.ProcessingTime(0L));
  }

  private void assertMicroBatchRecordSizes(
      Map<String, String> options, List<Long> expectedMicroBatchRecordSize, Trigger trigger)
      throws TimeoutException {
    Map<String, String> allOptions = Maps.newHashMap(options);
    allOptions.put(SparkReadOptions.ASYNC_MICRO_BATCH_PLANNING_ENABLED, async.toString());
    if (async) {
      allOptions.putIfAbsent(SparkReadOptions.STREAMING_SNAPSHOT_POLLING_INTERVAL_MS, "1");
    }

    Dataset<Row> ds = spark.readStream().options(allOptions).format("iceberg").load(tableName);

    List<Long> syncList = Collections.synchronizedList(Lists.newArrayList());
    ds.writeStream()
        .options(allOptions)
        .trigger(trigger)
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (dataset, batchId) -> {
                  microBatches.getAndIncrement();
                  syncList.add(dataset.count());
                })
        .start()
        .processAllAvailable();

    stopStreams();
    assertThat(syncList).containsExactlyInAnyOrderElementsOf(expectedMicroBatchRecordSize);
  }

  private List<SimpleRecord> rowsAvailable(StreamingQuery query) {
    query.processAllAvailable();
    return spark
        .sql("select * from " + MEMORY_TABLE)
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
  }

  private SparkMicroBatchStream newMicroBatchStream(
      Map<String, String> options, String checkpointDirName) {
    Map<String, String> allOptions = Maps.newHashMap(options);
    allOptions.put(SparkReadOptions.ASYNC_MICRO_BATCH_PLANNING_ENABLED, async.toString());
    if (async) {
      allOptions.putIfAbsent(SparkReadOptions.STREAMING_SNAPSHOT_POLLING_INTERVAL_MS, "1");
    }

    return new SparkMicroBatchStream(
        JavaSparkContext.fromSparkContext(spark.sparkContext()),
        table,
        table::io,
        new SparkReadConf(spark, table, allOptions),
        table.schema(),
        temp.resolve(checkpointDirName).toString());
  }
}
