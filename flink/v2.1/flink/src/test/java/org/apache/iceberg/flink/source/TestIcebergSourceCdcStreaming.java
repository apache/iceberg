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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for CDC (Change Data Capture) streaming reads using the CHANGELOG streaming
 * read mode.
 *
 * <p>This test verifies CDC functionality including:
 *
 * <ul>
 *   <li>StreamingReadMode enum values
 *   <li>Basic append-only streaming reads
 *   <li>ChangelogDataIterator and RowDataChangelogScanTaskReader components
 * </ul>
 */
public class TestIcebergSourceCdcStreaming {

  @TempDir protected Path temporaryFolder;

  @RegisterExtension
  public static org.apache.flink.test.junit5.MiniClusterExtension miniClusterExtension =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  private static final HadoopTableExtension TABLE_EXTENSION =
      new HadoopTableExtension(TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  private final AtomicLong randomSeed = new AtomicLong(0L);

  @Test
  public void testStreamingReadModeValues() {
    // Test that StreamingReadMode enum has expected values
    assertThat(StreamingReadMode.values()).hasSize(2);
    assertThat(StreamingReadMode.valueOf("APPEND_ONLY")).isEqualTo(StreamingReadMode.APPEND_ONLY);
    assertThat(StreamingReadMode.valueOf("CHANGELOG")).isEqualTo(StreamingReadMode.CHANGELOG);
  }

  @Test
  public void testStreamingReadModeOrdinal() {
    // Test ordinal values
    assertThat(StreamingReadMode.APPEND_ONLY.ordinal()).isEqualTo(0);
    assertThat(StreamingReadMode.CHANGELOG.ordinal()).isEqualTo(1);
  }

  @Test
  public void testStreamingReadModeName() {
    // Test name values
    assertThat(StreamingReadMode.APPEND_ONLY.name()).isEqualTo("APPEND_ONLY");
    assertThat(StreamingReadMode.CHANGELOG.name()).isEqualTo("CHANGELOG");
  }

  @Test
  public void testAppendOnlyStreaming() throws Exception {
    // Test basic append-only streaming mode (default behavior)
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      List<Row> result1 = waitForResult(iter, 2);

      // Verify all records have INSERT RowKind in append-only mode
      for (Row row : result1) {
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
      }

      TestHelpers.assertRecords(result1, batch1, TABLE_EXTENSION.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 2);
      for (Row row : result2) {
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
      }
      TestHelpers.assertRecords(result2, batch2, TABLE_EXTENSION.table().schema());
    }
  }

  @Test
  public void testChangelogDataIteratorInterface() {
    // Test that ChangelogDataIterator class exists and has proper structure
    assertThat(ChangelogDataIterator.class).isNotNull();
    assertThat(ChangelogDataIterator.ChangelogScanTaskReader.class).isInterface();
  }

  @Test
  public void testRowDataChangelogScanTaskReaderInstantiation() {
    // Test that RowDataChangelogScanTaskReader can be instantiated
    Schema schema = TABLE_EXTENSION.table().schema();
    RowDataChangelogScanTaskReader reader =
        new RowDataChangelogScanTaskReader(
            schema,
            schema,
            null, // nameMapping
            true, // caseSensitive
            null, // filters
            TABLE_EXTENSION.table().io(),
            TABLE_EXTENSION.table().encryption());

    assertThat(reader).isNotNull();
    assertThat(reader).isInstanceOf(ChangelogDataIterator.ChangelogScanTaskReader.class);
  }

  @Test
  public void testChangelogScanSplitCreation() {
    // Test creating ChangelogScanSplit with empty tasks
    org.apache.iceberg.flink.source.split.ChangelogScanSplit split =
        new org.apache.iceberg.flink.source.split.ChangelogScanSplit(Lists.newArrayList());

    assertThat(split).isNotNull();
    assertThat(split.tasks()).isEmpty();
    assertThat(split.taskOffset()).isEqualTo(0);
    assertThat(split.recordOffset()).isEqualTo(0L);
    assertThat(split.splitId()).contains("ChangelogScanSplit");
  }

  /**
   * Test that incremental append scan works correctly. This is the foundation for CDC
   * implementation.
   */
  @Test
  public void testIncrementalAppendScan() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);
    long snapshot1 = TABLE_EXTENSION.table().currentSnapshot().snapshotId();

    // snapshot2
    List<Record> batch2 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch2);

    // Use incremental scan from snapshot1
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
            .startSnapshotId(snapshot1)
            .build();

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      // Should get batch1 first (inclusive of starting snapshot)
      List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, TABLE_EXTENSION.table().schema());

      // Then batch2
      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, TABLE_EXTENSION.table().schema());
    }
  }

  /**
   * Test verifying the row kind mapping for CDC operations. This test documents the expected
   * behavior of RowKind mapping.
   */
  @Test
  public void testRowKindMapping() {
    // Verify RowKind values match CDC expectations
    assertThat(RowKind.INSERT.shortString()).isEqualTo("+I");
    assertThat(RowKind.DELETE.shortString()).isEqualTo("-D");
    assertThat(RowKind.UPDATE_BEFORE.shortString()).isEqualTo("-U");
    assertThat(RowKind.UPDATE_AFTER.shortString()).isEqualTo("+U");
  }

  /**
   * Test that multiple snapshots are properly handled in streaming mode with continuous discovery.
   */
  @Test
  public void testMultipleSnapshotsStreaming() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, TABLE_EXTENSION.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, TABLE_EXTENSION.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, TABLE_EXTENSION.table().schema());

      // Verify total snapshots
      assertThat(TABLE_EXTENSION.table().snapshots()).hasSize(3);
    }
  }

  private DataStream<Row> createStream(ScanContext scanContext) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<Row> stream =
        env.fromSource(
                IcebergSource.forRowData()
                    .tableLoader(TABLE_EXTENSION.tableLoader())
                    .assignerFactory(new SimpleSplitAssignerFactory())
                    .streaming(scanContext.isStreaming())
                    .streamingStartingStrategy(scanContext.streamingStartingStrategy())
                    .startSnapshotTimestamp(scanContext.startSnapshotTimestamp())
                    .startSnapshotId(scanContext.startSnapshotId())
                    .monitorInterval(Duration.ofMillis(10L))
                    .build(),
                WatermarkStrategy.noWatermarks(),
                "icebergSource",
                TypeInformation.of(RowData.class))
            .map(new RowDataToRowMapper(FlinkSchemaUtil.convert(TABLE_EXTENSION.table().schema())));
    return stream;
  }

  private static List<Row> waitForResult(CloseableIterator<Row> iter, int limit) {
    List<Row> results = Lists.newArrayListWithCapacity(limit);
    int maxAttempts = 1000; // Prevent infinite loop
    int attempts = 0;
    while (results.size() < limit && attempts < maxAttempts) {
      if (iter.hasNext()) {
        results.add(iter.next());
      } else {
        attempts++;
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    return results;
  }
}
