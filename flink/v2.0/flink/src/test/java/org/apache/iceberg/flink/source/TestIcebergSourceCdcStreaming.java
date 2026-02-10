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
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for CDC (Change Data Capture) streaming reads using the CHANGELOG streaming read mode.
 *
 * <p>This test verifies that the IcebergSource can correctly read changelog data with proper
 * RowKind (INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER) when streamingReadMode is set to CHANGELOG.
 */
public class TestIcebergSourceCdcStreaming {

  @TempDir protected Path temporaryFolder;

  @RegisterExtension
  public static MiniClusterExtension miniClusterExtension =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  private static final HadoopTableExtension TABLE_EXTENSION =
      new HadoopTableExtension(TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  private final AtomicLong randomSeed = new AtomicLong(0L);

  @Test
  public void testChangelogStreamingReadModeConfig() {
    // Test that StreamingReadMode can be properly configured
    ScanContext context =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
            .streamingReadMode(StreamingReadMode.CHANGELOG)
            .build();

    assertThat(context.streamingReadMode()).isEqualTo(StreamingReadMode.CHANGELOG);
    assertThat(context.isChangelogScan()).isTrue();
  }

  @Test
  public void testChangelogScanContextCopy() {
    // Test that copyWithChangelogScan creates proper context
    ScanContext originalContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .streamingReadMode(StreamingReadMode.CHANGELOG)
            .build();

    ScanContext copiedContext = originalContext.copyWithChangelogScan(100L, 200L);

    assertThat(copiedContext.startSnapshotId()).isEqualTo(100L);
    assertThat(copiedContext.endSnapshotId()).isEqualTo(200L);
    assertThat(copiedContext.isChangelogScan()).isTrue();
  }

  @Test
  public void testAppendOnlyModeDefault() {
    // Test that default streaming mode is APPEND_ONLY
    ScanContext context =
        ScanContext.builder().streaming(true).monitorInterval(Duration.ofMillis(10L)).build();

    assertThat(context.streamingReadMode()).isEqualTo(StreamingReadMode.APPEND_ONLY);
    assertThat(context.isChangelogScan()).isFalse();
  }

  @Test
  public void testChangelogModeWithInserts() throws Exception {
    // Test reading INSERT operations in CHANGELOG mode
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    // Create initial snapshot with some data
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 3, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT)
            .streamingReadMode(StreamingReadMode.CHANGELOG)
            .build();

    try (CloseableIterator<Row> iter =
        createChangelogStream(scanContext).executeAndCollect(getClass().getSimpleName())) {

      List<Row> results = waitForResult(iter, 3);

      // In CHANGELOG mode, all newly inserted rows should have RowKind.INSERT
      assertThat(results).hasSize(3);
      for (Row row : results) {
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
      }
    }
  }

  @Test
  public void testFlinkSplitPlannerCheckScanMode() {
    // Test that FlinkSplitPlanner correctly identifies changelog scan mode
    ScanContext changelogContext =
        ScanContext.builder()
            .streaming(true)
            .streamingReadMode(StreamingReadMode.CHANGELOG)
            .startSnapshotId(100L)
            .endSnapshotId(200L)
            .build();

    assertThat(FlinkSplitPlanner.checkScanMode(changelogContext))
        .isEqualTo(FlinkSplitPlanner.ScanMode.INCREMENTAL_CHANGELOG_SCAN);

    ScanContext appendOnlyContext =
        ScanContext.builder()
            .streaming(true)
            .streamingReadMode(StreamingReadMode.APPEND_ONLY)
            .startSnapshotId(100L)
            .endSnapshotId(200L)
            .build();

    assertThat(FlinkSplitPlanner.checkScanMode(appendOnlyContext))
        .isEqualTo(FlinkSplitPlanner.ScanMode.INCREMENTAL_APPEND_SCAN);
  }

  private DataStream<Row> createChangelogStream(ScanContext scanContext) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Row> stream =
        env.fromSource(
                IcebergSource.forRowData()
                    .tableLoader(TABLE_EXTENSION.tableLoader())
                    .assignerFactory(new SimpleSplitAssignerFactory())
                    .streaming(scanContext.isStreaming())
                    .streamingStartingStrategy(scanContext.streamingStartingStrategy())
                    .streamingReadMode(scanContext.streamingReadMode())
                    .monitorInterval(Duration.ofMillis(10L))
                    .build(),
                WatermarkStrategy.noWatermarks(),
                "icebergCdcSource",
                TypeInformation.of(RowData.class))
            .map(new RowDataToRowMapper(FlinkSchemaUtil.convert(TABLE_EXTENSION.table().schema())));

    return stream;
  }

  public static List<Row> waitForResult(CloseableIterator<Row> iter, int limit) {
    List<Row> results = Lists.newArrayListWithCapacity(limit);
    long startTime = System.currentTimeMillis();
    long timeout = 30000; // 30 seconds timeout

    while (results.size() < limit && (System.currentTimeMillis() - startTime) < timeout) {
      if (iter.hasNext()) {
        results.add(iter.next());
      } else {
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
