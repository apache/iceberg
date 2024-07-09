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
package org.apache.iceberg.flink.source.enumerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class TestContinuousSplitPlannerImplStartStrategy {
  private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;

  @TempDir protected Path temporaryFolder;

  @RegisterExtension
  private static final HadoopTableExtension TABLE_RESOURCE =
      new HadoopTableExtension(TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  private GenericAppenderHelper dataAppender;
  private Snapshot snapshot1;
  private Snapshot snapshot2;
  private Snapshot snapshot3;

  @BeforeEach
  public void before() throws IOException {
    dataAppender = new GenericAppenderHelper(TABLE_RESOURCE.table(), FILE_FORMAT, temporaryFolder);
  }

  private void appendThreeSnapshots() throws IOException {
    List<Record> batch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(batch1);
    snapshot1 = TABLE_RESOURCE.table().currentSnapshot();

    List<Record> batch2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 1L);
    dataAppender.appendToTable(batch2);
    snapshot2 = TABLE_RESOURCE.table().currentSnapshot();

    List<Record> batch3 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 2L);
    dataAppender.appendToTable(batch3);
    snapshot3 = TABLE_RESOURCE.table().currentSnapshot();
  }

  @Test
  public void testTableScanThenIncrementalStrategy() throws IOException {
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();

    assertThat(ContinuousSplitPlannerImpl.startSnapshot(TABLE_RESOURCE.table(), scanContext))
        .as("empty table")
        .isNotPresent();

    appendThreeSnapshots();
    Snapshot startSnapshot =
        ContinuousSplitPlannerImpl.startSnapshot(TABLE_RESOURCE.table(), scanContext).get();
    assertThat(startSnapshot.snapshotId()).isEqualTo(snapshot3.snapshotId());
  }

  @Test
  public void testForLatestSnapshotStrategy() throws IOException {
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
            .build();

    assertThat(ContinuousSplitPlannerImpl.startSnapshot(TABLE_RESOURCE.table(), scanContext))
        .as("empty table")
        .isNotPresent();

    appendThreeSnapshots();
    Snapshot startSnapshot =
        ContinuousSplitPlannerImpl.startSnapshot(TABLE_RESOURCE.table(), scanContext).get();
    assertThat(startSnapshot.snapshotId()).isEqualTo(snapshot3.snapshotId());
  }

  @Test
  public void testForEarliestSnapshotStrategy() throws IOException {
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT)
            .build();

    assertThat(ContinuousSplitPlannerImpl.startSnapshot(TABLE_RESOURCE.table(), scanContext))
        .as("empty table")
        .isNotPresent();

    appendThreeSnapshots();
    Snapshot startSnapshot =
        ContinuousSplitPlannerImpl.startSnapshot(TABLE_RESOURCE.table(), scanContext).get();
    assertThat(startSnapshot.snapshotId()).isEqualTo(snapshot1.snapshotId());
  }

  @Test
  public void testForSpecificSnapshotIdStrategy() throws IOException {
    ScanContext scanContextInvalidSnapshotId =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
            .startSnapshotId(1L)
            .build();

    assertThatThrownBy(
            () ->
                ContinuousSplitPlannerImpl.startSnapshot(
                    TABLE_RESOURCE.table(), scanContextInvalidSnapshotId))
        .as("empty table")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Start snapshot id not found in history: 1");

    appendThreeSnapshots();

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
            .startSnapshotId(snapshot2.snapshotId())
            .build();

    Snapshot startSnapshot =
        ContinuousSplitPlannerImpl.startSnapshot(TABLE_RESOURCE.table(), scanContext).get();
    assertThat(startSnapshot.snapshotId()).isEqualTo(snapshot2.snapshotId());
  }

  @Test
  public void testForSpecificSnapshotTimestampStrategySnapshot2() throws IOException {
    ScanContext scanContextInvalidSnapshotTimestamp =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .startSnapshotTimestamp(1L)
            .build();

    assertThatThrownBy(
            () ->
                ContinuousSplitPlannerImpl.startSnapshot(
                    TABLE_RESOURCE.table(), scanContextInvalidSnapshotTimestamp))
        .as("empty table")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot find a snapshot after: ");

    appendThreeSnapshots();

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .startSnapshotTimestamp(snapshot2.timestampMillis())
            .build();

    Snapshot startSnapshot =
        ContinuousSplitPlannerImpl.startSnapshot(TABLE_RESOURCE.table(), scanContext).get();
    assertThat(startSnapshot.snapshotId()).isEqualTo(snapshot2.snapshotId());
  }

  @Test
  public void testForSpecificSnapshotTimestampStrategySnapshot2Minus1() throws IOException {
    appendThreeSnapshots();

    ScanContext config =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .startSnapshotTimestamp(snapshot2.timestampMillis() - 1L)
            .build();

    Snapshot startSnapshot =
        ContinuousSplitPlannerImpl.startSnapshot(TABLE_RESOURCE.table(), config).get();
    assertThat(startSnapshot.snapshotId()).isEqualTo(snapshot2.snapshotId());
  }
}
