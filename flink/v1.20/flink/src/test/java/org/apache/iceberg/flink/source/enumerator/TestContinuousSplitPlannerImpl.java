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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class TestContinuousSplitPlannerImpl {
  @TempDir protected Path temporaryFolder;

  private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;
  private static final AtomicLong RANDOM_SEED = new AtomicLong();

  @RegisterExtension
  private static final HadoopTableExtension TABLE_RESOURCE =
      new HadoopTableExtension(TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  private GenericAppenderHelper dataAppender;
  private DataFile dataFile1;
  private Snapshot snapshot1;
  private DataFile dataFile2;
  private Snapshot snapshot2;

  @BeforeEach
  public void before() throws IOException {
    dataAppender = new GenericAppenderHelper(TABLE_RESOURCE.table(), FILE_FORMAT, temporaryFolder);
  }

  private void appendTwoSnapshots() throws IOException {
    // snapshot1
    List<Record> batch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataFile1 = dataAppender.writeFile(null, batch1);
    dataAppender.appendToTable(dataFile1);
    snapshot1 = TABLE_RESOURCE.table().currentSnapshot();

    // snapshot2
    List<Record> batch2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 1L);
    dataFile2 = dataAppender.writeFile(null, batch2);
    dataAppender.appendToTable(dataFile2);
    snapshot2 = TABLE_RESOURCE.table().currentSnapshot();
  }

  /**
   * @return the last enumerated snapshot id
   */
  private CycleResult verifyOneCycle(
      ContinuousSplitPlannerImpl splitPlanner, IcebergEnumeratorPosition lastPosition)
      throws Exception {
    List<Record> batch =
        RandomGenericData.generate(TestFixtures.SCHEMA, 2, RANDOM_SEED.incrementAndGet());
    DataFile dataFile = dataAppender.writeFile(null, batch);
    dataAppender.appendToTable(dataFile);
    Snapshot snapshot = TABLE_RESOURCE.table().currentSnapshot();

    ContinuousEnumerationResult result = splitPlanner.planSplits(lastPosition);
    assertThat(result.fromPosition().snapshotId()).isEqualTo(lastPosition.snapshotId());
    assertThat(result.fromPosition().snapshotTimestampMs())
        .isEqualTo(lastPosition.snapshotTimestampMs());
    assertThat(result.toPosition().snapshotId().longValue()).isEqualTo(snapshot.snapshotId());
    assertThat(result.toPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot.timestampMillis());
    assertThat(result.splits()).hasSize(1);
    IcebergSourceSplit split = Iterables.getOnlyElement(result.splits());
    assertThat(split.task().files())
        .hasSize(1)
        .first()
        .satisfies(
            fileScanTask -> assertThat(fileScanTask.file().path()).isEqualTo(dataFile.path()));
    return new CycleResult(result.toPosition(), split);
  }

  @Test
  public void testTableScanThenIncrementalWithEmptyTable() throws Exception {
    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult emptyTableInitialDiscoveryResult = splitPlanner.planSplits(null);
    assertThat(emptyTableInitialDiscoveryResult.splits()).isEmpty();
    assertThat(emptyTableInitialDiscoveryResult.fromPosition()).isNull();
    assertThat(emptyTableInitialDiscoveryResult.toPosition().isEmpty()).isTrue();
    assertThat(emptyTableInitialDiscoveryResult.toPosition().snapshotTimestampMs()).isNull();

    ContinuousEnumerationResult emptyTableSecondDiscoveryResult =
        splitPlanner.planSplits(emptyTableInitialDiscoveryResult.toPosition());
    assertThat(emptyTableSecondDiscoveryResult.splits()).isEmpty();
    assertThat(emptyTableSecondDiscoveryResult.fromPosition().isEmpty()).isTrue();
    assertThat(emptyTableSecondDiscoveryResult.fromPosition().snapshotTimestampMs()).isNull();
    assertThat(emptyTableSecondDiscoveryResult.toPosition().isEmpty()).isTrue();
    assertThat(emptyTableSecondDiscoveryResult.toPosition().snapshotTimestampMs()).isNull();

    // next 3 snapshots
    IcebergEnumeratorPosition lastPosition = emptyTableSecondDiscoveryResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition).lastPosition;
    }
  }

  @Test
  public void testTableScanThenIncrementalWithNonEmptyTable() throws Exception {
    appendTwoSnapshots();

    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    assertThat(initialResult.fromPosition()).isNull();
    assertThat(initialResult.toPosition().snapshotId().longValue())
        .isEqualTo(snapshot2.snapshotId());
    assertThat(initialResult.toPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot2.timestampMillis());
    assertThat(initialResult.splits()).hasSize(1);
    IcebergSourceSplit split = Iterables.getOnlyElement(initialResult.splits());
    assertThat(split.task().files()).hasSize(2);
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    Set<String> expectedFiles =
        ImmutableSet.of(dataFile1.path().toString(), dataFile2.path().toString());
    assertThat(discoveredFiles).containsExactlyInAnyOrderElementsOf(expectedFiles);

    IcebergEnumeratorPosition lastPosition = initialResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition).lastPosition;
    }
  }

  @Test
  public void testIncrementalFromLatestSnapshotWithEmptyTable() throws Exception {
    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
            .splitSize(1L)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult emptyTableInitialDiscoveryResult = splitPlanner.planSplits(null);
    assertThat(emptyTableInitialDiscoveryResult.splits()).isEmpty();
    assertThat(emptyTableInitialDiscoveryResult.fromPosition()).isNull();
    assertThat(emptyTableInitialDiscoveryResult.toPosition().isEmpty()).isTrue();
    assertThat(emptyTableInitialDiscoveryResult.toPosition().snapshotTimestampMs()).isNull();

    ContinuousEnumerationResult emptyTableSecondDiscoveryResult =
        splitPlanner.planSplits(emptyTableInitialDiscoveryResult.toPosition());
    assertThat(emptyTableSecondDiscoveryResult.splits()).isEmpty();
    assertThat(emptyTableSecondDiscoveryResult.fromPosition().isEmpty()).isTrue();
    assertThat(emptyTableSecondDiscoveryResult.fromPosition().snapshotTimestampMs()).isNull();
    assertThat(emptyTableSecondDiscoveryResult.toPosition().isEmpty()).isTrue();
    assertThat(emptyTableSecondDiscoveryResult.toPosition().snapshotTimestampMs()).isNull();

    // latest mode should discover both snapshots, as latest position is marked by when job starts
    appendTwoSnapshots();
    ContinuousEnumerationResult afterTwoSnapshotsAppended =
        splitPlanner.planSplits(emptyTableSecondDiscoveryResult.toPosition());
    assertThat(afterTwoSnapshotsAppended.splits()).hasSize(2);

    // next 3 snapshots
    IcebergEnumeratorPosition lastPosition = afterTwoSnapshotsAppended.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition).lastPosition;
    }
  }

  @Test
  public void testIncrementalFromLatestSnapshotWithNonEmptyTable() throws Exception {
    appendTwoSnapshots();

    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    assertThat(initialResult.fromPosition()).isNull();
    // For inclusive behavior, the initial result should point to snapshot1
    // Then the next incremental scan shall discover files from latest snapshot2 (inclusive)
    assertThat(initialResult.toPosition().snapshotId().longValue())
        .isEqualTo(snapshot1.snapshotId());
    assertThat(initialResult.toPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot1.timestampMillis());
    assertThat(initialResult.splits()).isEmpty();

    ContinuousEnumerationResult secondResult = splitPlanner.planSplits(initialResult.toPosition());
    assertThat(secondResult.fromPosition().snapshotId().longValue())
        .isEqualTo(snapshot1.snapshotId());
    assertThat(secondResult.fromPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot1.timestampMillis());
    assertThat(secondResult.toPosition().snapshotId().longValue())
        .isEqualTo(snapshot2.snapshotId());
    assertThat(secondResult.toPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot2.timestampMillis());
    IcebergSourceSplit split = Iterables.getOnlyElement(secondResult.splits());
    assertThat(split.task().files()).hasSize(1);
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    // should discover dataFile2 appended in snapshot2
    Set<String> expectedFiles = ImmutableSet.of(dataFile2.path().toString());
    assertThat(discoveredFiles).containsExactlyElementsOf(expectedFiles);

    IcebergEnumeratorPosition lastPosition = secondResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition).lastPosition;
    }
  }

  @Test
  public void testIncrementalFromEarliestSnapshotWithEmptyTable() throws Exception {
    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult emptyTableInitialDiscoveryResult = splitPlanner.planSplits(null);
    assertThat(emptyTableInitialDiscoveryResult.splits()).isEmpty();
    assertThat(emptyTableInitialDiscoveryResult.fromPosition()).isNull();
    assertThat(emptyTableInitialDiscoveryResult.toPosition().snapshotId()).isNull();
    assertThat(emptyTableInitialDiscoveryResult.toPosition().snapshotTimestampMs()).isNull();

    ContinuousEnumerationResult emptyTableSecondDiscoveryResult =
        splitPlanner.planSplits(emptyTableInitialDiscoveryResult.toPosition());
    assertThat(emptyTableSecondDiscoveryResult.splits()).isEmpty();
    assertThat(emptyTableSecondDiscoveryResult.fromPosition().snapshotId()).isNull();
    assertThat(emptyTableSecondDiscoveryResult.fromPosition().snapshotTimestampMs()).isNull();
    assertThat(emptyTableSecondDiscoveryResult.toPosition().snapshotId()).isNull();
    assertThat(emptyTableSecondDiscoveryResult.toPosition().snapshotTimestampMs()).isNull();

    // next 3 snapshots
    IcebergEnumeratorPosition lastPosition = emptyTableSecondDiscoveryResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition).lastPosition;
    }
  }

  @Test
  public void testIncrementalFromEarliestSnapshotWithNonEmptyTable() throws Exception {
    appendTwoSnapshots();

    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    assertThat(initialResult.fromPosition()).isNull();
    // For inclusive behavior, the initial result should point to snapshot1's parent,
    // which leads to null snapshotId and snapshotTimestampMs.
    assertThat(initialResult.toPosition().snapshotId()).isNull();
    assertThat(initialResult.toPosition().snapshotTimestampMs()).isNull();
    assertThat(initialResult.splits()).isEmpty();

    ContinuousEnumerationResult secondResult = splitPlanner.planSplits(initialResult.toPosition());
    assertThat(secondResult.fromPosition().snapshotId()).isNull();
    assertThat(secondResult.fromPosition().snapshotTimestampMs()).isNull();
    assertThat(secondResult.toPosition().snapshotId().longValue())
        .isEqualTo(snapshot2.snapshotId());
    assertThat(secondResult.toPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot2.timestampMillis());
    IcebergSourceSplit split = Iterables.getOnlyElement(secondResult.splits());
    assertThat(split.task().files()).hasSize(2);
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    // should discover files appended in both snapshot1 and snapshot2
    Set<String> expectedFiles =
        ImmutableSet.of(dataFile1.path().toString(), dataFile2.path().toString());
    assertThat(discoveredFiles).containsExactlyInAnyOrderElementsOf(expectedFiles);

    IcebergEnumeratorPosition lastPosition = secondResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition).lastPosition;
    }
  }

  @Test
  public void testIncrementalFromSnapshotIdWithEmptyTable() {
    ScanContext scanContextWithInvalidSnapshotId =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
            .startSnapshotId(1L)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(
            TABLE_RESOURCE.tableLoader().clone(), scanContextWithInvalidSnapshotId, null);

    assertThatThrownBy(() -> splitPlanner.planSplits(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Start snapshot id not found in history: 1");
  }

  @Test
  public void testIncrementalFromSnapshotIdWithInvalidIds() throws Exception {
    appendTwoSnapshots();

    // find an invalid snapshotId
    long invalidSnapshotId = 0L;
    while (invalidSnapshotId == snapshot1.snapshotId()
        || invalidSnapshotId == snapshot2.snapshotId()) {
      invalidSnapshotId++;
    }

    ScanContext scanContextWithInvalidSnapshotId =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
            .startSnapshotId(invalidSnapshotId)
            .build();

    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(
            TABLE_RESOURCE.tableLoader().clone(), scanContextWithInvalidSnapshotId, null);

    assertThatThrownBy(() -> splitPlanner.planSplits(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Start snapshot id not found in history: " + invalidSnapshotId);
  }

  @Test
  public void testIncrementalFromSnapshotId() throws Exception {
    appendTwoSnapshots();

    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
            .startSnapshotId(snapshot2.snapshotId())
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    assertThat(initialResult.fromPosition()).isNull();
    // For inclusive behavior of snapshot2, the initial result should point to snapshot1 (as
    // snapshot2's parent)
    assertThat(initialResult.toPosition().snapshotId().longValue())
        .isEqualTo(snapshot1.snapshotId());
    assertThat(initialResult.toPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot1.timestampMillis());
    assertThat(initialResult.splits()).isEmpty();

    ContinuousEnumerationResult secondResult = splitPlanner.planSplits(initialResult.toPosition());
    assertThat(secondResult.fromPosition().snapshotId().longValue())
        .isEqualTo(snapshot1.snapshotId());
    assertThat(secondResult.fromPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot1.timestampMillis());
    assertThat(secondResult.toPosition().snapshotId().longValue())
        .isEqualTo(snapshot2.snapshotId());
    assertThat(secondResult.toPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot2.timestampMillis());
    IcebergSourceSplit split = Iterables.getOnlyElement(secondResult.splits());
    assertThat(split.task().files()).hasSize(1);
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    // should  discover dataFile2 appended in snapshot2
    Set<String> expectedFiles = ImmutableSet.of(dataFile2.path().toString());
    assertThat(discoveredFiles).containsExactlyElementsOf(expectedFiles);

    IcebergEnumeratorPosition lastPosition = secondResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition).lastPosition;
    }
  }

  @Test
  public void testIncrementalFromSnapshotTimestampWithEmptyTable() {
    ScanContext scanContextWithInvalidSnapshotId =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .startSnapshotTimestamp(1L)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(
            TABLE_RESOURCE.tableLoader().clone(), scanContextWithInvalidSnapshotId, null);

    assertThatThrownBy(() -> splitPlanner.planSplits(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find a snapshot after: 1");
  }

  @Test
  public void testIncrementalFromSnapshotTimestampWithInvalidIds() throws Exception {
    appendTwoSnapshots();

    long invalidSnapshotTimestampMs = snapshot2.timestampMillis() + 1000L;

    ScanContext scanContextWithInvalidSnapshotId =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .startSnapshotTimestamp(invalidSnapshotTimestampMs)
            .build();

    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(
            TABLE_RESOURCE.tableLoader().clone(), scanContextWithInvalidSnapshotId, null);

    assertThatThrownBy(() -> splitPlanner.planSplits(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot find a snapshot after:");
  }

  @Test
  public void testIncrementalFromSnapshotTimestamp() throws Exception {
    appendTwoSnapshots();

    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .startSnapshotTimestamp(snapshot2.timestampMillis())
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    assertThat(initialResult.fromPosition()).isNull();
    // For inclusive behavior, the initial result should point to snapshot1 (as snapshot2's parent).
    assertThat(initialResult.toPosition().snapshotId().longValue())
        .isEqualTo(snapshot1.snapshotId());
    assertThat(initialResult.toPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot1.timestampMillis());
    assertThat(initialResult.splits()).isEmpty();

    ContinuousEnumerationResult secondResult = splitPlanner.planSplits(initialResult.toPosition());
    assertThat(secondResult.fromPosition().snapshotId().longValue())
        .isEqualTo(snapshot1.snapshotId());
    assertThat(secondResult.fromPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot1.timestampMillis());
    assertThat(secondResult.toPosition().snapshotId().longValue())
        .isEqualTo(snapshot2.snapshotId());
    assertThat(secondResult.toPosition().snapshotTimestampMs().longValue())
        .isEqualTo(snapshot2.timestampMillis());
    IcebergSourceSplit split = Iterables.getOnlyElement(secondResult.splits());
    assertThat(split.task().files()).hasSize(1);
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    // should discover dataFile2 appended in snapshot2
    Set<String> expectedFiles = ImmutableSet.of(dataFile2.path().toString());
    assertThat(discoveredFiles).containsExactlyElementsOf(expectedFiles);

    IcebergEnumeratorPosition lastPosition = secondResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition).lastPosition;
    }
  }

  @Test
  public void testMaxPlanningSnapshotCount() throws Exception {
    appendTwoSnapshots();
    // append 3 more snapshots
    for (int i = 2; i < 5; ++i) {
      appendSnapshot(i, 2);
    }

    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT)
            // limit to 1 snapshot per discovery
            .maxPlanningSnapshotCount(1)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    assertThat(initialResult.fromPosition()).isNull();
    // For inclusive behavior, the initial result should point to snapshot1's parent,
    // which leads to null snapshotId and snapshotTimestampMs.
    assertThat(initialResult.toPosition().snapshotId()).isNull();
    assertThat(initialResult.toPosition().snapshotTimestampMs()).isNull();
    assertThat(initialResult.splits()).isEmpty();

    ContinuousEnumerationResult secondResult = splitPlanner.planSplits(initialResult.toPosition());
    // should discover dataFile1 appended in snapshot1
    verifyMaxPlanningSnapshotCountResult(
        secondResult, null, snapshot1, ImmutableSet.of(dataFile1.path().toString()));

    ContinuousEnumerationResult thirdResult = splitPlanner.planSplits(secondResult.toPosition());
    // should discover dataFile2 appended in snapshot2
    verifyMaxPlanningSnapshotCountResult(
        thirdResult, snapshot1, snapshot2, ImmutableSet.of(dataFile2.path().toString()));
  }

  @Test
  public void testTableScanNoStats() throws Exception {
    appendTwoSnapshots();

    ScanContext scanContext =
        ScanContext.builder()
            .includeColumnStats(false)
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    assertThat(initialResult.splits()).hasSize(1);
    IcebergSourceSplit split = Iterables.getOnlyElement(initialResult.splits());
    assertThat(split.task().files()).hasSize(2);
    verifyStatCount(split, 0);

    IcebergEnumeratorPosition lastPosition = initialResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      CycleResult result = verifyOneCycle(splitPlanner, lastPosition);
      verifyStatCount(result.split, 0);
      lastPosition = result.lastPosition;
    }
  }

  @Test
  public void testTableScanAllStats() throws Exception {
    appendTwoSnapshots();

    ScanContext scanContext =
        ScanContext.builder()
            .includeColumnStats(true)
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    assertThat(initialResult.splits()).hasSize(1);
    IcebergSourceSplit split = Iterables.getOnlyElement(initialResult.splits());
    assertThat(split.task().files()).hasSize(2);
    verifyStatCount(split, 3);

    IcebergEnumeratorPosition lastPosition = initialResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      CycleResult result = verifyOneCycle(splitPlanner, lastPosition);
      verifyStatCount(result.split, 3);
      lastPosition = result.lastPosition;
    }
  }

  @Test
  public void testTableScanSingleStat() throws Exception {
    appendTwoSnapshots();

    ScanContext scanContext =
        ScanContext.builder()
            .includeColumnStats(ImmutableSet.of("data"))
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(TABLE_RESOURCE.tableLoader().clone(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    assertThat(initialResult.splits()).hasSize(1);
    IcebergSourceSplit split = Iterables.getOnlyElement(initialResult.splits());
    assertThat(split.task().files()).hasSize(2);
    verifyStatCount(split, 1);

    IcebergEnumeratorPosition lastPosition = initialResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      CycleResult result = verifyOneCycle(splitPlanner, lastPosition);
      verifyStatCount(result.split, 1);
      lastPosition = result.lastPosition;
    }
  }

  private void verifyStatCount(IcebergSourceSplit split, int expected) {
    if (expected == 0) {
      split
          .task()
          .files()
          .forEach(
              f -> {
                assertThat(f.file().valueCounts()).isNull();
                assertThat(f.file().columnSizes()).isNull();
                assertThat(f.file().lowerBounds()).isNull();
                assertThat(f.file().upperBounds()).isNull();
                assertThat(f.file().nanValueCounts()).isNull();
                assertThat(f.file().nullValueCounts()).isNull();
              });
    } else {
      split
          .task()
          .files()
          .forEach(
              f -> {
                assertThat(f.file().valueCounts()).hasSize(expected);
                assertThat(f.file().columnSizes()).hasSize(expected);
                assertThat(f.file().lowerBounds()).hasSize(expected);
                assertThat(f.file().upperBounds()).hasSize(expected);
                assertThat(f.file().nullValueCounts()).hasSize(expected);
                // The nanValue is not counted for long and string fields
                assertThat(f.file().nanValueCounts()).isEmpty();
              });
    }
  }

  private void verifyMaxPlanningSnapshotCountResult(
      ContinuousEnumerationResult result,
      Snapshot fromSnapshotExclusive,
      Snapshot toSnapshotInclusive,
      Set<String> expectedFiles) {
    if (fromSnapshotExclusive == null) {
      assertThat(result.fromPosition().snapshotId()).isNull();
      assertThat(result.fromPosition().snapshotTimestampMs()).isNull();
    } else {
      assertThat(result.fromPosition().snapshotId().longValue())
          .isEqualTo(fromSnapshotExclusive.snapshotId());
      assertThat(result.fromPosition().snapshotTimestampMs().longValue())
          .isEqualTo(fromSnapshotExclusive.timestampMillis());
    }
    assertThat(result.toPosition().snapshotId().longValue())
        .isEqualTo(toSnapshotInclusive.snapshotId());
    assertThat(result.toPosition().snapshotTimestampMs().longValue())
        .isEqualTo(toSnapshotInclusive.timestampMillis());
    // should only have one split with one data file, because split discover is limited to
    // one snapshot and each snapshot has only one data file appended.
    IcebergSourceSplit split = Iterables.getOnlyElement(result.splits());
    assertThat(split.task().files()).hasSize(1);
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    assertThat(discoveredFiles).containsExactlyElementsOf(expectedFiles);
  }

  private Snapshot appendSnapshot(long seed, int numRecords) throws Exception {
    List<Record> batch = RandomGenericData.generate(TestFixtures.SCHEMA, numRecords, seed);
    DataFile dataFile = dataAppender.writeFile(null, batch);
    dataAppender.appendToTable(dataFile);
    return TABLE_RESOURCE.table().currentSnapshot();
  }

  private static class CycleResult {
    IcebergEnumeratorPosition lastPosition;
    IcebergSourceSplit split;

    CycleResult(IcebergEnumeratorPosition lastPosition, IcebergSourceSplit split) {
      this.lastPosition = lastPosition;
      this.split = split;
    }
  }
}
