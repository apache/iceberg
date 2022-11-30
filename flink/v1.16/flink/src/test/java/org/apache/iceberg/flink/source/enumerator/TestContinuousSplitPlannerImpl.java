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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class TestContinuousSplitPlannerImpl {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final FileFormat fileFormat = FileFormat.PARQUET;
  private static final AtomicLong randomSeed = new AtomicLong();

  @Rule
  public final HadoopTableResource tableResource =
      new HadoopTableResource(
          TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  @Rule public TestName testName = new TestName();

  private GenericAppenderHelper dataAppender;
  private DataFile dataFile1;
  private Snapshot snapshot1;
  private DataFile dataFile2;
  private Snapshot snapshot2;

  @Before
  public void before() throws IOException {
    dataAppender = new GenericAppenderHelper(tableResource.table(), fileFormat, TEMPORARY_FOLDER);
  }

  private void appendTwoSnapshots() throws IOException {
    // snapshot1
    List<Record> batch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataFile1 = dataAppender.writeFile(null, batch1);
    dataAppender.appendToTable(dataFile1);
    snapshot1 = tableResource.table().currentSnapshot();

    // snapshot2
    List<Record> batch2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 1L);
    dataFile2 = dataAppender.writeFile(null, batch2);
    dataAppender.appendToTable(dataFile2);
    snapshot2 = tableResource.table().currentSnapshot();
  }

  /** @return the last enumerated snapshot id */
  private IcebergEnumeratorPosition verifyOneCycle(
      ContinuousSplitPlannerImpl splitPlanner, IcebergEnumeratorPosition lastPosition)
      throws Exception {
    List<Record> batch =
        RandomGenericData.generate(TestFixtures.SCHEMA, 2, randomSeed.incrementAndGet());
    DataFile dataFile = dataAppender.writeFile(null, batch);
    dataAppender.appendToTable(dataFile);
    Snapshot snapshot = tableResource.table().currentSnapshot();

    ContinuousEnumerationResult result = splitPlanner.planSplits(lastPosition);
    Assert.assertEquals(lastPosition.snapshotId(), result.fromPosition().snapshotId());
    Assert.assertEquals(
        lastPosition.snapshotTimestampMs(), result.fromPosition().snapshotTimestampMs());
    Assert.assertEquals(snapshot.snapshotId(), result.toPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot.timestampMillis(), result.toPosition().snapshotTimestampMs().longValue());
    Assert.assertEquals(1, result.splits().size());
    IcebergSourceSplit split = Iterables.getOnlyElement(result.splits());
    Assert.assertEquals(1, split.task().files().size());
    Assert.assertEquals(
        dataFile.path().toString(),
        Iterables.getOnlyElement(split.task().files()).file().path().toString());
    return result.toPosition();
  }

  @Test
  public void testTableScanThenIncrementalWithEmptyTable() throws Exception {
    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(tableResource.table(), scanContext, null);

    ContinuousEnumerationResult emptyTableInitialDiscoveryResult = splitPlanner.planSplits(null);
    Assert.assertTrue(emptyTableInitialDiscoveryResult.splits().isEmpty());
    Assert.assertNull(emptyTableInitialDiscoveryResult.fromPosition());
    Assert.assertTrue(emptyTableInitialDiscoveryResult.toPosition().isEmpty());
    Assert.assertNull(emptyTableInitialDiscoveryResult.toPosition().snapshotTimestampMs());

    ContinuousEnumerationResult emptyTableSecondDiscoveryResult =
        splitPlanner.planSplits(emptyTableInitialDiscoveryResult.toPosition());
    Assert.assertTrue(emptyTableSecondDiscoveryResult.splits().isEmpty());
    Assert.assertTrue(emptyTableSecondDiscoveryResult.fromPosition().isEmpty());
    Assert.assertNull(emptyTableSecondDiscoveryResult.fromPosition().snapshotTimestampMs());
    Assert.assertTrue(emptyTableSecondDiscoveryResult.toPosition().isEmpty());
    Assert.assertNull(emptyTableSecondDiscoveryResult.toPosition().snapshotTimestampMs());

    // next 3 snapshots
    IcebergEnumeratorPosition lastPosition = emptyTableSecondDiscoveryResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
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
        new ContinuousSplitPlannerImpl(tableResource.table(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    Assert.assertNull(initialResult.fromPosition());
    Assert.assertEquals(
        snapshot2.snapshotId(), initialResult.toPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot2.timestampMillis(), initialResult.toPosition().snapshotTimestampMs().longValue());
    Assert.assertEquals(1, initialResult.splits().size());
    IcebergSourceSplit split = Iterables.getOnlyElement(initialResult.splits());
    Assert.assertEquals(2, split.task().files().size());
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    Set<String> expectedFiles =
        ImmutableSet.of(dataFile1.path().toString(), dataFile2.path().toString());
    Assert.assertEquals(expectedFiles, discoveredFiles);

    IcebergEnumeratorPosition lastPosition = initialResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
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
        new ContinuousSplitPlannerImpl(tableResource.table(), scanContext, null);

    ContinuousEnumerationResult emptyTableInitialDiscoveryResult = splitPlanner.planSplits(null);
    Assert.assertTrue(emptyTableInitialDiscoveryResult.splits().isEmpty());
    Assert.assertNull(emptyTableInitialDiscoveryResult.fromPosition());
    Assert.assertTrue(emptyTableInitialDiscoveryResult.toPosition().isEmpty());
    Assert.assertNull(emptyTableInitialDiscoveryResult.toPosition().snapshotTimestampMs());

    ContinuousEnumerationResult emptyTableSecondDiscoveryResult =
        splitPlanner.planSplits(emptyTableInitialDiscoveryResult.toPosition());
    Assert.assertTrue(emptyTableSecondDiscoveryResult.splits().isEmpty());
    Assert.assertTrue(emptyTableSecondDiscoveryResult.fromPosition().isEmpty());
    Assert.assertNull(emptyTableSecondDiscoveryResult.fromPosition().snapshotTimestampMs());
    Assert.assertTrue(emptyTableSecondDiscoveryResult.toPosition().isEmpty());
    Assert.assertNull(emptyTableSecondDiscoveryResult.toPosition().snapshotTimestampMs());

    // latest mode should discover both snapshots, as latest position is marked by when job starts
    appendTwoSnapshots();
    ContinuousEnumerationResult afterTwoSnapshotsAppended =
        splitPlanner.planSplits(emptyTableSecondDiscoveryResult.toPosition());
    Assert.assertEquals(2, afterTwoSnapshotsAppended.splits().size());

    // next 3 snapshots
    IcebergEnumeratorPosition lastPosition = afterTwoSnapshotsAppended.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
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
        new ContinuousSplitPlannerImpl(tableResource.table(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    Assert.assertNull(initialResult.fromPosition());
    // For inclusive behavior, the initial result should point to snapshot1
    // Then the next incremental scan shall discover files from latest snapshot2 (inclusive)
    Assert.assertEquals(
        snapshot1.snapshotId(), initialResult.toPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot1.timestampMillis(), initialResult.toPosition().snapshotTimestampMs().longValue());
    Assert.assertEquals(0, initialResult.splits().size());

    ContinuousEnumerationResult secondResult = splitPlanner.planSplits(initialResult.toPosition());
    Assert.assertEquals(
        snapshot1.snapshotId(), secondResult.fromPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot1.timestampMillis(), secondResult.fromPosition().snapshotTimestampMs().longValue());
    Assert.assertEquals(snapshot2.snapshotId(), secondResult.toPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot2.timestampMillis(), secondResult.toPosition().snapshotTimestampMs().longValue());
    IcebergSourceSplit split = Iterables.getOnlyElement(secondResult.splits());
    Assert.assertEquals(1, split.task().files().size());
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    // should discover dataFile2 appended in snapshot2
    Set<String> expectedFiles = ImmutableSet.of(dataFile2.path().toString());
    Assert.assertEquals(expectedFiles, discoveredFiles);

    IcebergEnumeratorPosition lastPosition = secondResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
    }
  }

  @Test
  public void testIncrementalFromEarliestSnapshotWithEmptyTable() throws Exception {
    ScanContext scanContext =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(tableResource.table(), scanContext, null);

    ContinuousEnumerationResult emptyTableInitialDiscoveryResult = splitPlanner.planSplits(null);
    Assert.assertTrue(emptyTableInitialDiscoveryResult.splits().isEmpty());
    Assert.assertNull(emptyTableInitialDiscoveryResult.fromPosition());
    Assert.assertNull(emptyTableInitialDiscoveryResult.toPosition().snapshotId());
    Assert.assertNull(emptyTableInitialDiscoveryResult.toPosition().snapshotTimestampMs());

    ContinuousEnumerationResult emptyTableSecondDiscoveryResult =
        splitPlanner.planSplits(emptyTableInitialDiscoveryResult.toPosition());
    Assert.assertTrue(emptyTableSecondDiscoveryResult.splits().isEmpty());
    Assert.assertNull(emptyTableSecondDiscoveryResult.fromPosition().snapshotId());
    Assert.assertNull(emptyTableSecondDiscoveryResult.fromPosition().snapshotTimestampMs());
    Assert.assertNull(emptyTableSecondDiscoveryResult.toPosition().snapshotId());
    Assert.assertNull(emptyTableSecondDiscoveryResult.toPosition().snapshotTimestampMs());

    // next 3 snapshots
    IcebergEnumeratorPosition lastPosition = emptyTableSecondDiscoveryResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
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
        new ContinuousSplitPlannerImpl(tableResource.table(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    Assert.assertNull(initialResult.fromPosition());
    // For inclusive behavior, the initial result should point to snapshot1's parent,
    // which leads to null snapshotId and snapshotTimestampMs.
    Assert.assertNull(initialResult.toPosition().snapshotId());
    Assert.assertNull(initialResult.toPosition().snapshotTimestampMs());
    Assert.assertEquals(0, initialResult.splits().size());

    ContinuousEnumerationResult secondResult = splitPlanner.planSplits(initialResult.toPosition());
    Assert.assertNull(secondResult.fromPosition().snapshotId());
    Assert.assertNull(secondResult.fromPosition().snapshotTimestampMs());
    Assert.assertEquals(snapshot2.snapshotId(), secondResult.toPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot2.timestampMillis(), secondResult.toPosition().snapshotTimestampMs().longValue());
    IcebergSourceSplit split = Iterables.getOnlyElement(secondResult.splits());
    Assert.assertEquals(2, split.task().files().size());
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    // should discover files appended in both snapshot1 and snapshot2
    Set<String> expectedFiles =
        ImmutableSet.of(dataFile1.path().toString(), dataFile2.path().toString());
    Assert.assertEquals(expectedFiles, discoveredFiles);

    IcebergEnumeratorPosition lastPosition = secondResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
    }
  }

  @Test
  public void testIncrementalFromSnapshotIdWithEmptyTable() throws Exception {
    ScanContext scanContextWithInvalidSnapshotId =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
            .startSnapshotId(1L)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(
            tableResource.table(), scanContextWithInvalidSnapshotId, null);

    AssertHelpers.assertThrows(
        "Should detect invalid starting snapshot id",
        IllegalArgumentException.class,
        "Start snapshot id not found in history: 1",
        () -> splitPlanner.planSplits(null));
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
            tableResource.table(), scanContextWithInvalidSnapshotId, null);

    AssertHelpers.assertThrows(
        "Should detect invalid starting snapshot id",
        IllegalArgumentException.class,
        "Start snapshot id not found in history: " + invalidSnapshotId,
        () -> splitPlanner.planSplits(null));
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
        new ContinuousSplitPlannerImpl(tableResource.table(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    Assert.assertNull(initialResult.fromPosition());
    // For inclusive behavior of snapshot2, the initial result should point to snapshot1 (as
    // snapshot2's parent)
    Assert.assertEquals(
        snapshot1.snapshotId(), initialResult.toPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot1.timestampMillis(), initialResult.toPosition().snapshotTimestampMs().longValue());
    Assert.assertEquals(0, initialResult.splits().size());

    ContinuousEnumerationResult secondResult = splitPlanner.planSplits(initialResult.toPosition());
    Assert.assertEquals(
        snapshot1.snapshotId(), secondResult.fromPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot1.timestampMillis(), secondResult.fromPosition().snapshotTimestampMs().longValue());
    Assert.assertEquals(snapshot2.snapshotId(), secondResult.toPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot2.timestampMillis(), secondResult.toPosition().snapshotTimestampMs().longValue());
    IcebergSourceSplit split = Iterables.getOnlyElement(secondResult.splits());
    Assert.assertEquals(1, split.task().files().size());
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    // should  discover dataFile2 appended in snapshot2
    Set<String> expectedFiles = ImmutableSet.of(dataFile2.path().toString());
    Assert.assertEquals(expectedFiles, discoveredFiles);

    IcebergEnumeratorPosition lastPosition = secondResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
    }
  }

  @Test
  public void testIncrementalFromSnapshotTimestampWithEmptyTable() throws Exception {
    ScanContext scanContextWithInvalidSnapshotId =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .startSnapshotTimestamp(1L)
            .build();
    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(
            tableResource.table(), scanContextWithInvalidSnapshotId, null);

    AssertHelpers.assertThrows(
        "Should detect invalid starting snapshot timestamp",
        IllegalArgumentException.class,
        "Cannot find a snapshot older than 1970-01-01T00:00:00.001+00:00",
        () -> splitPlanner.planSplits(null));
  }

  @Test
  public void testIncrementalFromSnapshotTimestampWithInvalidIds() throws Exception {
    appendTwoSnapshots();

    long invalidSnapshotTimestampMs = snapshot1.timestampMillis() - 1000L;
    String invalidSnapshotTimestampMsStr =
        DateTimeUtil.formatTimestampMillis(invalidSnapshotTimestampMs);

    ScanContext scanContextWithInvalidSnapshotId =
        ScanContext.builder()
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .startSnapshotTimestamp(invalidSnapshotTimestampMs)
            .build();

    ContinuousSplitPlannerImpl splitPlanner =
        new ContinuousSplitPlannerImpl(
            tableResource.table(), scanContextWithInvalidSnapshotId, null);

    AssertHelpers.assertThrows(
        "Should detect invalid starting snapshot timestamp",
        IllegalArgumentException.class,
        "Cannot find a snapshot older than " + invalidSnapshotTimestampMsStr,
        () -> splitPlanner.planSplits(null));
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
        new ContinuousSplitPlannerImpl(tableResource.table(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    Assert.assertNull(initialResult.fromPosition());
    // For inclusive behavior, the initial result should point to snapshot1 (as snapshot2's parent).
    Assert.assertEquals(
        snapshot1.snapshotId(), initialResult.toPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot1.timestampMillis(), initialResult.toPosition().snapshotTimestampMs().longValue());
    Assert.assertEquals(0, initialResult.splits().size());

    ContinuousEnumerationResult secondResult = splitPlanner.planSplits(initialResult.toPosition());
    Assert.assertEquals(
        snapshot1.snapshotId(), secondResult.fromPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot1.timestampMillis(), secondResult.fromPosition().snapshotTimestampMs().longValue());
    Assert.assertEquals(snapshot2.snapshotId(), secondResult.toPosition().snapshotId().longValue());
    Assert.assertEquals(
        snapshot2.timestampMillis(), secondResult.toPosition().snapshotTimestampMs().longValue());
    IcebergSourceSplit split = Iterables.getOnlyElement(secondResult.splits());
    Assert.assertEquals(1, split.task().files().size());
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    // should discover dataFile2 appended in snapshot2
    Set<String> expectedFiles = ImmutableSet.of(dataFile2.path().toString());
    Assert.assertEquals(expectedFiles, discoveredFiles);

    IcebergEnumeratorPosition lastPosition = secondResult.toPosition();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
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
        new ContinuousSplitPlannerImpl(tableResource.table(), scanContext, null);

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    Assert.assertNull(initialResult.fromPosition());
    // For inclusive behavior, the initial result should point to snapshot1's parent,
    // which leads to null snapshotId and snapshotTimestampMs.
    Assert.assertNull(initialResult.toPosition().snapshotId());
    Assert.assertNull(initialResult.toPosition().snapshotTimestampMs());
    Assert.assertEquals(0, initialResult.splits().size());

    ContinuousEnumerationResult secondResult = splitPlanner.planSplits(initialResult.toPosition());
    // should discover dataFile1 appended in snapshot1
    verifyMaxPlanningSnapshotCountResult(
        secondResult, null, snapshot1, ImmutableSet.of(dataFile1.path().toString()));

    ContinuousEnumerationResult thirdResult = splitPlanner.planSplits(secondResult.toPosition());
    // should discover dataFile2 appended in snapshot2
    verifyMaxPlanningSnapshotCountResult(
        thirdResult, snapshot1, snapshot2, ImmutableSet.of(dataFile2.path().toString()));
  }

  private void verifyMaxPlanningSnapshotCountResult(
      ContinuousEnumerationResult result,
      Snapshot fromSnapshotExclusive,
      Snapshot toSnapshotInclusive,
      Set<String> expectedFiles) {
    if (fromSnapshotExclusive == null) {
      Assert.assertNull(result.fromPosition().snapshotId());
      Assert.assertNull(result.fromPosition().snapshotTimestampMs());
    } else {
      Assert.assertEquals(
          fromSnapshotExclusive.snapshotId(), result.fromPosition().snapshotId().longValue());
      Assert.assertEquals(
          fromSnapshotExclusive.timestampMillis(),
          result.fromPosition().snapshotTimestampMs().longValue());
    }
    Assert.assertEquals(
        toSnapshotInclusive.snapshotId(), result.toPosition().snapshotId().longValue());
    Assert.assertEquals(
        toSnapshotInclusive.timestampMillis(),
        result.toPosition().snapshotTimestampMs().longValue());
    // should only have one split with one data file, because split discover is limited to
    // one snapshot and each snapshot has only one data file appended.
    IcebergSourceSplit split = Iterables.getOnlyElement(result.splits());
    Assert.assertEquals(1, split.task().files().size());
    Set<String> discoveredFiles =
        split.task().files().stream()
            .map(fileScanTask -> fileScanTask.file().path().toString())
            .collect(Collectors.toSet());
    Assert.assertEquals(expectedFiles, discoveredFiles);
  }

  private Snapshot appendSnapshot(long seed, int numRecords) throws Exception {
    List<Record> batch = RandomGenericData.generate(TestFixtures.SCHEMA, numRecords, seed);
    DataFile dataFile = dataAppender.writeFile(null, batch);
    dataAppender.appendToTable(dataFile);
    return tableResource.table().currentSnapshot();
  }
}
