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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.SplitHelpers;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.flink.source.split.SplitRequestEvent;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestContinuousIcebergEnumerator {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void testDiscoverSplitWhenNoReaderRegistered() throws Exception {
    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();
    ManualContinuousSplitPlanner splitPlanner = new ManualContinuousSplitPlanner(scanContext);
    ContinuousIcebergEnumerator enumerator =
        createEnumerator(enumeratorContext, scanContext, splitPlanner);

    Collection<IcebergSourceSplitState> pendingSplitsEmpty =
        enumerator.snapshotState(1).pendingSplits();
    Assert.assertEquals(0, pendingSplitsEmpty.size());

    // make one split available and trigger the periodic discovery
    List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    splitPlanner.addSplits(splits);
    enumeratorContext.triggerAllActions();

    Collection<IcebergSourceSplitState> pendingSplits = enumerator.snapshotState(2).pendingSplits();
    Assert.assertEquals(1, pendingSplits.size());
    IcebergSourceSplitState pendingSplit = pendingSplits.iterator().next();
    Assert.assertEquals(splits.get(0).splitId(), pendingSplit.split().splitId());
    Assert.assertEquals(IcebergSourceSplitStatus.UNASSIGNED, pendingSplit.status());
  }

  @Test
  public void testDiscoverWhenReaderRegistered() throws Exception {
    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();
    ManualContinuousSplitPlanner splitPlanner = new ManualContinuousSplitPlanner(scanContext);
    ContinuousIcebergEnumerator enumerator =
        createEnumerator(enumeratorContext, scanContext, splitPlanner);

    // register one reader, and let it request a split
    enumeratorContext.registerReader(2, "localhost");
    enumerator.addReader(2);
    enumerator.handleSourceEvent(2, new SplitRequestEvent());

    // make one split available and trigger the periodic discovery
    List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    splitPlanner.addSplits(splits);
    enumeratorContext.triggerAllActions();

    Assert.assertTrue(enumerator.snapshotState(1).pendingSplits().isEmpty());
    Assertions.assertThat(enumeratorContext.getSplitAssignments().get(2).getAssignedSplits())
        .contains(splits.get(0));
  }

  @Test
  public void testRequestingReaderUnavailableWhenSplitDiscovered() throws Exception {
    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();
    ManualContinuousSplitPlanner splitPlanner = new ManualContinuousSplitPlanner(scanContext);
    ContinuousIcebergEnumerator enumerator =
        createEnumerator(enumeratorContext, scanContext, splitPlanner);

    // register one reader, and let it request a split
    enumeratorContext.registerReader(2, "localhost");
    enumerator.addReader(2);
    enumerator.handleSourceEvent(2, new SplitRequestEvent());

    // remove the reader (like in a failure)
    enumeratorContext.registeredReaders().remove(2);

    // make one split available and trigger the periodic discovery
    List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    Assert.assertEquals(1, splits.size());
    splitPlanner.addSplits(splits);
    enumeratorContext.triggerAllActions();

    Assert.assertFalse(enumeratorContext.getSplitAssignments().containsKey(2));
    List<String> pendingSplitIds =
        enumerator.snapshotState(1).pendingSplits().stream()
            .map(IcebergSourceSplitState::split)
            .map(IcebergSourceSplit::splitId)
            .collect(Collectors.toList());
    Assert.assertEquals(splits.size(), pendingSplitIds.size());
    Assert.assertEquals(splits.get(0).splitId(), pendingSplitIds.get(0));

    // register the reader again, and let it request a split
    enumeratorContext.registerReader(2, "localhost");
    enumerator.addReader(2);
    enumerator.handleSourceEvent(2, new SplitRequestEvent());

    Assert.assertTrue(enumerator.snapshotState(2).pendingSplits().isEmpty());
    Assertions.assertThat(enumeratorContext.getSplitAssignments().get(2).getAssignedSplits())
        .contains(splits.get(0));
  }

  @Test
  public void testThrottlingDiscovery() throws Exception {
    // create 10 splits
    List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 10, 1);

    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT)
            // discover one snapshot at a time
            .maxPlanningSnapshotCount(1)
            .build();
    ManualContinuousSplitPlanner splitPlanner = new ManualContinuousSplitPlanner(scanContext);
    ContinuousIcebergEnumerator enumerator =
        createEnumerator(enumeratorContext, scanContext, splitPlanner);

    // register reader-2, and let it request a split
    enumeratorContext.registerReader(2, "localhost");
    enumerator.addReader(2);
    enumerator.handleSourceEvent(2, new SplitRequestEvent());

    // add splits[0] to the planner for next discovery
    splitPlanner.addSplits(Arrays.asList(splits.get(0)));
    enumeratorContext.triggerAllActions();

    // because discovered split was assigned to reader, pending splits should be empty
    Assert.assertEquals(0, enumerator.snapshotState(1).pendingSplits().size());
    // split assignment to reader-2 should contain splits[0, 1)
    Assert.assertEquals(
        splits.subList(0, 1), enumeratorContext.getSplitAssignments().get(2).getAssignedSplits());

    // add the remaining 9 splits (one for every snapshot)
    // run discovery cycles while reader-2 still processing the splits[0]
    for (int i = 1; i < 10; ++i) {
      splitPlanner.addSplits(Arrays.asList(splits.get(i)));
      enumeratorContext.triggerAllActions();
    }

    // can only discover up to 3 snapshots/splits
    Assert.assertEquals(3, enumerator.snapshotState(2).pendingSplits().size());
    // split assignment to reader-2 should be splits[0, 1)
    Assert.assertEquals(
        splits.subList(0, 1), enumeratorContext.getSplitAssignments().get(2).getAssignedSplits());

    // now reader-2 finished splits[0]
    enumerator.handleSourceEvent(2, new SplitRequestEvent(Arrays.asList(splits.get(0).splitId())));
    enumeratorContext.triggerAllActions();
    // still have 3 pending splits. After assigned splits[1] to reader-2, one more split was
    // discovered and added.
    Assert.assertEquals(3, enumerator.snapshotState(3).pendingSplits().size());
    // split assignment to reader-2 should be splits[0, 2)
    Assert.assertEquals(
        splits.subList(0, 2), enumeratorContext.getSplitAssignments().get(2).getAssignedSplits());

    // run 3 more split discovery cycles
    for (int i = 0; i < 3; ++i) {
      enumeratorContext.triggerAllActions();
    }

    // no more splits are discovered due to throttling
    Assert.assertEquals(3, enumerator.snapshotState(4).pendingSplits().size());
    // split assignment to reader-2 should still be splits[0, 2)
    Assert.assertEquals(
        splits.subList(0, 2), enumeratorContext.getSplitAssignments().get(2).getAssignedSplits());

    // now reader-2 finished splits[1]
    enumerator.handleSourceEvent(2, new SplitRequestEvent(Arrays.asList(splits.get(1).splitId())));
    enumeratorContext.triggerAllActions();
    // still have 3 pending splits. After assigned new splits[2] to reader-2, one more split was
    // discovered and added.
    Assert.assertEquals(3, enumerator.snapshotState(5).pendingSplits().size());
    // split assignment to reader-2 should be splits[0, 3)
    Assert.assertEquals(
        splits.subList(0, 3), enumeratorContext.getSplitAssignments().get(2).getAssignedSplits());
  }

  private static ContinuousIcebergEnumerator createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> context,
      ScanContext scanContext,
      ContinuousSplitPlanner splitPlanner) {

    ContinuousIcebergEnumerator enumerator =
        new ContinuousIcebergEnumerator(
            context,
            new SimpleSplitAssigner(Collections.emptyList()),
            scanContext,
            splitPlanner,
            null);
    enumerator.start();
    return enumerator;
  }
}
