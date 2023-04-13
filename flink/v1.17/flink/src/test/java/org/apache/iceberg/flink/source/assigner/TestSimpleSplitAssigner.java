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
package org.apache.iceberg.flink.source.assigner;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.flink.source.SplitHelpers;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSimpleSplitAssigner {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void testEmptyInitialization() {
    SimpleSplitAssigner assigner = new SimpleSplitAssigner();
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
  }

  /** Test a sequence of interactions for StaticEnumerator */
  @Test
  public void testStaticEnumeratorSequence() throws Exception {
    SimpleSplitAssigner assigner = new SimpleSplitAssigner();
    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 4, 2));

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertSnapshot(assigner, 1);
    assigner.onUnassignedSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1));
    assertSnapshot(assigner, 2);

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }

  /** Test a sequence of interactions for ContinuousEnumerator */
  @Test
  public void testContinuousEnumeratorSequence() throws Exception {
    SimpleSplitAssigner assigner = new SimpleSplitAssigner();
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);

    List<IcebergSourceSplit> splits1 =
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    assertAvailableFuture(assigner, 1, () -> assigner.onDiscoveredSplits(splits1));
    List<IcebergSourceSplit> splits2 =
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    assertAvailableFuture(assigner, 1, () -> assigner.onUnassignedSplits(splits2));

    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 2, 1));
    assertSnapshot(assigner, 2);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }

  private void assertAvailableFuture(
      SimpleSplitAssigner assigner, int splitCount, Runnable addSplitsRunnable) {
    // register callback
    AtomicBoolean futureCompleted = new AtomicBoolean();
    CompletableFuture<Void> future = assigner.isAvailable();
    future.thenAccept(ignored -> futureCompleted.set(true));
    // calling isAvailable again should return the same object reference
    // note that thenAccept will return a new future.
    // we want to assert the same instance on the assigner returned future
    Assert.assertSame(future, assigner.isAvailable());

    // now add some splits
    addSplitsRunnable.run();
    Assert.assertEquals(true, futureCompleted.get());

    for (int i = 0; i < splitCount; ++i) {
      assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    }
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }

  private void assertGetNext(SimpleSplitAssigner assigner, GetSplitResult.Status expectedStatus) {
    GetSplitResult result = assigner.getNext(null);
    Assert.assertEquals(expectedStatus, result.status());
    switch (expectedStatus) {
      case AVAILABLE:
        Assert.assertNotNull(result.split());
        break;
      case CONSTRAINED:
      case UNAVAILABLE:
        Assert.assertNull(result.split());
        break;
      default:
        Assert.fail("Unknown status: " + expectedStatus);
    }
  }

  private void assertSnapshot(SimpleSplitAssigner assigner, int splitCount) {
    Collection<IcebergSourceSplitState> stateBeforeGet = assigner.state();
    Assert.assertEquals(splitCount, stateBeforeGet.size());
  }
}
