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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.flink.source.SplitHelpers;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class SplitAssignerTestBase {
  @TempDir protected Path temporaryFolder;

  @Test
  public void testEmptyInitialization() {
    SplitAssigner assigner = splitAssigner();
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
  }

  /** Test a sequence of interactions for StaticEnumerator */
  @Test
  public void testStaticEnumeratorSequence() throws Exception {
    SplitAssigner assigner = splitAssigner();
    assigner.onDiscoveredSplits(createSplits(4, 1, "1"));

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertSnapshot(assigner, 1);
    assigner.onUnassignedSplits(createSplits(1, 1, "1"));
    assertSnapshot(assigner, 2);

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }

  /** Test a sequence of interactions for ContinuousEnumerator */
  @Test
  public void testContinuousEnumeratorSequence() throws Exception {
    SplitAssigner assigner = splitAssigner();
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);

    List<IcebergSourceSplit> splits1 = createSplits(1, 1, "1");
    assertAvailableFuture(assigner, 1, () -> assigner.onDiscoveredSplits(splits1));
    List<IcebergSourceSplit> splits2 = createSplits(1, 1, "1");
    assertAvailableFuture(assigner, 1, () -> assigner.onUnassignedSplits(splits2));

    assigner.onDiscoveredSplits(createSplits(2, 1, "1"));
    assertSnapshot(assigner, 2);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }

  private void assertAvailableFuture(
      SplitAssigner assigner, int splitCount, Runnable addSplitsRunnable) {
    // register callback
    AtomicBoolean futureCompleted = new AtomicBoolean();
    CompletableFuture<Void> future = assigner.isAvailable();
    future.thenAccept(ignored -> futureCompleted.set(true));
    // calling isAvailable again should return the same object reference
    // note that thenAccept will return a new future.
    // we want to assert the same instance on the assigner returned future
    assertThat(assigner.isAvailable()).isSameAs(future);

    // now add some splits
    addSplitsRunnable.run();
    assertThat(futureCompleted.get()).isTrue();

    for (int i = 0; i < splitCount; ++i) {
      assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    }
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }

  protected void assertGetNext(SplitAssigner assigner, GetSplitResult.Status expectedStatus) {
    GetSplitResult result = assigner.getNext(null);
    assertThat(result.status()).isEqualTo(expectedStatus);
    switch (expectedStatus) {
      case AVAILABLE:
        assertThat(result.split()).isNotNull();
        break;
      case CONSTRAINED:
      case UNAVAILABLE:
        assertThat(result.split()).isNull();
        break;
      default:
        fail("Unknown status: %s", expectedStatus);
    }
  }

  protected void assertSnapshot(SplitAssigner assigner, int splitCount) {
    Collection<IcebergSourceSplitState> stateBeforeGet = assigner.state();
    assertThat(stateBeforeGet).hasSize(splitCount);
  }

  protected List<IcebergSourceSplit> createSplits(int fileCount, int filesPerSplit, String version)
      throws Exception {
    return SplitHelpers.createSplitsFromTransientHadoopTable(
        temporaryFolder, fileCount, filesPerSplit, version);
  }

  protected abstract SplitAssigner splitAssigner();
}
