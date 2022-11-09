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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.junit.Assert;
import org.mockito.internal.util.collections.Sets;

public abstract class TestSplitAssignerBase {

  protected void assertAvailableFuture(
      SplitAssigner assigner, int splitCount, Runnable addSplitsRunnable) {
    assertAvailableFuture(assigner, splitCount, addSplitsRunnable, null);
  }

  protected void assertAvailableFuture(
      SplitAssigner assigner, int splitCount, Runnable addSplitsRunnable, String hostname) {
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
    Assert.assertTrue(futureCompleted.get());
    Assert.assertEquals(assigner.pendingSplitCount(), splitCount);

    for (int i = 0; i < splitCount; ++i) {
      assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, hostname);
      Assert.assertEquals(assigner.pendingSplitCount(), splitCount - i - 1);
    }
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE, hostname);
    assertSnapshot(assigner, 0);
  }

  protected void assertGetNext(SplitAssigner assigner, GetSplitResult.Status expectedStatus) {
    assertGetNext(assigner, expectedStatus, null);
  }

  protected void assertGetNext(
      SplitAssigner assigner, GetSplitResult.Status expectedStatus, String requestHostname) {
    assertGetNext(
        assigner,
        expectedStatus,
        requestHostname,
        requestHostname == null ? null : Sets.newSet(requestHostname));
  }

  protected void assertGetNext(
      SplitAssigner assigner,
      GetSplitResult.Status expectedStatus,
      String requestHostname,
      Set<String> expectedHostname) {
    assertGetNext(assigner, expectedStatus, requestHostname, expectedHostname, null);
  }

  protected void assertGetNext(
      SplitAssigner assigner,
      GetSplitResult.Status expectedStatus,
      String requestHostname,
      Set<String> expectedHostname,
      String incorrectHostname) {
    GetSplitResult result = assigner.getNext(requestHostname);
    Assert.assertEquals(expectedStatus, result.status());

    switch (expectedStatus) {
      case AVAILABLE:
        if (incorrectHostname != null) {
          Assert.assertFalse(Sets.newSet(result.split().hostnames()).contains(incorrectHostname));
        }

        if (expectedHostname != null) {
          Assert.assertTrue(Sets.newSet(result.split().hostnames()).containsAll(expectedHostname));
        } else {
          Assert.assertNull(result.split().hostnames());
        }
        break;
      case CONSTRAINED:
      case UNAVAILABLE:
        Assert.assertNull(result.split());
        break;
      default:
        Assert.fail("Unknown status: " + expectedStatus);
    }
  }

  protected void assertSnapshot(SplitAssigner assigner, int splitCount) {
    Collection<IcebergSourceSplitState> stateBeforeGet = assigner.state();
    Assert.assertEquals(splitCount, stateBeforeGet.size());
  }
}
