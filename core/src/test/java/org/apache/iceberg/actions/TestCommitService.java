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
package org.apache.iceberg.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCommitService extends TestBase {
  private static final int BATCH_EXTRACTION_TIMEOUT_MS = 1000;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1);
  }

  @TestTemplate
  void rejectsNonPositiveCommitGroupSize() {
    assertThatThrownBy(() -> new CustomCommitService(table, 0, 10000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid number of file groups per commit: 0 (must be positive)");
    assertThatThrownBy(() -> new CustomCommitService(table, -1, 10000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid number of file groups per commit: -1 (must be positive)");
  }

  @TestTemplate
  public void testCommittedResultsCorrectly() {
    CustomCommitService commitService = new CustomCommitService(table, 5, 10000);
    commitService.start();

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    int numberOfFileGroups = 100;
    Tasks.range(numberOfFileGroups).executeWith(executorService).run(commitService::offer);
    commitService.close();

    Set<Integer> expected = Sets.newHashSet(IntStream.range(0, 100).iterator());
    Set<Integer> actual = Sets.newHashSet(commitService.results());
    assertThat(actual).isEqualTo(expected);
  }

  @TestTemplate
  public void testAbortFileGroupsAfterTimeout() throws Exception {
    BlockingCommitService commitService = new BlockingCommitService(table, 5, 100);
    commitService.start();

    CustomCommitService spyCommitService = spy(commitService);
    doReturn(false).when(spyCommitService).canCreateCommitGroup();
    for (int i = 0; i < 7; i++) {
      spyCommitService.offer(i);
    }

    ExecutorService offerExecutor = Executors.newSingleThreadExecutor();
    ExecutorService closeExecutor = Executors.newSingleThreadExecutor();
    try {
      Future<?> commitResult = offerExecutor.submit(() -> commitService.offer(7));
      assertThat(commitService.commitStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<Throwable> closeResult =
          closeExecutor.submit(
              () -> {
                try {
                  commitService.close();
                  return null;
                } catch (Throwable e) {
                  return e;
                }
              });

      assertThat(closeResult.get(5, TimeUnit.SECONDS))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Timeout occurred when waiting for commits")
          .hasMessageNotContaining("{}")
          .hasMessageMatching(
              ".*\\d+ file groups committed\\. \\d+ file groups remain uncommitted\\..*");
      assertThat(commitService.aborted).containsExactlyInAnyOrder(5, 6, 7);
      commitService.releaseCommit.countDown();
      commitResult.get(5, TimeUnit.SECONDS);
    } finally {
      commitService.releaseCommit.countDown();
      offerExecutor.shutdownNow();
      closeExecutor.shutdownNow();
      closeQuietly(commitService);
    }

    assertThat(offerExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(closeExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInSameThread()
        .untilAsserted(() -> assertThat(commitService.completedRewritesAllCommitted()).isTrue());
    assertThat(commitService.results()).containsExactlyInAnyOrder(0, 1, 2, 3, 4);
  }

  @TestTemplate
  void closeReportsTimeoutWhileBatchExtractionIsInProgress() throws Exception {
    BlockingHashCodeCommitService commitService =
        new BlockingHashCodeCommitService(table, 1, BATCH_EXTRACTION_TIMEOUT_MS);
    BlockingHashCodeGroup group = new BlockingHashCodeGroup();
    commitService.start();

    ExecutorService offerExecutor = Executors.newSingleThreadExecutor();
    ExecutorService closeExecutor = Executors.newSingleThreadExecutor();
    try {
      Future<?> commitResult = offerExecutor.submit(() -> commitService.offer(group));
      assertThat(group.hashCodeStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<Throwable> closeResult =
          closeExecutor.submit(
              () -> {
                try {
                  commitService.close();
                  return null;
                } catch (Throwable e) {
                  return e;
                }
              });

      assertThat(closeResult.get(5, TimeUnit.SECONDS))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Timeout occurred when waiting for commits");
      group.releaseHashCode.countDown();
      commitResult.get(5, TimeUnit.SECONDS);
    } finally {
      group.releaseHashCode.countDown();
      offerExecutor.shutdownNow();
      closeExecutor.shutdownNow();
      closeQuietly(commitService);
    }

    assertThat(offerExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(closeExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(commitService.results()).containsExactly(group);
  }

  @TestTemplate
  void failedBatchExtractionDoesNotStrandClose() {
    ThrowingHashCodeCommitService commitService =
        new ThrowingHashCodeCommitService(table, 1, BATCH_EXTRACTION_TIMEOUT_MS);
    commitService.start();

    boolean closed = false;
    try {
      assertThatThrownBy(() -> commitService.offer(new ThrowingHashCodeGroup()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Cannot hash file group");

      commitService.close();
      closed = true;
    } finally {
      if (!closed) {
        closeQuietly(commitService);
      }
    }
  }

  @TestTemplate
  void serializesCommitsWithoutBlockingOtherOffers() throws Exception {
    TrackingCommitService commitService = new TrackingCommitService(table);
    commitService.start();

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    boolean closed = false;
    try {
      Future<?> firstCommit = executorService.submit(() -> commitService.offer(0));
      assertThat(commitService.firstCommitStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<?> secondCommit = executorService.submit(() -> commitService.offer(1));
      secondCommit.get(5, TimeUnit.SECONDS);
      assertThat(commitService.secondCommitStarted.getCount()).isEqualTo(1);
      assertThat(commitService.maxConcurrentCommits.get()).isEqualTo(1);

      commitService.releaseCommits.countDown();
      firstCommit.get(5, TimeUnit.SECONDS);
      assertThat(commitService.secondCommitStarted.await(5, TimeUnit.SECONDS)).isTrue();
      commitService.close();
      closed = true;
    } finally {
      commitService.releaseCommits.countDown();
      executorService.shutdownNow();
      if (!closed) {
        closeQuietly(commitService);
      }
    }

    assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(commitService.results()).containsExactlyInAnyOrder(0, 1);
  }

  @TestTemplate
  void drainOwnerProcessesBatchOfferedDuringHandoff() throws Exception {
    HandoffCommitService commitService = new HandoffCommitService(table);
    commitService.start();

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    boolean closed = false;
    try {
      Future<?> firstCommit = executorService.submit(() -> commitService.offer(0));
      assertThat(commitService.firstCommitStarted.await(5, TimeUnit.SECONDS)).isTrue();
      commitService.releaseFirstCommit.countDown();
      assertThat(commitService.ownerObservedEmptyQueue.await(5, TimeUnit.SECONDS)).isTrue();

      Future<?> handoffOffer = executorService.submit(() -> commitService.offer(1));
      handoffOffer.get(5, TimeUnit.SECONDS);
      assertThat(commitService.secondCommitStarted.getCount()).isEqualTo(1);

      commitService.releaseOwner.countDown();
      assertThat(commitService.secondCommitStarted.await(5, TimeUnit.SECONDS)).isTrue();
      firstCommit.get(5, TimeUnit.SECONDS);
      commitService.close();
      closed = true;
    } finally {
      commitService.releaseFirstCommit.countDown();
      commitService.releaseOwner.countDown();
      executorService.shutdownNow();
      if (!closed) {
        closeQuietly(commitService);
      }
    }

    assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(commitService.results()).containsExactlyInAnyOrder(0, 1);
  }

  private static class CustomCommitService extends BaseCommitService<Integer> {
    CustomCommitService(Table table, int rewritesPerCommit, int timeoutInMS) {
      super(table, rewritesPerCommit, timeoutInMS);
    }

    @Override
    protected void commitOrClean(Set<Integer> batch) {}

    @Override
    protected void abortFileGroup(Integer group) {}
  }

  private static class BlockingCommitService extends CustomCommitService {
    private final Set<Integer> aborted = Sets.newConcurrentHashSet();
    private final CountDownLatch commitStarted = new CountDownLatch(1);
    private final CountDownLatch releaseCommit = new CountDownLatch(1);

    BlockingCommitService(Table table, int rewritesPerCommit, int timeoutInMS) {
      super(table, rewritesPerCommit, timeoutInMS);
    }

    @Override
    protected void commitOrClean(Set<Integer> batch) {
      commitStarted.countDown();
      await(releaseCommit);
    }

    @Override
    protected void abortFileGroup(Integer group) {
      aborted.add(group);
    }
  }

  private static class BlockingHashCodeCommitService
      extends BaseCommitService<BlockingHashCodeGroup> {

    BlockingHashCodeCommitService(Table table, int rewritesPerCommit, int timeoutInMS) {
      super(table, rewritesPerCommit, timeoutInMS);
    }

    @Override
    protected void commitOrClean(Set<BlockingHashCodeGroup> batch) {}

    @Override
    protected void abortFileGroup(BlockingHashCodeGroup group) {}
  }

  private static class BlockingHashCodeGroup {
    private final CountDownLatch hashCodeStarted = new CountDownLatch(1);
    private final CountDownLatch releaseHashCode = new CountDownLatch(1);

    @Override
    public int hashCode() {
      hashCodeStarted.countDown();
      await(releaseHashCode);
      return 0;
    }

    @Override
    public boolean equals(Object other) {
      return this == other;
    }
  }

  private static class ThrowingHashCodeCommitService
      extends BaseCommitService<ThrowingHashCodeGroup> {

    ThrowingHashCodeCommitService(Table table, int rewritesPerCommit, int timeoutInMS) {
      super(table, rewritesPerCommit, timeoutInMS);
    }

    @Override
    protected void commitOrClean(Set<ThrowingHashCodeGroup> batch) {}

    @Override
    protected void abortFileGroup(ThrowingHashCodeGroup group) {}
  }

  private static class ThrowingHashCodeGroup {
    @Override
    public int hashCode() {
      throw new IllegalStateException("Cannot hash file group");
    }

    @Override
    public boolean equals(Object other) {
      return this == other;
    }
  }

  private static class TrackingCommitService extends BaseCommitService<Integer> {
    private final CountDownLatch firstCommitStarted = new CountDownLatch(1);
    private final CountDownLatch secondCommitStarted = new CountDownLatch(1);
    private final CountDownLatch releaseCommits = new CountDownLatch(1);
    private final AtomicInteger activeCommits = new AtomicInteger(0);
    private final AtomicInteger maxConcurrentCommits = new AtomicInteger(0);

    TrackingCommitService(Table table) {
      super(table, 1, 10000);
    }

    @Override
    protected void commitOrClean(Set<Integer> batch) {
      int active = activeCommits.incrementAndGet();
      maxConcurrentCommits.accumulateAndGet(active, Math::max);
      if (batch.contains(0)) {
        firstCommitStarted.countDown();
      } else {
        secondCommitStarted.countDown();
      }

      try {
        await(releaseCommits);
      } finally {
        activeCommits.decrementAndGet();
      }
    }

    @Override
    protected void abortFileGroup(Integer group) {}
  }

  private static class HandoffCommitService extends BaseCommitService<Integer> {
    private final CountDownLatch firstCommitStarted = new CountDownLatch(1);
    private final CountDownLatch releaseFirstCommit = new CountDownLatch(1);
    private final CountDownLatch ownerObservedEmptyQueue = new CountDownLatch(1);
    private final CountDownLatch releaseOwner = new CountDownLatch(1);
    private final CountDownLatch secondCommitStarted = new CountDownLatch(1);
    private final AtomicBoolean pauseOwner = new AtomicBoolean(false);
    private final AtomicBoolean ownerPaused = new AtomicBoolean(false);
    private volatile Thread ownerThread = null;

    HandoffCommitService(Table table) {
      super(table, 1, 10000);
    }

    @Override
    protected void commitOrClean(Set<Integer> batch) {
      if (batch.contains(0)) {
        ownerThread = Thread.currentThread();
        firstCommitStarted.countDown();
        await(releaseFirstCommit);
        pauseOwner.set(true);
      } else {
        secondCommitStarted.countDown();
      }
    }

    @Override
    protected void abortFileGroup(Integer group) {}

    @Override
    boolean canCreateCommitGroup() {
      boolean canCreate = super.canCreateCommitGroup();
      if (!canCreate
          && pauseOwner.get()
          && Thread.currentThread().equals(ownerThread)
          && ownerPaused.compareAndSet(false, true)) {
        ownerObservedEmptyQueue.countDown();
        await(releaseOwner);
      }

      return canCreate;
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void closeQuietly(BaseCommitService<?> commitService) {
    try {
      commitService.close();
    } catch (RuntimeException ignored) {
      // The original test failure should not be replaced by a cleanup failure.
    }
  }
}
