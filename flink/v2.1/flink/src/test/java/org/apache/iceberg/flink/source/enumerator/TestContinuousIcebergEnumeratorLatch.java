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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.DefaultSplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

/**
 * Targeted tests for {@link ContinuousIcebergEnumerator}'s single-flight latch ({@code
 * planningInFlight}). These exercise the latch's state machine directly and under real concurrency,
 * because {@link TestingSplitEnumeratorContext} runs callable + handler as one sequential unit and
 * cannot reproduce the race that motivated the latch (the IO thread being preempted between {@code
 * synchronized planSplits} exit and the framework's handler dispatch).
 *
 * <p>Uses reflection to invoke the package-internal {@code discoverSplits} / {@code
 * processDiscoveredSplits} private methods and to read the {@code planningInFlight} flag.
 */
public class TestContinuousIcebergEnumeratorLatch {

  private static ScanContext streamingContext() {
    return ScanContext.builder()
        .streaming(true)
        .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
  }

  private static ContinuousIcebergEnumerator newEnumerator(
      TestingSplitEnumeratorContext<IcebergSourceSplit> ctx, ContinuousSplitPlanner planner) {
    return new ContinuousIcebergEnumerator(
        ctx,
        new DefaultSplitAssigner(null, Collections.emptyList()),
        streamingContext(),
        planner,
        null);
  }

  private static AtomicBoolean latchOf(ContinuousIcebergEnumerator enumerator) throws Exception {
    Field field = ContinuousIcebergEnumerator.class.getDeclaredField("planningInFlight");
    field.setAccessible(true);
    return (AtomicBoolean) field.get(enumerator);
  }

  private static ContinuousEnumerationResult noopSentinel() throws Exception {
    Field field = ContinuousIcebergEnumerator.class.getDeclaredField("LATCH_HELD_NOOP");
    field.setAccessible(true);
    return (ContinuousEnumerationResult) field.get(null);
  }

  private static Method discoverSplitsMethod() throws Exception {
    Method method = ContinuousIcebergEnumerator.class.getDeclaredMethod("discoverSplits");
    method.setAccessible(true);
    return method;
  }

  private static Method processDiscoveredSplitsMethod() throws Exception {
    Method method =
        ContinuousIcebergEnumerator.class.getDeclaredMethod(
            "processDiscoveredSplits", ContinuousEnumerationResult.class, Throwable.class);
    method.setAccessible(true);
    return method;
  }

  private static ContinuousEnumerationResult callDiscover(
      Method method, ContinuousIcebergEnumerator enumerator) throws Exception {
    try {
      return (ContinuousEnumerationResult) method.invoke(enumerator);
    } catch (InvocationTargetException ite) {
      Throwable cause = ite.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  @Test
  public void testLatchStateMachine() throws Exception {
    AtomicInteger plannerCalls = new AtomicInteger();
    AtomicInteger throwsRemaining = new AtomicInteger();
    ContinuousSplitPlanner planner = trackingPlanner(plannerCalls, throwsRemaining);

    TestingSplitEnumeratorContext<IcebergSourceSplit> ctx = new TestingSplitEnumeratorContext<>(1);
    ContinuousIcebergEnumerator enumerator = newEnumerator(ctx, planner);

    AtomicBoolean latch = latchOf(enumerator);
    ContinuousEnumerationResult noop = noopSentinel();
    Method discoverSplits = discoverSplitsMethod();
    Method processDiscoveredSplits = processDiscoveredSplitsMethod();

    // Happy path: discoverSplits acquires the latch, calls the planner, returns a real result.
    assertThat(latch.get()).isFalse();
    ContinuousEnumerationResult r1 = callDiscover(discoverSplits, enumerator);
    assertThat(r1).isNotSameAs(noop);
    assertThat(latch.get()).isTrue();
    assertThat(plannerCalls.get()).isEqualTo(1);

    // Re-entrant call while the latch is held returns the identity sentinel and does NOT
    // invoke the planner.
    ContinuousEnumerationResult r2 = callDiscover(discoverSplits, enumerator);
    assertThat(r2).isSameAs(noop);
    assertThat(plannerCalls.get()).isEqualTo(1);
    assertThat(latch.get()).isTrue();

    // The sentinel handler must NOT release the latch (it didn't acquire it).
    processDiscoveredSplits.invoke(enumerator, noop, null);
    assertThat(latch.get()).isTrue();

    // The real handler releases the latch in its finally block.
    processDiscoveredSplits.invoke(enumerator, r1, null);
    assertThat(latch.get()).isFalse();

    // Exception path: planner throws. The latch must stay TRUE after the throw — only the
    // handler's finally is allowed to release it. (Regression for the original double-release
    // bug, where discoverSplits also released on exception and a concurrent caller's freshly-
    // acquired latch would be wrongly cleared by the failing handler's finally.)
    throwsRemaining.set(1);
    assertThatThrownBy(() -> callDiscover(discoverSplits, enumerator))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("simulated planner failure");
    assertThat(latch.get()).isTrue();
    assertThat(plannerCalls.get()).isEqualTo(2);

    // Framework dispatches handler(null, exception); finally releases the latch.
    processDiscoveredSplits.invoke(
        enumerator, null, new RuntimeException("simulated planner failure"));
    assertThat(latch.get()).isFalse();

    // The latch is now usable again — next discoverSplits acquires.
    ContinuousEnumerationResult r3 = callDiscover(discoverSplits, enumerator);
    assertThat(r3).isNotSameAs(noop);
    assertThat(latch.get()).isTrue();
    assertThat(plannerCalls.get()).isEqualTo(3);

    processDiscoveredSplits.invoke(enumerator, r3, null);
    assertThat(latch.get()).isFalse();
  }

  @Test
  public void testLatchPreventsConcurrentPlannerEntries() throws Exception {
    // Two real threads race into discoverSplits. The first to CAS wins and enters the planner;
    // the second must see the latch held and short-circuit with the NOOP sentinel without
    // calling the planner.
    CountDownLatch insidePlanner = new CountDownLatch(1);
    CountDownLatch releasePlanner = new CountDownLatch(1);
    AtomicInteger currentlyInsidePlanner = new AtomicInteger();
    AtomicInteger peakInsidePlanner = new AtomicInteger();
    AtomicInteger plannerCalls = new AtomicInteger();

    ContinuousSplitPlanner planner =
        new ContinuousSplitPlanner() {
          @Override
          public ContinuousEnumerationResult planSplits(IcebergEnumeratorPosition lastPosition) {
            int cur = currentlyInsidePlanner.incrementAndGet();
            peakInsidePlanner.accumulateAndGet(cur, Math::max);
            plannerCalls.incrementAndGet();
            insidePlanner.countDown();
            try {
              if (!releasePlanner.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for releasePlanner");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            currentlyInsidePlanner.decrementAndGet();
            return new ContinuousEnumerationResult(
                Collections.emptyList(), lastPosition, IcebergEnumeratorPosition.empty());
          }

          @Override
          public int recommendedLowWatermark() {
            return 100;
          }

          @Override
          public void close() {}
        };

    TestingSplitEnumeratorContext<IcebergSourceSplit> ctx = new TestingSplitEnumeratorContext<>(1);
    ContinuousIcebergEnumerator enumerator = newEnumerator(ctx, planner);

    AtomicBoolean latch = latchOf(enumerator);
    ContinuousEnumerationResult noop = noopSentinel();
    Method discoverSplits = discoverSplitsMethod();
    Method processDiscoveredSplits = processDiscoveredSplitsMethod();

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      CountDownLatch ready = new CountDownLatch(2);
      CountDownLatch start = new CountDownLatch(1);
      List<Future<ContinuousEnumerationResult>> futures = Lists.newArrayList();
      for (int i = 0; i < 2; i++) {
        futures.add(
            pool.submit(
                () -> {
                  ready.countDown();
                  start.await();
                  return callDiscover(discoverSplits, enumerator);
                }));
      }

      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      start.countDown();

      // The winning thread enters the planner. Wait until we're certain it's inside, then give
      // the loser a generous window to reach its CAS check.
      assertThat(insidePlanner.await(5, TimeUnit.SECONDS)).isTrue();
      Thread.sleep(100);
      releasePlanner.countDown();

      ContinuousEnumerationResult r0 = futures.get(0).get(5, TimeUnit.SECONDS);
      ContinuousEnumerationResult r1 = futures.get(1).get(5, TimeUnit.SECONDS);

      // Exactly one thread should have received the sentinel; the other a real result.
      long noopCount = (r0 == noop ? 1 : 0) + (r1 == noop ? 1 : 0);
      assertThat(noopCount).isEqualTo(1);
      assertThat(plannerCalls.get()).isEqualTo(1);
      assertThat(peakInsidePlanner.get()).isEqualTo(1);
      // Latch still held until the winning handler runs.
      assertThat(latch.get()).isTrue();

      // Dispatch handlers in either order: NOOP first, then real. The latch must end up
      // released regardless of order.
      ContinuousEnumerationResult real = r0 == noop ? r1 : r0;
      processDiscoveredSplits.invoke(enumerator, noop, null);
      assertThat(latch.get()).isTrue();
      processDiscoveredSplits.invoke(enumerator, real, null);
      assertThat(latch.get()).isFalse();

      // Reverse handler order (real first, NOOP second) must also leave the latch released.
      assertReverseHandlerOrderReleasesLatch(
          ctx, pool, discoverSplits, processDiscoveredSplits, noop);
    } finally {
      pool.shutdownNow();
    }
  }

  private void assertReverseHandlerOrderReleasesLatch(
      TestingSplitEnumeratorContext<IcebergSourceSplit> ctx,
      ExecutorService pool,
      Method discoverSplits,
      Method processDiscoveredSplits,
      ContinuousEnumerationResult noop)
      throws Exception {
    CountDownLatch insideAgain = new CountDownLatch(1);
    CountDownLatch releaseAgain = new CountDownLatch(1);
    ContinuousSplitPlanner planner2 =
        new ContinuousSplitPlanner() {
          @Override
          public ContinuousEnumerationResult planSplits(IcebergEnumeratorPosition pos) {
            insideAgain.countDown();
            try {
              releaseAgain.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return new ContinuousEnumerationResult(
                Collections.emptyList(), pos, IcebergEnumeratorPosition.empty());
          }

          @Override
          public int recommendedLowWatermark() {
            return 100;
          }

          @Override
          public void close() {}
        };
    ContinuousIcebergEnumerator enumerator2 = newEnumerator(ctx, planner2);
    AtomicBoolean latch2 = latchOf(enumerator2);

    CountDownLatch ready2 = new CountDownLatch(2);
    CountDownLatch start2 = new CountDownLatch(1);
    List<Future<ContinuousEnumerationResult>> futures = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      futures.add(
          pool.submit(
              () -> {
                ready2.countDown();
                start2.await();
                return callDiscover(discoverSplits, enumerator2);
              }));
    }
    assertThat(ready2.await(5, TimeUnit.SECONDS)).isTrue();
    start2.countDown();
    assertThat(insideAgain.await(5, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(100);
    releaseAgain.countDown();
    ContinuousEnumerationResult s0 = futures.get(0).get(5, TimeUnit.SECONDS);
    ContinuousEnumerationResult s1 = futures.get(1).get(5, TimeUnit.SECONDS);
    ContinuousEnumerationResult realResult2 = s0 == noop ? s1 : s0;
    // Real handler first.
    processDiscoveredSplits.invoke(enumerator2, realResult2, null);
    assertThat(latch2.get()).isFalse();
    // NOOP handler second — must not re-flip the latch.
    processDiscoveredSplits.invoke(enumerator2, noop, null);
    assertThat(latch2.get()).isFalse();
  }

  @Test
  public void testLatchSurvivesPlanSplitsExceptionUnderConcurrency() throws Exception {
    // Race two threads: the winner enters the planner and throws; the loser gets the NOOP
    // sentinel. Then dispatch the exception handler — latch must release. Then a fresh
    // discoverSplits call must succeed (no leftover latch from the prior failure path).
    CountDownLatch insidePlanner = new CountDownLatch(1);
    CountDownLatch releasePlanner = new CountDownLatch(1);
    AtomicInteger plannerCalls = new AtomicInteger();

    ContinuousSplitPlanner planner =
        new ContinuousSplitPlanner() {
          @Override
          public ContinuousEnumerationResult planSplits(IcebergEnumeratorPosition lastPosition) {
            plannerCalls.incrementAndGet();
            insidePlanner.countDown();
            try {
              releasePlanner.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            throw new RuntimeException("boom");
          }

          @Override
          public int recommendedLowWatermark() {
            return 100;
          }

          @Override
          public void close() {}
        };

    TestingSplitEnumeratorContext<IcebergSourceSplit> ctx = new TestingSplitEnumeratorContext<>(1);
    ContinuousIcebergEnumerator enumerator = newEnumerator(ctx, planner);

    AtomicBoolean latch = latchOf(enumerator);
    ContinuousEnumerationResult noop = noopSentinel();
    Method discoverSplits = discoverSplitsMethod();
    Method processDiscoveredSplits = processDiscoveredSplitsMethod();

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      CountDownLatch ready = new CountDownLatch(2);
      CountDownLatch start = new CountDownLatch(1);
      List<Future<Object>> futures = Lists.newArrayList();
      for (int i = 0; i < 2; i++) {
        futures.add(
            pool.submit(
                () -> {
                  ready.countDown();
                  start.await();
                  try {
                    return callDiscover(discoverSplits, enumerator);
                  } catch (Throwable t) {
                    return t;
                  }
                }));
      }
      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      start.countDown();
      assertThat(insidePlanner.await(5, TimeUnit.SECONDS)).isTrue();
      Thread.sleep(100);
      releasePlanner.countDown();

      Object o0 = futures.get(0).get(5, TimeUnit.SECONDS);
      Object o1 = futures.get(1).get(5, TimeUnit.SECONDS);

      // One thread saw the planner throw; the other got the NOOP sentinel.
      long throwCount =
          (o0 instanceof RuntimeException ? 1 : 0) + (o1 instanceof RuntimeException ? 1 : 0);
      long noopCount = (o0 == noop ? 1 : 0) + (o1 == noop ? 1 : 0);
      assertThat(throwCount).isEqualTo(1);
      assertThat(noopCount).isEqualTo(1);
      assertThat(plannerCalls.get()).isEqualTo(1);

      // Critical invariant: latch is STILL held after the planner threw. discoverSplits
      // must not release on exception — only the handler does. Otherwise a concurrent
      // CAS from a third caller could acquire between (a) the catch-side release and
      // (b) the handler-side release, causing the handler to wrongly clear the third
      // caller's freshly-acquired latch.
      assertThat(latch.get()).isTrue();

      // Frameworks dispatches handler(null, exception); finally releases the latch.
      processDiscoveredSplits.invoke(enumerator, null, new RuntimeException("boom"));
      assertThat(latch.get()).isFalse();
      // NOOP handler is independent — also a no-op for the released latch.
      processDiscoveredSplits.invoke(enumerator, noop, null);
      assertThat(latch.get()).isFalse();

      // The latch is healthy: a fresh discoverSplits acquires again.
      // (Swap in a non-blocking planner so the call returns immediately.)
      ContinuousSplitPlanner happyPlanner =
          trackingPlanner(new AtomicInteger(), new AtomicInteger());
      ContinuousIcebergEnumerator enumerator2 = newEnumerator(ctx, happyPlanner);
      AtomicBoolean latch2 = latchOf(enumerator2);
      ContinuousEnumerationResult fresh = callDiscover(discoverSplits, enumerator2);
      assertThat(fresh).isNotSameAs(noop);
      assertThat(latch2.get()).isTrue();
      processDiscoveredSplits.invoke(enumerator2, fresh, null);
      assertThat(latch2.get()).isFalse();
    } finally {
      pool.shutdownNow();
    }
  }

  private static ContinuousSplitPlanner trackingPlanner(
      AtomicInteger callCount, AtomicInteger throwsRemaining) {
    return new ContinuousSplitPlanner() {
      @Override
      public ContinuousEnumerationResult planSplits(IcebergEnumeratorPosition lastPosition) {
        callCount.incrementAndGet();
        if (throwsRemaining.get() > 0) {
          throwsRemaining.decrementAndGet();
          throw new RuntimeException("simulated planner failure");
        }
        return new ContinuousEnumerationResult(
            Collections.emptyList(), lastPosition, IcebergEnumeratorPosition.empty());
      }

      @Override
      public int recommendedLowWatermark() {
        return 100;
      }

      @Override
      public void close() throws IOException {}
    };
  }
}
