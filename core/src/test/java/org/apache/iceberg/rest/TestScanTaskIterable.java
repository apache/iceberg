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
package org.apache.iceberg.rest;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestScanTaskIterable {

  private static final TableIdentifier TABLE_IDENTIFIER =
      TableIdentifier.of(Namespace.of("ns"), "table");
  private static final String FETCH_TASKS_PATH = "v1/namespaces/ns/tables/table/tasks";
  private static final Map<String, String> HEADERS =
      ImmutableMap.of("Authorization", "Bearer token");

  private RESTClient mockClient;
  private ResourcePaths resourcePaths;
  private ExecutorService executorService;
  private ParserContext parserContext;

  @BeforeEach
  public void before() {
    mockClient = mock(RESTClient.class);
    resourcePaths = ResourcePaths.forCatalogProperties(ImmutableMap.of());
    executorService = Executors.newFixedThreadPool(4);
    parserContext = ParserContext.builder().build();
  }

  @AfterEach
  public void after() {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  private void assertIteratorThrows(CloseableIterator<FileScanTask> iterator, String errorPattern) {
    assertThatThrownBy(iterator::hasNext)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(errorPattern);
  }

  private List<String> planTasks(int count) {
    return IntStream.range(0, count).mapToObj(i -> "planTask" + i).collect(toList());
  }

  private List<FileScanTask> fileTasks(int count) {
    return IntStream.range(1, count + 1).mapToObj(i -> new MockFileScanTask(i)).collect(toList());
  }

  private List<FileScanTask> collectAll(CloseableIterator<FileScanTask> iterator)
      throws IOException {
    try (iterator) {
      return Lists.newArrayList(iterator);
    }
  }

  private ScanTaskIterable createIterable(List<String> planTasks, List<FileScanTask> initialTasks) {
    return new ScanTaskIterable(
        planTasks,
        initialTasks,
        mockClient,
        resourcePaths,
        TABLE_IDENTIFIER,
        HEADERS,
        executorService,
        parserContext);
  }

  private void mockClientPost(FetchScanTasksResponse... responses) {
    if (responses.length == 1) {
      when(mockClient.post(
              eq(FETCH_TASKS_PATH),
              any(FetchScanTasksRequest.class),
              eq(FetchScanTasksResponse.class),
              eq(HEADERS),
              any(),
              any(),
              eq(parserContext)))
          .thenReturn(responses[0]);
    } else {
      when(mockClient.post(
              eq(FETCH_TASKS_PATH),
              any(FetchScanTasksRequest.class),
              eq(FetchScanTasksResponse.class),
              eq(HEADERS),
              any(),
              any(),
              eq(parserContext)))
          .thenReturn(responses[0], java.util.Arrays.copyOfRange(responses, 1, responses.length));
    }
  }

  private void mockClientPostAnswer(org.mockito.stubbing.Answer<FetchScanTasksResponse> answer) {
    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenAnswer(answer);
  }

  // ==================== Nested/Paginated Plan Tasks Tests ====================

  @Test
  public void iterableWithNestedPlanTasks() throws IOException {
    // First plan task returns more plan tasks
    FetchScanTasksResponse response1 =
        FetchScanTasksResponse.builder()
            .withPlanTasks(ImmutableList.of("nestedPlanTask1", "nestedPlanTask2"))
            .withFileScanTasks(ImmutableList.of(new MockFileScanTask(100)))
            .build();

    FetchScanTasksResponse response2 =
        FetchScanTasksResponse.builder()
            .withFileScanTasks(ImmutableList.of(new MockFileScanTask(200)))
            .build();
    FetchScanTasksResponse response3 =
        FetchScanTasksResponse.builder()
            .withFileScanTasks(ImmutableList.of(new MockFileScanTask(300)))
            .build();

    mockClientPost(response1, response2, response3);

    ScanTaskIterable iterable =
        createIterable(ImmutableList.of("planTask1"), Collections.emptyList());

    List<FileScanTask> result = collectAll(iterable.iterator());
    assertThat(result).hasSize(3);
    assertThat(result).extracting(FileScanTask::length).containsExactlyInAnyOrder(100L, 200L, 300L);

    verify(mockClient, times(3))
        .post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext));
  }

  @Test
  public void iterableWithDeeplyNestedPlanTasks() throws IOException {
    FetchScanTasksResponse response1 =
        FetchScanTasksResponse.builder().withPlanTasks(ImmutableList.of("level2")).build();
    FetchScanTasksResponse response2 =
        FetchScanTasksResponse.builder().withPlanTasks(ImmutableList.of("level3")).build();
    FetchScanTasksResponse response3 =
        FetchScanTasksResponse.builder()
            .withFileScanTasks(ImmutableList.of(new MockFileScanTask(100)))
            .build();

    mockClientPost(response1, response2, response3);

    ScanTaskIterable iterable = createIterable(ImmutableList.of("level1"), Collections.emptyList());

    List<FileScanTask> result = collectAll(iterable.iterator());
    assertThat(result).hasSize(1);
    assertThat(result.get(0).length()).isEqualTo(100L);
  }

  // ==================== Iterator Behavior Tests ====================

  @Test
  public void iteratorNextWithoutHasNext() throws IOException {
    ScanTaskIterable iterable = createIterable(null, ImmutableList.of(new MockFileScanTask(100)));

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      FileScanTask task = iterator.next();
      assertThat(task.length()).isEqualTo(100L);
      assertThatThrownBy(iterator::next)
          .isInstanceOf(NoSuchElementException.class)
          .hasMessage("No more scan tasks available");
    }
  }

  @Test
  public void iteratorMultipleHasNextCallsIdempotent() throws IOException {
    ScanTaskIterable iterable = createIterable(null, ImmutableList.of(new MockFileScanTask(100)));

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      // Multiple hasNext() calls should be idempotent
      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.hasNext()).isTrue();

      FileScanTask task = iterator.next();
      assertThat(task.length()).isEqualTo(100L);

      assertThat(iterator.hasNext()).isFalse();
      assertThat(iterator.hasNext()).isFalse();
    }
  }

  // ==================== Error Handling Tests ====================

  @Test
  public void workerFailurePropagatesException() throws IOException {
    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenThrow(new RuntimeException("Network error"));

    ScanTaskIterable iterable =
        createIterable(ImmutableList.of("planTask1"), Collections.emptyList());

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      assertIteratorThrows(iterator, "Worker failed");
    }
  }

  // ==================== Chained Plan Tasks Test ====================

  @Test
  public void chainedPlanTasks() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    mockClientPostAnswer(
        invocation -> {
          int count = callCount.incrementAndGet();
          if (count <= 3) {
            return FetchScanTasksResponse.builder()
                .withPlanTasks(ImmutableList.of("chainedTask" + count))
                .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 100L)))
                .build();
          } else {
            return FetchScanTasksResponse.builder()
                .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 100L)))
                .build();
          }
        });

    ScanTaskIterable iterable =
        createIterable(ImmutableList.of("initialTask"), Collections.emptyList());

    List<FileScanTask> result = collectAll(iterable.iterator());
    assertThat(result).hasSize(4);
  }

  // ==================== Concurrency Tests ====================

  @Test
  public void concurrentWorkersProcessingTasks() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    mockClientPostAnswer(
        invocation -> {
          int count = callCount.incrementAndGet();
          // Simulate some network latency
          Thread.sleep(10);
          return FetchScanTasksResponse.builder()
              .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 100)))
              .build();
        });

    // Create many plan tasks to trigger multiple workers
    ScanTaskIterable iterable = createIterable(planTasks(50), Collections.emptyList());

    List<FileScanTask> result = collectAll(iterable.iterator());
    assertThat(result).hasSize(50);

    // All plan tasks should have been processed exactly once
    assertThat(callCount.get()).isEqualTo(50);
  }

  @Test
  public void slowProducerFastConsumer() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    mockClientPostAnswer(
        invocation -> {
          // Slow producer - simulate network delay
          Thread.sleep(50);
          int count = callCount.incrementAndGet();
          return FetchScanTasksResponse.builder()
              .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 100)))
              .build();
        });

    ScanTaskIterable iterable = createIterable(planTasks(3), Collections.emptyList());

    List<FileScanTask> result = collectAll(iterable.iterator());
    assertThat(result).hasSize(3);
  }

  @Test
  public void closeWhileWorkersAreRunning() throws IOException, InterruptedException {
    CountDownLatch workerStarted = new CountDownLatch(1);

    mockClientPostAnswer(
        invocation -> {
          workerStarted.countDown();
          // Simulate a very slow network call
          Thread.sleep(5000);
          return FetchScanTasksResponse.builder()
              .withFileScanTasks(ImmutableList.of(new MockFileScanTask(100)))
              .build();
        });

    ScanTaskIterable iterable =
        createIterable(ImmutableList.of("planTask1"), Collections.emptyList());

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      // Wait for worker to start
      workerStarted.await(1, TimeUnit.SECONDS);
      // Close immediately - should not block
      iterator.close();
      // hasNext should return false after close
      assertThat(iterator.hasNext()).isFalse();
    }
  }

  @Test
  public void multipleWorkersWithMixedNestedPlanTasks() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    mockClientPostAnswer(
        invocation -> {
          int count = callCount.incrementAndGet();
          // First few calls return nested plan tasks to generate more work
          if (count <= 3) {
            return FetchScanTasksResponse.builder()
                .withPlanTasks(
                    ImmutableList.of("nestedTask" + count + "a", "nestedTask" + count + "b"))
                .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 100)))
                .build();
          } else {
            return FetchScanTasksResponse.builder()
                .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 100)))
                .build();
          }
        });

    ScanTaskIterable iterable = createIterable(planTasks(3), Collections.emptyList());

    List<FileScanTask> result = collectAll(iterable.iterator());
    // 3 initial tasks + 6 nested tasks = 9 total
    assertThat(result).hasSize(9);
  }

  @Test
  public void initialFileScanTasksWithConcurrentPlanTasks() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    mockClientPostAnswer(
        invocation -> {
          int count = callCount.incrementAndGet();
          // Simulate network delay
          Thread.sleep(20);
          return FetchScanTasksResponse.builder()
              .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 1000)))
              .build();
        });

    // Initial tasks should be available immediately while plan tasks are being fetched
    ScanTaskIterable iterable = createIterable(planTasks(10), fileTasks(10));

    List<FileScanTask> result = collectAll(iterable.iterator());
    // 10 initial + 10 from plan tasks = 20 total
    assertThat(result).hasSize(20);
  }

  @Test
  public void workerExceptionDoesNotBlockOtherTasks() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    mockClientPostAnswer(
        invocation -> {
          int count = callCount.incrementAndGet();
          if (count == 1) {
            // First call fails
            throw new RuntimeException("First call failed");
          }
          return FetchScanTasksResponse.builder()
              .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 100)))
              .build();
        });

    ScanTaskIterable iterable =
        createIterable(ImmutableList.of("planTask1"), Collections.emptyList());

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      assertIteratorThrows(iterator, "Worker failed");
    }
  }

  @Test
  public void multipleWorkerFailuresOnlySignalOnce() throws IOException {
    // This test verifies that when multiple workers fail, only one DUMMY_TASK is added
    // and the iterator correctly propagates the first failure without hanging
    AtomicInteger callCount = new AtomicInteger(0);

    mockClientPostAnswer(
        invocation -> {
          int count = callCount.incrementAndGet();
          // Add small delay to allow multiple workers to start
          Thread.sleep(10);
          throw new RuntimeException("Worker " + count + " failed");
        });

    // Create multiple plan tasks so multiple workers can pick them up and fail
    ScanTaskIterable iterable = createIterable(planTasks(10), Collections.emptyList());

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      // Should propagate the first failure without hanging
      assertIteratorThrows(iterator, "Worker failed");

      // Subsequent calls should also throw (not hang waiting for more DUMMY_TASKs)
      assertIteratorThrows(iterator, "Worker failed");
    }
  }

  @Test
  public void workerExceptionWithFullQueueDoesNotHangOtherWorkers() throws Exception {
    // This test verifies that when one worker fails and the consumer throws (stops draining),
    // other workers don't hang indefinitely trying to put tasks into the full queue.
    // Key: consumer does NOT call close() - it just blows up on the exception.
    CountDownLatch firstWorkerStarted = new CountDownLatch(1);
    CountDownLatch failureTriggered = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger(0);

    mockClientPostAnswer(
        invocation -> {
          int count = callCount.incrementAndGet();
          if (count == 1) {
            firstWorkerStarted.countDown();
            // First worker returns MORE tasks than queue capacity (1000) to ensure it blocks on
            // offer().Also adds more plan tasks so other workers have work.
            return FetchScanTasksResponse.builder()
                .withPlanTasks(planTasks(5))
                .withFileScanTasks(fileTasks(1500))
                .build();
          } else if (count == 2) {
            // Second worker waits for first worker to start blocking on full queue, then fails.
            // This sets shutdown=true, which should unblock worker 1.
            Thread.sleep(200);
            failureTriggered.countDown();
            throw new RuntimeException("Worker failed");
          } else {
            // Other workers also return many tasks - they may also block on full queue
            return FetchScanTasksResponse.builder().withFileScanTasks(fileTasks(1500)).build();
          }
        });

    ScanTaskIterable iterable = createIterable(planTasks(3), Collections.emptyList());

    // Intentionally NOT using try-with-resources - we don't want close() called
    CloseableIterator<FileScanTask> iterator = iterable.iterator();
    try {
      // Wait for first worker to start filling the queue
      firstWorkerStarted.await(5, TimeUnit.SECONDS);

      // Consume just a few tasks
      int consumed = 0;
      while (consumed < 5) {
        if (iterator.hasNext()) {
          iterator.next();
          consumed++;
        }
      }

      // Wait for failure to be triggered
      failureTriggered.await(5, TimeUnit.SECONDS);

      // Verify at least 2 workers ran. Worker 1 produced 1500 tasks (queue capacity is 1000),
      // so worker 1 should be blocked on offer() when worker 2 fails and sets shutdown=true.
      assertThat(callCount.get()).isGreaterThanOrEqualTo(2);

      // Give time for failure to propagate
      Thread.sleep(200);

      // This hasNext() should throw due to worker failure
      iterator.hasNext();

      // Should not reach here
      assertThat(false).as("Expected RuntimeException from hasNext()").isTrue();
    } catch (RuntimeException e) {
      // Expected - consumer blows up, does NOT call close()
      assertThat(e.getMessage()).contains("Worker failed");
    }
    // Note: iterator.close() is intentionally NOT called
    // Give workers time to see the shutdown flag (set by failing worker) and exit.
    Thread.sleep(500);

    // Verify executor can shut down cleanly (workers aren't stuck on offer())
    executorService.shutdown();
    boolean terminated = executorService.awaitTermination(2, TimeUnit.SECONDS);
    assertThat(terminated)
        .as("Executor should terminate - workers should have exited via shutdown flag")
        .isTrue();
  }

  @Test
  public void closeWithFullQueueDoesNotHangWorkers() throws Exception {
    // This test verifies that when the queue is full and the consumer closes the iterator,
    // workers don't hang indefinitely trying to put tasks into the full queue.
    // The queue capacity is 1000, so we need to generate more tasks than that.
    CountDownLatch workerStarted = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger(0);

    mockClientPostAnswer(
        invocation -> {
          workerStarted.countDown();
          int count = callCount.incrementAndGet();
          // Each response returns many file scan tasks to fill the queue quickly,
          // plus more plan tasks to keep workers busy
          if (count <= 10) {
            return FetchScanTasksResponse.builder()
                .withPlanTasks(planTasks(2))
                .withFileScanTasks(fileTasks(500))
                .build();
          }
          return FetchScanTasksResponse.builder().withFileScanTasks(fileTasks(500)).build();
        });

    ScanTaskIterable iterable = createIterable(planTasks(2), Collections.emptyList());

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      // Wait for workers to start producing
      workerStarted.await(5, TimeUnit.SECONDS);

      // Consume a few tasks to let the queue fill up
      int consumed = 0;
      while (iterator.hasNext() && consumed < 10) {
        iterator.next();
        consumed++;
      }

      // Give workers time to fill the queue (each worker produces 500 tasks, queue capacity is
      // 1000)
      Thread.sleep(200);

      // Verify enough workers ran to fill the queue beyond capacity.
      // With 500 tasks per call and queue capacity 1000, we need 3+ calls to overflow.
      assertThat(callCount.get()).isGreaterThanOrEqualTo(3);
    }
    // iterator.close() called here by try-with-resources

    // Give workers a bit more time to exit after consumer closed
    Thread.sleep(500);

    // Verify executor can shut down cleanly (workers aren't stuck on offer())
    executorService.shutdown();
    boolean terminated = executorService.awaitTermination(2, TimeUnit.SECONDS);
    assertThat(terminated)
        .as("Executor should terminate - workers should have exited gracefully")
        .isTrue();
  }
}
