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

  // ==================== Nested/Paginated Plan Tasks Tests ====================

  @Test
  public void testIterableWithNestedPlanTasks() throws IOException {
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

    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenReturn(response1, response2, response3);

    ScanTaskIterable iterable =
        new ScanTaskIterable(
            ImmutableList.of("planTask1"),
            Collections.emptyList(),
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      List<FileScanTask> result = Lists.newArrayList(iterator);
      assertThat(result).hasSize(3);
      assertThat(result)
          .extracting(FileScanTask::length)
          .containsExactlyInAnyOrder(100L, 200L, 300L);
    }

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
  public void testIterableWithDeeplyNestedPlanTasks() throws IOException {
    FetchScanTasksResponse response1 =
        FetchScanTasksResponse.builder().withPlanTasks(ImmutableList.of("level2")).build();
    FetchScanTasksResponse response2 =
        FetchScanTasksResponse.builder().withPlanTasks(ImmutableList.of("level3")).build();
    FetchScanTasksResponse response3 =
        FetchScanTasksResponse.builder()
            .withFileScanTasks(ImmutableList.of(new MockFileScanTask(100)))
            .build();

    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenReturn(response1, response2, response3);

    ScanTaskIterable iterable =
        new ScanTaskIterable(
            ImmutableList.of("level1"),
            Collections.emptyList(),
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      List<FileScanTask> result = Lists.newArrayList(iterator);
      assertThat(result).hasSize(1);
      assertThat(result.get(0).length()).isEqualTo(100L);
    }
  }

  // ==================== Iterator Behavior Tests ====================

  @Test
  public void testIteratorNextWithoutHasNext() throws IOException {
    List<FileScanTask> initialTasks = ImmutableList.of(new MockFileScanTask(100));

    ScanTaskIterable iterable =
        new ScanTaskIterable(
            null,
            initialTasks,
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      FileScanTask task = iterator.next();
      assertThat(task.length()).isEqualTo(100L);
      assertThatThrownBy(iterator::next)
          .isInstanceOf(NoSuchElementException.class)
          .hasMessage("No more scan tasks available");
    }
  }

  @Test
  public void testIteratorMultipleHasNextCallsIdempotent() throws IOException {
    List<FileScanTask> initialTasks = ImmutableList.of(new MockFileScanTask(100));

    ScanTaskIterable iterable =
        new ScanTaskIterable(
            null,
            initialTasks,
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

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
  public void testWorkerFailurePropagatesException() throws IOException {
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
        new ScanTaskIterable(
            ImmutableList.of("planTask1"),
            Collections.emptyList(),
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      // hasNext() should throw the worker failure
      assertThatThrownBy(iterator::hasNext)
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Worker failed");
    }
  }

  // ==================== Chained Plan Tasks Test ====================

  @Test
  public void testChainedPlanTasks() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenAnswer(
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
        new ScanTaskIterable(
            ImmutableList.of("initialTask"),
            Collections.emptyList(),
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      List<FileScanTask> result = Lists.newArrayList(iterator);
      assertThat(result).hasSize(4);
    }
  }

  // ==================== Concurrency Tests ====================

  @Test
  public void testConcurrentWorkersProcessingTasks() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenAnswer(
            invocation -> {
              int count = callCount.incrementAndGet();
              // Simulate some network latency
              Thread.sleep(10);
              return FetchScanTasksResponse.builder()
                  .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 100)))
                  .build();
            });

    // Create many plan tasks to trigger multiple workers
    List<String> planTasks = Lists.newArrayList();
    for (int i = 0; i < 50; i++) {
      planTasks.add("planTask" + i);
    }

    ScanTaskIterable iterable =
        new ScanTaskIterable(
            planTasks,
            Collections.emptyList(),
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      List<FileScanTask> result = Lists.newArrayList(iterator);
      assertThat(result).hasSize(50);
    }

    // All plan tasks should have been processed exactly once
    assertThat(callCount.get()).isEqualTo(50);
  }

  @Test
  public void testSlowProducerFastConsumer() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenAnswer(
            invocation -> {
              // Slow producer - simulate network delay
              Thread.sleep(50);
              int count = callCount.incrementAndGet();
              return FetchScanTasksResponse.builder()
                  .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 100)))
                  .build();
            });

    ScanTaskIterable iterable =
        new ScanTaskIterable(
            ImmutableList.of("planTask1", "planTask2", "planTask3"),
            Collections.emptyList(),
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      List<FileScanTask> result = Lists.newArrayList(iterator);
      assertThat(result).hasSize(3);
    }
  }

  @Test
  public void testCloseWhileWorkersAreRunning() throws IOException, InterruptedException {
    CountDownLatch workerStarted = new CountDownLatch(1);

    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenAnswer(
            invocation -> {
              workerStarted.countDown();
              // Simulate a very slow network call
              Thread.sleep(5000);
              return FetchScanTasksResponse.builder()
                  .withFileScanTasks(ImmutableList.of(new MockFileScanTask(100)))
                  .build();
            });

    ScanTaskIterable iterable =
        new ScanTaskIterable(
            ImmutableList.of("planTask1"),
            Collections.emptyList(),
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

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
  public void testMultipleWorkersWithMixedNestedPlanTasks() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenAnswer(
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

    ScanTaskIterable iterable =
        new ScanTaskIterable(
            ImmutableList.of("task1", "task2", "task3"),
            Collections.emptyList(),
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      List<FileScanTask> result = Lists.newArrayList(iterator);
      // 3 initial tasks + 6 nested tasks = 9 total
      assertThat(result).hasSize(9);
    }
  }

  @Test
  public void testInitialFileScanTasksWithConcurrentPlanTasks() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenAnswer(
            invocation -> {
              int count = callCount.incrementAndGet();
              // Simulate network delay
              Thread.sleep(20);
              return FetchScanTasksResponse.builder()
                  .withFileScanTasks(ImmutableList.of(new MockFileScanTask(count * 1000)))
                  .build();
            });

    // Initial tasks should be available immediately while plan tasks are being fetched
    List<FileScanTask> initialTasks = Lists.newArrayList();
    for (int i = 1; i <= 10; i++) {
      initialTasks.add(new MockFileScanTask(i));
    }

    List<String> planTasks = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      planTasks.add("planTask" + i);
    }

    ScanTaskIterable iterable =
        new ScanTaskIterable(
            planTasks,
            initialTasks,
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      List<FileScanTask> result = Lists.newArrayList(iterator);
      // 10 initial + 10 from plan tasks = 20 total
      assertThat(result).hasSize(20);
    }
  }

  @Test
  public void testWorkerExceptionDoesNotBlockOtherTasks() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    when(mockClient.post(
            eq(FETCH_TASKS_PATH),
            any(FetchScanTasksRequest.class),
            eq(FetchScanTasksResponse.class),
            eq(HEADERS),
            any(),
            any(),
            eq(parserContext)))
        .thenAnswer(
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
        new ScanTaskIterable(
            ImmutableList.of("planTask1"),
            Collections.emptyList(),
            mockClient,
            resourcePaths,
            TABLE_IDENTIFIER,
            HEADERS,
            executorService,
            parserContext);

    try (CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      // Should propagate the exception from the failed worker
      assertThatThrownBy(iterator::hasNext)
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Worker failed");
    }
  }
}
