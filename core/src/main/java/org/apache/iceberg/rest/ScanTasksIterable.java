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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScanTasksIterable implements CloseableIterable<FileScanTask> {

  private static final Logger LOG = LoggerFactory.getLogger(ScanTasksIterable.class);
  private static final int DEFAULT_TASK_QUEUE_CAPACITY = 1000;
  private static final long QUEUE_POLL_TIMEOUT_MS = 100;
  private static final int WORKER_POOL_SIZE = Math.max(1, ThreadPools.WORKER_THREAD_POOL_SIZE / 4);
  private final BlockingQueue<FileScanTask> taskQueue;
  private final ConcurrentLinkedQueue<FileScanTask> initialFileScanTasks;
  private final ConcurrentLinkedQueue<String> planTasks;
  private final AtomicInteger activeWorkers = new AtomicInteger(0);
  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final ExecutorService executorService;
  private final RESTClient client;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Map<String, String> headers;
  private final ParserContext parserContext;
  private final Supplier<Boolean> cancellationCallback;

  ScanTasksIterable(
      List<String> initialPlanTasks,
      List<FileScanTask> initialFileScanTasks,
      RESTClient client,
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Map<String, String> headers,
      ExecutorService executorService,
      Map<Integer, PartitionSpec> specsById,
      boolean caseSensitive,
      Supplier<Boolean> cancellationCallback) {

    this.taskQueue = new LinkedBlockingQueue<>(DEFAULT_TASK_QUEUE_CAPACITY);
    this.planTasks = new ConcurrentLinkedQueue<>();
    // Initialize initial file scan tasks queue so that multiple workers can poll produce from it.
    this.initialFileScanTasks = new ConcurrentLinkedQueue<>(initialFileScanTasks);

    this.client = client;
    this.resourcePaths = resourcePaths;
    this.tableIdentifier = tableIdentifier;
    this.headers = headers;
    this.executorService = executorService;
    this.cancellationCallback = cancellationCallback;
    this.parserContext =
        ParserContext.builder()
            .add("specsById", specsById)
            .add("caseSensitive", caseSensitive)
            .build();

    if (initialPlanTasks != null && !initialPlanTasks.isEmpty()) {
      planTasks.addAll(initialPlanTasks);
    }

    submitFixedWorkers();
  }

  private void submitFixedWorkers() {
    if (planTasks.isEmpty() && initialFileScanTasks.isEmpty()) {
      // nothing to do
      return;
    }

    // need to spawn at least one worker to enqueue initial file scan tasks
    int numWorkers = Math.min(WORKER_POOL_SIZE, Math.max(planTasks.size(), 1));

    for (int i = 0; i < numWorkers; i++) {
      executorService.execute(new PlanTaskWorker());
    }
  }

  @Override
  public CloseableIterator<FileScanTask> iterator() {
    return new ScanTasksIterator();
  }

  @Override
  public void close() throws IOException {}

  private class PlanTaskWorker implements Runnable {

    @Override
    public void run() {
      activeWorkers.incrementAndGet();

      try {
        while (true) {
          if (shutdown.get()) {
            return;
          }

          String planTask = planTasks.poll();
          if (planTask == null) {
            // if there are no more plan tasks, see if we can just add any remaining initial
            // file scan tasks before exiting.
            while (!initialFileScanTasks.isEmpty()) {
              FileScanTask initialFileScanTask = initialFileScanTasks.poll();
              if (initialFileScanTask != null) {
                taskQueue.put(initialFileScanTask);
              }
            }
            return;
          }

          processPlanTask(planTask);
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        throw new RuntimeException("Worker failed processing planTask", e);
      } finally {
        int remaining = activeWorkers.decrementAndGet();

        if (remaining == 0
            && !planTasks.isEmpty()
            && !shutdown.get()
            && !initialFileScanTasks.isEmpty()) {
          executorService.execute(new PlanTaskWorker());
        }
      }
    }

    private void processPlanTask(String planTask) throws InterruptedException {
      FetchScanTasksResponse response = fetchScanTasks(planTask);
      // immediately add any new plan tasks to the queue so the idle workers can pick them up
      if (response.planTasks() != null) {
        planTasks.addAll(response.planTasks());
      }

      // before blocking on the task queue, check for shutdown again
      if (shutdown.get()) {
        return;
      }

      // Now since the network IO is done, first add any initial file scan tasks
      while (!initialFileScanTasks.isEmpty()) {
        FileScanTask initialFileScanTask = initialFileScanTasks.poll();
        if (initialFileScanTask != null) {
          taskQueue.put(initialFileScanTask);
        }
      }

      if (response.fileScanTasks() != null) {
        for (FileScanTask task : response.fileScanTasks()) {
          taskQueue.put(task);
        }
      }
    }

    private FetchScanTasksResponse fetchScanTasks(String planTask) {
      FetchScanTasksRequest request = new FetchScanTasksRequest(planTask);

      return client.post(
          resourcePaths.fetchScanTasks(tableIdentifier),
          request,
          FetchScanTasksResponse.class,
          headers,
          ErrorHandlers.defaultErrorHandler(),
          stringStringMap -> {},
          parserContext);
    }
  }

  private class ScanTasksIterator implements CloseableIterator<FileScanTask> {
    private FileScanTask nextTask = null;

    @Override
    public boolean hasNext() {
      if (nextTask != null) {
        return true;
      }

      while (true) {
        if (isDone()) {
          return false;
        }

        try {
          nextTask = taskQueue.poll(QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

          if (nextTask == null) {
            // it may happen the iterator is called when there are still initial file scan tasks
            // waiting to be added.
            nextTask = initialFileScanTasks.poll();
          }

          if (nextTask != null) {
            return true;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }

    @Override
    public FileScanTask next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more scan tasks available");
      }
      FileScanTask result = nextTask;
      nextTask = null;
      return result;
    }

    @Override
    public void close() {
      shutdown.set(true);
      if (cancellationCallback != null) {
        try {
          @SuppressWarnings("unused")
          Boolean ignored = cancellationCallback.get();
        } catch (Exception e) {
          // Ignore cancellation failures
        }
      }
      LOG.info(
          "ScanTasksIterator is closing. Clearing {} queued tasks and {} plan tasks.",
          taskQueue.size(),
          planTasks.size());
      taskQueue.clear();
      planTasks.clear();
    }

    private boolean isDone() {
      return taskQueue.isEmpty()
          && planTasks.isEmpty()
          && activeWorkers.get() == 0
          && initialFileScanTasks.isEmpty();
    }
  }
}
