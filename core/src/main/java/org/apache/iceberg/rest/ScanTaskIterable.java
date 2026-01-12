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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScanTaskIterable implements CloseableIterable<FileScanTask> {

  private static final Logger LOG = LoggerFactory.getLogger(ScanTaskIterable.class);
  private static final int DEFAULT_TASK_QUEUE_CAPACITY = 1000;
  private static final long QUEUE_POLL_TIMEOUT_MS = 100;
  // Dummy task acts as a poison pill to indicate that there will be no more tasks
  private static final FileScanTask DUMMY_TASK = new BaseFileScanTask(null, null, null, null, null);
  private final AtomicReference<RuntimeException> failure = new AtomicReference<>(null);
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

  ScanTaskIterable(
      List<String> initialPlanTasks,
      List<FileScanTask> initialFileScanTasks,
      RESTClient client,
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Map<String, String> headers,
      ExecutorService executorService,
      ParserContext parserContext) {

    this.taskQueue = new LinkedBlockingQueue<>(DEFAULT_TASK_QUEUE_CAPACITY);
    this.planTasks = new ConcurrentLinkedQueue<>();
    // Initialize initial file scan tasks queue so that multiple workers can poll produce from it.
    this.initialFileScanTasks = new ConcurrentLinkedQueue<>(initialFileScanTasks);

    this.client = client;
    this.resourcePaths = resourcePaths;
    this.tableIdentifier = tableIdentifier;
    this.headers = headers;
    this.executorService = executorService;
    this.parserContext = parserContext;

    if (initialPlanTasks != null && !initialPlanTasks.isEmpty()) {
      planTasks.addAll(initialPlanTasks);
    } else if (initialFileScanTasks.isEmpty()) {
      // Add dummy task to indicate there is no work to be done.
      // Queue is empty at this point, so add() will never fail.
      taskQueue.add(DUMMY_TASK);
      return;
    }

    submitFixedWorkers();
  }

  private void submitFixedWorkers() {
    for (int i = 0; i < ThreadPools.WORKER_THREAD_POOL_SIZE; i++) {
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
        while (!shutdown.get() && !Thread.currentThread().isInterrupted()) {
          String planTask = planTasks.poll();
          if (planTask == null) {
            // if there are no more plan tasks, see if we can just add any remaining initial
            // file scan tasks before exiting.
            offerInitialFileScanTasks();
            return;
          }

          processPlanTask(planTask);
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        failure.compareAndSet(null, new RuntimeException("PlanWorker was interrupted", e));
        shutdown.set(true);
      } catch (Exception e) {
        failure.compareAndSet(null, new RuntimeException("Worker failed processing planTask", e));
        shutdown.set(true);
      } finally {
        handleWorkerExit();
      }
    }

    /**
     * Offers a task to the queue with timeout, periodically checking for shutdown. Returns true if
     * the task was successfully added, false if shutdown was requested. Throws InterruptedException
     * if the thread is interrupted while waiting.
     */
    private boolean offerWithTimeout(FileScanTask task) throws InterruptedException {
      while (!shutdown.get()) {
        if (taskQueue.offer(task, QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          return true;
        }
      }
      return false;
    }

    private void handleWorkerExit() {
      boolean isLastWorker = activeWorkers.decrementAndGet() == 0;
      boolean hasWorkLeft = !planTasks.isEmpty() || !initialFileScanTasks.isEmpty();
      boolean isShuttingDown = shutdown.get();

      if (isLastWorker && (!hasWorkLeft || isShuttingDown)) {
        signalCompletion();
      } else if (isLastWorker && hasWorkLeft) {
        failure.compareAndSet(
            null,
            new IllegalStateException("Workers have exited but there is still work to be done"));
        shutdown.set(true);
      }
    }

    private void signalCompletion() {
      try {
        // Use offer with timeout to avoid blocking indefinitely if queue is full and consumer
        // stopped draining. If shutdown is already set, consumer will exit via its shutdown check.
        offerWithTimeout(DUMMY_TASK);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // we should just shut down and not rethrow since we are trying to signal completion
        // its fine if we fail to put the dummy task in this case.
        shutdown.set(true);
      }
    }

    private void offerInitialFileScanTasks() throws InterruptedException {
      while (!initialFileScanTasks.isEmpty() && !Thread.currentThread().isInterrupted()) {
        FileScanTask initialFileScanTask = initialFileScanTasks.poll();
        if (initialFileScanTask != null) {
          if (!offerWithTimeout(initialFileScanTask)) {
            return;
          }
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
      offerInitialFileScanTasks();

      if (response.fileScanTasks() != null) {
        for (FileScanTask task : response.fileScanTasks()) {
          if (!offerWithTimeout(task)) {
            return;
          }
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
          ErrorHandlers.planTaskHandler(),
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

      boolean hasNext = false;
      while (!shutdown.get()) {
        try {
          nextTask = taskQueue.poll(QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (nextTask == DUMMY_TASK) {
            nextTask = null;
            shutdown.set(true); // Mark as done so while loop exits on subsequent calls
            break;
          } else if (nextTask != null) {
            hasNext = true;
            break;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          shutdown.set(true);
        }
      }

      RuntimeException workerFailure = failure.get();
      if (workerFailure != null) {
        throw workerFailure;
      }

      return hasNext;
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
      LOG.info(
          "ScanTasksIterator is closing. Clearing {} queued tasks and {} plan tasks.",
          taskQueue.size(),
          planTasks.size());
      taskQueue.clear();
      planTasks.clear();
    }
  }
}
