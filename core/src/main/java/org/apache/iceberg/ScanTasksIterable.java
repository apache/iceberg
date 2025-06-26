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
package org.apache.iceberg;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.util.ParallelIterable;

class ScanTasksIterable implements CloseableIterable<FileScanTask> {
  private final RESTClient client;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Supplier<Map<String, String>> headers;
  // parallelizing on this where a planTask produces a list of file scan tasks, as
  //  well more planTasks.
  private final String planTask;
  private final ArrayDeque<FileScanTask> fileScanTasks;
  private final ExecutorService executorService;
  private final Map<Integer, PartitionSpec> specsById;
  private final boolean caseSensitive;
  private final Supplier<Boolean> cancellationCallback;
  private final ParallelIterable<FileScanTask> sharedParallelIterable;
  private final RESTTableScan.ScanTasksReferenceCounter referenceCounter;

  ScanTasksIterable(
      String planTask,
      RESTClient client,
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Supplier<Map<String, String>> headers,
      ExecutorService executorService,
      Map<Integer, PartitionSpec> specsById,
      boolean caseSensitive,
      Supplier<Boolean> cancellationCallback,
      ParallelIterable<FileScanTask> sharedParallelIterable,
      RESTTableScan.ScanTasksReferenceCounter referenceCounter) {
    this.planTask = planTask;
    this.fileScanTasks = null;
    this.client = client;
    this.resourcePaths = resourcePaths;
    this.tableIdentifier = tableIdentifier;
    this.headers = headers;
    this.executorService = executorService;
    this.specsById = specsById;
    this.caseSensitive = caseSensitive;
    this.cancellationCallback = cancellationCallback;
    this.sharedParallelIterable = sharedParallelIterable;
    this.referenceCounter = referenceCounter;
  }

  ScanTasksIterable(
      List<FileScanTask> fileScanTasks,
      RESTClient client,
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Supplier<Map<String, String>> headers,
      ExecutorService executorService,
      Map<Integer, PartitionSpec> specsById,
      boolean caseSensitive,
      Supplier<Boolean> cancellationCallback,
      ParallelIterable<FileScanTask> sharedParallelIterable,
      RESTTableScan.ScanTasksReferenceCounter referenceCounter) {
    this.planTask = null;
    this.fileScanTasks = new ArrayDeque<>(fileScanTasks);
    this.client = client;
    this.resourcePaths = resourcePaths;
    this.tableIdentifier = tableIdentifier;
    this.headers = headers;
    this.executorService = executorService;
    this.specsById = specsById;
    this.caseSensitive = caseSensitive;
    this.cancellationCallback = cancellationCallback;
    this.sharedParallelIterable = sharedParallelIterable;
    this.referenceCounter = referenceCounter;
  }

  @Override
  public CloseableIterator<FileScanTask> iterator() {
    return new ScanTasksIterator(
        planTask,
        fileScanTasks,
        client,
        resourcePaths,
        tableIdentifier,
        headers,
        executorService,
        specsById,
        caseSensitive,
        cancellationCallback,
        sharedParallelIterable,
        referenceCounter);
  }

  @Override
  public void close() throws IOException {}

  private static class ScanTasksIterator implements CloseableIterator<FileScanTask> {
    private final RESTClient client;
    private final ResourcePaths resourcePaths;
    private final TableIdentifier tableIdentifier;
    private final Supplier<Map<String, String>> headers;
    private String planTask;
    private final ArrayDeque<FileScanTask> fileScanTasks;
    private final ExecutorService executorService;
    private final Map<Integer, PartitionSpec> specsById;
    private final boolean caseSensitive;
    private final Supplier<Boolean> cancellationCallback;
    private final ParallelIterable<FileScanTask> sharedParallelIterable;
    private final RESTTableScan.ScanTasksReferenceCounter referenceCounter;

    ScanTasksIterator(
        String planTask,
        ArrayDeque<FileScanTask> fileScanTasks,
        RESTClient client,
        ResourcePaths resourcePaths,
        TableIdentifier tableIdentifier,
        Supplier<Map<String, String>> headers,
        ExecutorService executorService,
        Map<Integer, PartitionSpec> specsById,
        boolean caseSensitive,
        Supplier<Boolean> cancellationCallback,
        ParallelIterable<FileScanTask> sharedParallelIterable,
        RESTTableScan.ScanTasksReferenceCounter referenceCounter) {
      this.client = client;
      this.resourcePaths = resourcePaths;
      this.tableIdentifier = tableIdentifier;
      this.headers = headers;
      this.planTask = planTask;
      this.fileScanTasks = fileScanTasks != null ? fileScanTasks : new ArrayDeque<>();
      this.executorService = executorService;
      this.specsById = specsById;
      this.caseSensitive = caseSensitive;
      this.cancellationCallback = cancellationCallback;
      this.sharedParallelIterable = sharedParallelIterable;
      this.referenceCounter = referenceCounter;
    }

    @Override
    public boolean hasNext() {
      if (!fileScanTasks.isEmpty()) {
        // Have file scan tasks so continue to consume
        return true;
      }
      // Out of file scan tasks, so need to now fetch more from each planTask
      // Service can send back more planTasks which acts as pagination
      if (planTask != null) {
        fetchScanTasks(planTask);
        planTask = null;
        // Make another hasNext() call, as more fileScanTasks have been fetched
        return hasNext();
      }
      // we have no file scan tasks left to consume
      // so means we are finished - decrement reference counter
      decrementReferenceCounter();
      return false;
    }

    private void decrementReferenceCounter() {
      if (referenceCounter != null) {
        referenceCounter.decrement();
      }
    }

    @Override
    public FileScanTask next() {
      return fileScanTasks.removeFirst();
    }

    private void fetchScanTasks(String withPlanTask) {
      FetchScanTasksRequest fetchScanTasksRequest = new FetchScanTasksRequest(withPlanTask);
      ParserContext parserContext =
          ParserContext.builder()
              .add("specsById", specsById)
              .add("caseSensitive", caseSensitive)
              .build();
      FetchScanTasksResponse response =
          client.post(
              resourcePaths.fetchScanTasks(tableIdentifier),
              fetchScanTasksRequest,
              FetchScanTasksResponse.class,
              headers.get(),
              ErrorHandlers.defaultErrorHandler(),
              stringStringMap -> {},
              parserContext);
      if (response.fileScanTasks() != null) {
        fileScanTasks.addAll(response.fileScanTasks());
      }

      if (response.planTasks() != null) {
        // This is the case where a plan task returned additional plan tasks, so add them
        // to the shared ParallelIterable for processing
        for (String planTaskId : response.planTasks()) {
          ScanTasksIterable iterable =
              new ScanTasksIterable(
                  planTaskId,
                  client,
                  resourcePaths,
                  tableIdentifier,
                  headers,
                  executorService,
                  specsById,
                  caseSensitive,
                  cancellationCallback,
                  sharedParallelIterable,
                  referenceCounter);
          sharedParallelIterable.addIterable(iterable);
          // Increment reference counter for the new iterable
          if (referenceCounter != null) {
            referenceCounter.increment();
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      // Decrement reference counter when closing
      decrementReferenceCounter();

      // Cancel the plan if we have a cancellation callback
      if (cancellationCallback != null) {
        try {
          @SuppressWarnings("unused")
          Boolean ignored = cancellationCallback.get();
        } catch (Exception e) {
          // Log but don't fail the close - cancellation failures are not critical
        }
      }
    }
  }
}
