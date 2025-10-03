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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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

  ScanTasksIterable(
      String planTask,
      RESTClient client,
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Supplier<Map<String, String>> headers,
      ExecutorService executorService,
      Map<Integer, PartitionSpec> specsById,
      boolean caseSensitive,
      Supplier<Boolean> cancellationCallback) {
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
      Supplier<Boolean> cancellationCallback) {
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
        cancellationCallback);
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
        Supplier<Boolean> cancellationCallback) {
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
      // so means we are finished
      return false;
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
        // This is the case where a plan task returned an additional plan task, so ensure that this
        // result is added to top level fileScanTasks list.
        Iterable<FileScanTask> fileScanTasksFromPlanTasks =
            getScanTasksIterable(response.planTasks());

        fileScanTasksFromPlanTasks.forEach(fileScanTasks::add);
      }
    }

    public CloseableIterable<FileScanTask> getScanTasksIterable(List<String> planTasks) {
      List<ScanTasksIterable> iterableOfScanTaskIterables = Lists.newArrayList();
      for (String withPlanTask : planTasks) {
        ScanTasksIterable iterable =
            new ScanTasksIterable(
                withPlanTask,
                client,
                resourcePaths,
                tableIdentifier,
                headers,
                executorService,
                specsById,
                caseSensitive,
                cancellationCallback);
        iterableOfScanTaskIterables.add(iterable);
      }
      return new ParallelIterable<>(iterableOfScanTaskIterables, executorService);
    }

    @Override
    public void close() throws IOException {
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
