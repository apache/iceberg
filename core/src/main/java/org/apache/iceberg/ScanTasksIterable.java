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

public class ScanTasksIterable implements CloseableIterable<FileScanTask> {
  private final RESTClient client;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Supplier<Map<String, String>> headers;
  private final ExecutorService executorService;
  private final Map<Integer, PartitionSpec> specsById;
  private final boolean caseSensitive;

  // Either planTask OR fileScanTasks will be non-null, never both
  private final String planTask;
  private final ArrayDeque<FileScanTask> fileScanTasks;

  private volatile boolean closed = false;

  public ScanTasksIterable(
      String planTask,
      RESTClient client,
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Supplier<Map<String, String>> headers,
      ExecutorService executorService,
      Map<Integer, PartitionSpec> specsById,
      boolean caseSensitive) {
    this(
        planTask,
        null,
        client,
        resourcePaths,
        tableIdentifier,
        headers,
        executorService,
        specsById,
        caseSensitive);
  }

  public ScanTasksIterable(
      List<FileScanTask> fileScanTasks,
      RESTClient client,
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Supplier<Map<String, String>> headers,
      ExecutorService executorService,
      Map<Integer, PartitionSpec> specsById,
      boolean caseSensitive) {
    this(
        null,
        fileScanTasks,
        client,
        resourcePaths,
        tableIdentifier,
        headers,
        executorService,
        specsById,
        caseSensitive);
  }

  private ScanTasksIterable(
      String planTask,
      List<FileScanTask> fileScanTasks,
      RESTClient client,
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Supplier<Map<String, String>> headers,
      ExecutorService executorService,
      Map<Integer, PartitionSpec> specsById,
      boolean caseSensitive) {
    this.planTask = planTask;
    this.fileScanTasks = fileScanTasks != null ? new ArrayDeque<>(fileScanTasks) : null;
    this.client = client;
    this.resourcePaths = resourcePaths;
    this.tableIdentifier = tableIdentifier;
    this.headers = headers;
    this.executorService = executorService;
    this.specsById = specsById;
    this.caseSensitive = caseSensitive;
  }

  @Override
  public CloseableIterator<FileScanTask> iterator() {
    validateState();
    if (closed) {
      throw new IllegalStateException("ScanTasksIterable has been closed");
    }
    return new ScanTasksIterator(
        planTask,
        fileScanTasks,
        client,
        resourcePaths,
        tableIdentifier,
        headers,
        executorService,
        specsById,
        caseSensitive);
  }

  private void validateState() {
    if (planTask == null && fileScanTasks == null) {
      throw new IllegalStateException("Either planTask or fileScanTasks must be non-null");
    }
    if (planTask != null && fileScanTasks != null) {
      throw new IllegalStateException("Only one of planTask or fileScanTasks should be non-null");
    }
  }

  @Override
  public void close() {
    closed = true;
    // Clear any pre-existing file scan tasks to free memory
    if (fileScanTasks != null) {
      fileScanTasks.clear();
    }
  }

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
    private final ParserContext parserContext;

    private volatile boolean closed = false;

    @SuppressWarnings("unused")
    ScanTasksIterator(
        String planTask,
        ArrayDeque<FileScanTask> fileScanTasks,
        RESTClient client,
        ResourcePaths resourcePaths,
        TableIdentifier tableIdentifier,
        Supplier<Map<String, String>> headers,
        ExecutorService executorService,
        Map<Integer, PartitionSpec> specsById,
        boolean caseSensitive) {
      this.client = client;
      this.resourcePaths = resourcePaths;
      this.tableIdentifier = tableIdentifier;
      this.headers = headers;
      this.planTask = planTask;
      this.fileScanTasks = fileScanTasks != null ? fileScanTasks : new ArrayDeque<>();
      this.executorService = executorService;
      this.specsById = specsById;
      this.caseSensitive = caseSensitive;
      // Create ParserContext once to avoid recreation on each fetchScanTasks call
      this.parserContext =
          ParserContext.builder()
              .add("specsById", specsById)
              .add("caseSensitive", caseSensitive)
              .build();
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        return false;
      }

      // Return true if we have tasks queued
      if (!fileScanTasks.isEmpty()) {
        return true;
      }

      // If we have a plan task to process, fetch its scan tasks
      if (planTask != null) {
        fetchScanTasks(planTask);
        planTask = null; // Mark as processed
        return !fileScanTasks.isEmpty(); // Return true if fetch added tasks
      }

      // No more tasks available
      return false;
    }

    @Override
    public FileScanTask next() {
      if (closed) {
        throw new IllegalStateException("Iterator has been closed");
      }
      if (fileScanTasks.isEmpty()) {
        throw new java.util.NoSuchElementException("No more scan tasks available");
      }
      return fileScanTasks.removeFirst();
    }

    private void fetchScanTasks(String withPlanTask) {
      if (closed || withPlanTask == null || withPlanTask.trim().isEmpty()) {
        return;
      }

      FetchScanTasksRequest fetchScanTasksRequest = new FetchScanTasksRequest(withPlanTask);
      FetchScanTasksResponse response =
          client.post(
              resourcePaths.fetchScanTasks(tableIdentifier),
              fetchScanTasksRequest,
              FetchScanTasksResponse.class,
              headers.get(),
              ErrorHandlers.defaultErrorHandler(),
              ignored -> {},
              parserContext);

      List<FileScanTask> responseTasks = response.fileScanTasks();
      if (responseTasks != null && !responseTasks.isEmpty()) {
        fileScanTasks.addAll(responseTasks);
      }

      List<String> responsePlanTasks = response.planTasks();
      if (responsePlanTasks != null) {
        processNestedPlanTasks(responsePlanTasks);
      }
    }

    private void processNestedPlanTasks(List<String> planTasks) {
      // Process nested plan tasks sequentially and add results to current queue
      for (String nestedPlanTask : planTasks) {
        if (closed) {
          return; // Exit early if closed
        }
        if (nestedPlanTask == null || nestedPlanTask.trim().isEmpty()) {
          continue;
        }

        ScanTasksIterable nestedIterable = createNestedScanTasksIterable(nestedPlanTask);
        try (CloseableIterator<FileScanTask> nestedIterator = nestedIterable.iterator()) {
          while (nestedIterator.hasNext() && !closed) {
            fileScanTasks.add(nestedIterator.next());
          }
        } catch (Exception e) {
          throw new RuntimeException("Failed to process nested plan task: " + nestedPlanTask, e);
        }
      }
    }

    private ScanTasksIterable createNestedScanTasksIterable(String nestedPlanTask) {
      return new ScanTasksIterable(
          nestedPlanTask,
          client,
          resourcePaths,
          tableIdentifier,
          headers,
          executorService,
          specsById,
          caseSensitive);
    }

    @Override
    public void close() {
      if (closed) {
        return; // Already closed
      }

      closed = true;

      // Clear remaining tasks to free memory
      fileScanTasks.clear();

      // Stop any further plan task processing
      planTask = null;
    }
  }
}
