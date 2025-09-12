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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RESTTableScan extends DataTableScan implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RESTTableScan.class);

  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> headers;
  private final TableOperations operations;
  private final Table table;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final ParserContext parserContext;

  // Plan ID lifecycle management
  private final AtomicReference<String> activePlanId = new AtomicReference<>();

  // TODO revisit if this property should be configurable
  private static final int FETCH_PLANNING_SLEEP_DURATION_MS = 1000;

  RESTTableScan(
      Table table,
      Schema schema,
      TableScanContext context,
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      TableOperations operations,
      TableIdentifier tableIdentifier,
      ResourcePaths resourcePaths) {
    super(table, schema, context);
    this.table = table;
    this.client = client;
    this.headers = headers;
    this.path = path;
    this.operations = operations;
    this.tableIdentifier = tableIdentifier;
    this.resourcePaths = resourcePaths;
    this.parserContext =
        ParserContext.builder()
            .add("specsById", table.specs())
            .add("caseSensitive", context.caseSensitive())
            .build();
  }

  @Override
  protected TableScan newRefinedScan(
      Table refinedTable, Schema refinedSchema, TableScanContext refinedContext) {
    return new RESTTableScan(
        refinedTable,
        refinedSchema,
        refinedContext,
        client,
        path,
        headers,
        operations,
        tableIdentifier,
        resourcePaths);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    Long startSnapshotId = context().fromSnapshotId();
    Long endSnapshotId = context().toSnapshotId();
    Long snapshotId = snapshotId();
    List<String> selectedColumns =
        schema().columns().stream().map(Types.NestedField::name).collect(Collectors.toList());

    List<String> statsFields = null;
    if (columnsToKeepStats() != null) {
      statsFields =
          columnsToKeepStats().stream()
              .map(columnId -> schema().findColumnName(columnId))
              .collect(Collectors.toList());
    }

    PlanTableScanRequest.Builder planTableScanRequestBuilder =
        new PlanTableScanRequest.Builder()
            .withSelect(selectedColumns)
            .withFilter(filter())
            .withCaseSensitive(isCaseSensitive())
            .withStatsFields(statsFields);

    if (startSnapshotId != null && endSnapshotId != null) {
      planTableScanRequestBuilder
          .withStartSnapshotId(startSnapshotId)
          .withEndSnapshotId(endSnapshotId)
          .withUseSnapshotSchema(true);

    } else if (snapshotId != null) {
      boolean useSnapShotSchema = snapshotId != table.currentSnapshot().snapshotId();
      planTableScanRequestBuilder
          .withSnapshotId(snapshotId)
          .withUseSnapshotSchema(useSnapShotSchema);

    } else {
      planTableScanRequestBuilder.withSnapshotId(table().currentSnapshot().snapshotId());
    }

    return planTableScan(planTableScanRequestBuilder.build());
  }

  private CloseableIterable<FileScanTask> planTableScan(PlanTableScanRequest planTableScanRequest) {
    PlanTableScanResponse response =
        client.post(
            resourcePaths.planTableScan(tableIdentifier),
            planTableScanRequest,
            PlanTableScanResponse.class,
            headers.get(),
            ErrorHandlers.defaultErrorHandler(),
            stringStringMap -> {},
            parserContext);

    return handleInitialPlanStatus(response.planStatus(), response);
  }

  private CloseableIterable<FileScanTask> fetchPlanningResult(String planId) {
    activePlanId.set(planId);

    try {
      while (true) {
        FetchPlanningResultResponse response =
            client.get(
                resourcePaths.fetchPlanningResult(tableIdentifier, planId),
                Map.of(),
                FetchPlanningResultResponse.class,
                headers.get(),
                ErrorHandlers.defaultErrorHandler(),
                parserContext);

        CloseableIterable<FileScanTask> result =
            handlePlanningStatus(response.planStatus(), planId, response);
        if (result != null) {
          return result;
        }
      }
    } catch (Exception e) {
      // Ensure cleanup on any exception
      cancelPlanningWithId(planId);
      throw e;
    }
  }

  private CloseableIterable<FileScanTask> handlePlanningStatus(
      PlanStatus planStatus, String planId, FetchPlanningResultResponse response) {

    switch (planStatus) {
      case COMPLETED:
        activePlanId.compareAndSet(planId, null);
        return getScanTasksIterable(response.planTasks(), response.fileScanTasks());

      case SUBMITTED:
        handleSubmittedStatus(planId);
        return null; // Continue polling

      case FAILED:
        activePlanId.compareAndSet(planId, null);
        throw new RuntimeException(
            "Received \"failed\" status from service when fetching a table scan");

      case CANCELLED:
        activePlanId.compareAndSet(planId, null);
        throw new RuntimeException(
            String.format(
                Locale.ROOT,
                "Received \"cancelled\" status from service when fetching a table scan, planId: %s is invalid",
                planId));

      default:
        throw new RuntimeException(
            String.format(
                Locale.ROOT, "Invalid planStatus during fetchPlanningResult: %s", planStatus));
    }
  }

  private void handleSubmittedStatus(String planId) {
    try {
      Thread.sleep(FETCH_PLANNING_SLEEP_DURATION_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      cancelPlanningWithId(planId);
      throw new RuntimeException("Interrupted while fetching plan status", e);
    }
  }

  private CloseableIterable<FileScanTask> handleInitialPlanStatus(
      PlanStatus planStatus, PlanTableScanResponse response) {

    switch (planStatus) {
      case COMPLETED:
        return getScanTasksIterable(response.planTasks(), response.fileScanTasks());

      case SUBMITTED:
        return fetchPlanningResult(response.planId());

      case FAILED:
        throw new IllegalStateException(
            "Received \"failed\" status from service when planning a table scan");

      case CANCELLED:
        throw new IllegalStateException(
            "Received \"cancelled\" status from service when planning a table scan");

      default:
        throw new RuntimeException(
            String.format(Locale.ROOT, "Invalid planStatus during planTableScan: %s", planStatus));
    }
  }

  public CloseableIterable<FileScanTask> getScanTasksIterable(
      List<String> planTasks, List<FileScanTask> fileScanTasks) {

    if (isInputEmpty(planTasks, fileScanTasks)) {
      return CloseableIterable.empty();
    }

    validateDependencies();

    try {
      List<ScanTasksIterable> iterables = Lists.newArrayList();

      addFileScanTaskIterables(fileScanTasks, iterables);
      addPlanTaskIterables(planTasks, iterables);

      return combineIterables(iterables);

    } catch (Exception e) {
      LOG.error("Failed to create scan tasks iterable", e);
      throw new RuntimeException("Failed to create scan tasks iterable", e);
    }
  }

  private boolean isInputEmpty(List<String> planTasks, List<FileScanTask> fileScanTasks) {
    boolean isEmpty =
        (planTasks == null || planTasks.isEmpty())
            && (fileScanTasks == null || fileScanTasks.isEmpty());
    if (isEmpty) {
      LOG.debug("Both planTasks and fileScanTasks are null or empty, returning empty iterable");
    }
    return isEmpty;
  }

  private void validateDependencies() {
    if (client == null) {
      throw new IllegalStateException("RESTClient is null");
    }
    if (resourcePaths == null) {
      throw new IllegalStateException("ResourcePaths is null");
    }
    if (tableIdentifier == null) {
      throw new IllegalStateException("TableIdentifier is null");
    }
  }

  private void addFileScanTaskIterables(
      List<FileScanTask> fileScanTasks, List<ScanTasksIterable> iterables) {
    if (fileScanTasks != null && !fileScanTasks.isEmpty()) {
      LOG.debug("Creating ScanTasksIterable for {} file scan tasks", fileScanTasks.size());
      ScanTasksIterable scanTasksIterable = createScanTasksIterable(fileScanTasks);
      iterables.add(scanTasksIterable);
    }
  }

  private void addPlanTaskIterables(List<String> planTasks, List<ScanTasksIterable> iterables) {
    if (planTasks == null || planTasks.isEmpty()) {
      return;
    }

    LOG.debug("Creating ScanTasksIterables for {} plan tasks", planTasks.size());

    for (String planTask : planTasks) {
      if (planTask == null || planTask.trim().isEmpty()) {
        LOG.warn("Skipping null or empty plan task");
        continue;
      }

      try {
        ScanTasksIterable iterable = createScanTasksIterable(planTask);
        iterables.add(iterable);
      } catch (Exception e) {
        LOG.error("Failed to create ScanTasksIterable for plan task: {}", planTask, e);
        throw new RuntimeException(
            "Failed to create ScanTasksIterable for plan task: " + planTask, e);
      }
    }
  }

  private CloseableIterable<FileScanTask> combineIterables(List<ScanTasksIterable> iterables) {

    if (iterables.isEmpty()) {
      LOG.warn("No valid iterables found, returning empty iterable");
      return CloseableIterable.empty();
    }

    if (iterables.size() == 1) {
      return iterables.get(0);
    }

    return new ParallelIterable<>(iterables, planExecutor());
  }

  /**
   * Cancel the active plan if one exists. This method is idempotent and safe to call multiple
   * times.
   */
  public void cancelPlanning() {
    String planId = activePlanId.get();
    if (planId != null) {
      cancelPlanningWithId(planId);
    }
  }

  /**
   * Cancel a specific plan by ID. This is a best-effort operation - failures are logged but not
   * thrown.
   */
  private void cancelPlanningWithId(String planId) {
    if (planId == null) {
      return;
    }

    try {
      client.delete(
          resourcePaths.cancelPlanning(tableIdentifier, planId),
          null, // 204 response has no content
          headers.get(),
          ErrorHandlers.defaultErrorHandler());

      // Clear the active plan ID if it matches
      activePlanId.compareAndSet(planId, null);
    } catch (Exception e) {
      // Log but don't throw - cancel is best effort for cleanup
      // The server will eventually clean up abandoned plans
      LOG.warn("Failed to cancel plan {}: {}", planId, e.getMessage(), e);
    }
  }

  /**
   * Close this scan and cancel any active planning operations. Implements AutoCloseable for proper
   * resource management.
   */
  @Override
  public void close() {
    cancelPlanning();
  }

  private ScanTasksIterable createScanTasksIterable(List<FileScanTask> fileScanTasks) {
    return new ScanTasksIterable(
        fileScanTasks,
        client,
        resourcePaths,
        tableIdentifier,
        headers,
        planExecutor(),
        table.specs(),
        isCaseSensitive());
  }

  private ScanTasksIterable createScanTasksIterable(String planTask) {
    return new ScanTasksIterable(
        planTask,
        client,
        resourcePaths,
        tableIdentifier,
        headers,
        planExecutor(),
        table.specs(),
        isCaseSensitive());
  }
}
