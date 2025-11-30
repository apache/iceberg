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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;

public class RESTTableScan extends DataTableScan {
  private final RESTClient client;
  private final Map<String, String> headers;
  private final TableOperations operations;
  private final Table table;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Set<Endpoint> supportedEndpoints;
  private final ParserContext parserContext;

  private String currentPlanId = null;

  // TODO revisit if this property should be configurable
  // Sleep duration between polling attempts for plan completion
  private static final int FETCH_PLANNING_SLEEP_DURATION_MS = 1000;
  // Maximum time to wait for scan planning to complete (5 minutes)
  // This prevents indefinite blocking on server-side planning issues
  private static final long MAX_WAIT_TIME_MS = 5 * 60 * 1000L;

  RESTTableScan(
      Table table,
      Schema schema,
      TableScanContext context,
      RESTClient client,
      Map<String, String> headers,
      TableOperations operations,
      TableIdentifier tableIdentifier,
      ResourcePaths resourcePaths,
      Set<Endpoint> supportedEndpoints) {
    super(table, schema, context);
    this.table = table;
    this.client = client;
    this.headers = headers;
    this.operations = operations;
    this.tableIdentifier = tableIdentifier;
    this.resourcePaths = resourcePaths;
    this.supportedEndpoints = supportedEndpoints;
    this.parserContext =
        ParserContext.builder()
            .add("specsById", table.specs())
            .add("caseSensitive", context().caseSensitive())
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
        headers,
        operations,
        tableIdentifier,
        resourcePaths,
        supportedEndpoints);
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
        PlanTableScanRequest.builder()
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
    }

    return planTableScan(planTableScanRequestBuilder.build());
  }

  private CloseableIterable<FileScanTask> planTableScan(PlanTableScanRequest planTableScanRequest) {
    PlanTableScanResponse response =
        client.post(
            resourcePaths.planTableScan(tableIdentifier),
            planTableScanRequest,
            PlanTableScanResponse.class,
            headers,
            ErrorHandlers.defaultErrorHandler(),
            stringStringMap -> {},
            parserContext);

    PlanStatus planStatus = response.planStatus();
    switch (planStatus) {
      case COMPLETED:
        currentPlanId = response.planId();
        return scanTasksIterable(response.planTasks(), response.fileScanTasks());
      case SUBMITTED:
        Endpoint.check(supportedEndpoints, Endpoint.V1_FETCH_TABLE_SCAN_PLAN);
        return fetchPlanningResult(response.planId());

      default:
        handleNonSuccessfulTerminalState(planStatus, response.planId());
        throw new IllegalStateException("Handler should have thrown an exception.");
    }
  }

  private CloseableIterable<FileScanTask> fetchPlanningResult(String planId) {
    // Set the current plan ID for potential cancellation
    currentPlanId = planId;
    try {
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime <= MAX_WAIT_TIME_MS) {
        FetchPlanningResultResponse response =
            client.get(
                resourcePaths.plan(tableIdentifier, planId),
                Map.of(),
                FetchPlanningResultResponse.class,
                headers,
                ErrorHandlers.defaultErrorHandler(),
                parserContext);

        PlanStatus planStatus = response.planStatus();
        switch (planStatus) {
          case COMPLETED:
            return scanTasksIterable(response.planTasks(), response.fileScanTasks());
          case SUBMITTED:
            try {
              // TODO: if we want to add some jitter here to avoid thundering herd.
              Thread.sleep(FETCH_PLANNING_SLEEP_DURATION_MS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              // Attempt to cancel the plan before exiting
              cancelPlan();
              throw new RuntimeException("Interrupted while fetching plan status", e);
            }
            break;
        }
      }
      // If we reach here, we've exceeded the max wait time
      // Attempt to cancel the plan before timing out
      cancelPlan();
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Exceeded max wait time of %d ms when fetching planning result for planId: %s",
              MAX_WAIT_TIME_MS,
              planId));
    } catch (Exception e) {
      // Clear the plan ID on any exception (except successful completion)
      // Also attempt to cancel the plan to clean up server resources
      try {
        cancelPlan();
      } catch (Exception cancelException) {
        // Ignore cancellation failures during exception handling
        // The original exception is more important
      }
      throw e;
    }
  }

  private CloseableIterable<FileScanTask> scanTasksIterable(
      List<String> planTasks, List<FileScanTask> fileScanTasks) {
    if (planTasks != null && !planTasks.isEmpty()) {
      Endpoint.check(supportedEndpoints, Endpoint.V1_FETCH_TABLE_SCAN_PLAN_TASKS);
    }

    return new ScanTasksIterable(
        planTasks,
        fileScanTasks,
        client,
        resourcePaths,
        tableIdentifier,
        headers,
        planExecutor(),
        table.specs(),
        isCaseSensitive(),
        this::cancelPlan);
  }

  @VisibleForTesting
  @SuppressWarnings("checkstyle:RegexpMultiline")
  public boolean cancelPlan() {
    String planId = currentPlanId;
    if (planId == null || !supportedEndpoints.contains(Endpoint.V1_CANCEL_TABLE_SCAN_PLAN)) {
      return false;
    }

    try {
      client.delete(
          resourcePaths.plan(tableIdentifier, planId),
          Map.of(),
          null,
          headers,
          ErrorHandlers.defaultErrorHandler());
      currentPlanId = null;
      return true;
    } catch (Exception e) {
      // Plan might have already completed or failed, which is acceptable
      return false;
    }
  }

  private void handleNonSuccessfulTerminalState(PlanStatus status, String planId) {
    switch (status) {
      case FAILED:
        throw new IllegalStateException(
            String.format("Received status: %s for planId: %s", PlanStatus.FAILED, planId));
      case CANCELLED:
        throw new IllegalStateException(
            String.format("Received status: %s for planId: %s", PlanStatus.CANCELLED, planId));
      default:
        throw new IllegalStateException(
            String.format("Invalid planStatus during fetchPlanningResult: %s", status));
    }
  }
}
