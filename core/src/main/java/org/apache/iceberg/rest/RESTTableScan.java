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

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.TableScanContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RESTTableScan extends DataTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(RESTTableScan.class);
  private static final long MIN_SLEEP_MS = 1000; // Initial delay
  private static final long MAX_SLEEP_MS = 60 * 1000; // Max backoff delay (1 minute)
  private static final int MAX_ATTEMPTS = 10; // Max number of poll checks
  private static final long MAX_WAIT_TIME_MS = 5 * 60 * 1000; // Total maximum duration (5 minutes)
  private static final double SCALE_FACTOR = 2.0; // Exponential scale factor

  private final RESTClient client;
  private final Map<String, String> headers;
  private final TableOperations operations;
  private final Table table;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Set<Endpoint> supportedEndpoints;
  private final ParserContext parserContext;
  private String planId = null;

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

    PlanTableScanRequest.Builder builder =
        PlanTableScanRequest.builder()
            .withSelect(selectedColumns)
            .withFilter(filter())
            .withCaseSensitive(isCaseSensitive())
            .withStatsFields(statsFields);

    if (startSnapshotId != null && endSnapshotId != null) {
      builder
          .withStartSnapshotId(startSnapshotId)
          .withEndSnapshotId(endSnapshotId)
          .withUseSnapshotSchema(true);
    } else if (snapshotId != null) {
      boolean useSnapShotSchema = snapshotId != table.currentSnapshot().snapshotId();
      builder.withSnapshotId(snapshotId).withUseSnapshotSchema(useSnapShotSchema);
    }

    return planTableScan(builder.build());
  }

  private CloseableIterable<FileScanTask> planTableScan(PlanTableScanRequest planTableScanRequest) {
    PlanTableScanResponse response =
        client.post(
            resourcePaths.planTableScan(tableIdentifier),
            planTableScanRequest,
            PlanTableScanResponse.class,
            headers,
            ErrorHandlers.tableErrorHandler(),
            stringStringMap -> {},
            parserContext);

    this.planId = response.planId();
    PlanStatus planStatus = response.planStatus();
    switch (planStatus) {
      case COMPLETED:
        return scanTasksIterable(response.planTasks(), response.fileScanTasks());
      case SUBMITTED:
        Endpoint.check(supportedEndpoints, Endpoint.V1_FETCH_TABLE_SCAN_PLAN);
        return fetchPlanningResult();
      case FAILED:
        throw new IllegalStateException(
            String.format("Received status: %s for planId: %s", PlanStatus.FAILED, planId));
      case CANCELLED:
        throw new IllegalStateException(
            String.format("Received status: %s for planId: %s", PlanStatus.CANCELLED, planId));
      default:
        throw new IllegalStateException(
            String.format("Invalid planStatus: %s for planId: %s", planStatus, planId));
    }
  }

  private CloseableIterable<FileScanTask> fetchPlanningResult() {
    RetryPolicy<FetchPlanningResultResponse> retryPolicy =
        RetryPolicy.<FetchPlanningResultResponse>builder()
            .handleResultIf(response -> response.planStatus() == PlanStatus.SUBMITTED)
            .withBackoff(
                Duration.ofMillis(MIN_SLEEP_MS), Duration.ofMillis(MAX_SLEEP_MS), SCALE_FACTOR)
            .withJitter(0.1) // Add jitter up to 10% of the calculated delay
            .withMaxAttempts(MAX_ATTEMPTS)
            .withMaxDuration(Duration.ofMillis(MAX_WAIT_TIME_MS))
            .onFailedAttempt(
                e -> {
                  // Log when a retry occurs
                  LOG.debug(
                      "Plan {} still SUBMITTED (Attempt {}/{}). Previous attempt took {} ms.",
                      planId,
                      e.getAttemptCount(),
                      MAX_ATTEMPTS,
                      e.getElapsedAttemptTime().toMillis());
                })
            .onFailure(
                e -> {
                  LOG.warn(
                      "Polling for plan {} failed due to: {}",
                      planId,
                      e.getException().getMessage());
                  cancelPlan();
                })
            .build();

    try {
      FetchPlanningResultResponse response =
          Failsafe.with(retryPolicy)
              .get(
                  () ->
                      client.get(
                          resourcePaths.plan(tableIdentifier, planId),
                          headers,
                          FetchPlanningResultResponse.class,
                          headers,
                          ErrorHandlers.planErrorHandler(),
                          parserContext));
      Preconditions.checkState(
          response.planStatus() == PlanStatus.COMPLETED,
          "Plan finished with unexpected status %s for planId: %s",
          response.planStatus(),
          planId);

      return scanTasksIterable(response.planTasks(), response.fileScanTasks());
    } catch (FailsafeException e) {
      // FailsafeException is thrown when retries are exhausted (Max Attempts/Duration)
      // Cleanup is handled by the .onFailure() hook, so we just wrap and rethrow.
      throw new IllegalStateException(
          String.format("Polling timed out or exceeded max attempts for planId: %s.", planId), e);
    } catch (Exception e) {
      // Catch any immediate non-retryable exceptions (e.g., I/O errors, auth errors)
      try {
        cancelPlan();
      } catch (Exception cancelException) {
        // Ignore cancellation failures during exception handling
        e.addSuppressed(cancelException);
      }
      throw e;
    }
  }

  private CloseableIterable<FileScanTask> scanTasksIterable(
      List<String> planTasks, List<FileScanTask> fileScanTasks) {
    if (planTasks != null && !planTasks.isEmpty()) {
      Endpoint.check(supportedEndpoints, Endpoint.V1_FETCH_TABLE_SCAN_PLAN_TASKS);
    }

    return CloseableIterable.whenComplete(
        new ScanTaskIterable(
            planTasks,
            fileScanTasks == null ? List.of() : fileScanTasks,
            client,
            resourcePaths,
            tableIdentifier,
            headers,
            planExecutor(),
            parserContext),
        this::cancelPlan);
  }

  @VisibleForTesting
  @SuppressWarnings("checkstyle:RegexpMultiline")
  public boolean cancelPlan() {
    if (planId == null || !supportedEndpoints.contains(Endpoint.V1_CANCEL_TABLE_SCAN_PLAN)) {
      return false;
    }

    try {
      client.delete(
          resourcePaths.plan(tableIdentifier, planId),
          Map.of(),
          null,
          headers,
          ErrorHandlers.planErrorHandler());
      this.planId = null;
      return true;
    } catch (Exception e) {
      // Plan might have already completed or failed, which is acceptable
      return false;
    }
  }
}
