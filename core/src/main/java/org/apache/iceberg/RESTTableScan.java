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
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;

public class RESTTableScan extends DataTableScan {
  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> headers;
  private final TableOperations operations;
  private final Table table;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;

  // TODO revisit if this property should be configurable
  private static final int FETCH_PLANNING_SLEEP_DURATION_MS = 1000;

  public RESTTableScan(
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
    List<String> selectedColumns =
        schema().columns().stream().map(Types.NestedField::name).collect(Collectors.toList());

    List<String> statsFields = null;
    if (columnsToKeepStats() != null) {
      statsFields =
          columnsToKeepStats().stream()
              .map(columnId -> schema().findColumnName(columnId))
              .collect(Collectors.toList());
    }

    Long startSnapshotId = context().fromSnapshotId();
    Long endSnapshotId = context().toSnapshotId();
    Long snapshotId = snapshotId();

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
            headers,
            ErrorHandlers.defaultErrorHandler());

    PlanStatus planStatus = response.planStatus();
    switch (planStatus) {
      case COMPLETED:
        // List<FileScanTask> fileScanTasks = bindFileScanTasksWithSpec(response.fileScanTasks());
        return getScanTasksIterable(response.planTasks(), response.fileScanTasks());
      case SUBMITTED:
        return fetchPlanningResult(response.planId());
      case FAILED:
        throw new RuntimeException(
            "Received \"failed\" status from service when planning a table scan");
      default:
        throw new RuntimeException(
            String.format("Invalid planStatus during planTableScan: %s", planStatus));
    }
  }

  private CloseableIterable<FileScanTask> fetchPlanningResult(String planId) {

    // TODO need to introduce a max wait time for this loop potentially
    boolean planningFinished = false;
    while (!planningFinished) {
      FetchPlanningResultResponse response =
          client.get(
              resourcePaths.fetchPlanningResult(tableIdentifier, planId),
              FetchPlanningResultResponse.class,
              headers,
              ErrorHandlers.defaultErrorHandler());

      PlanStatus planStatus = response.planStatus();
      switch (planStatus) {
        case COMPLETED:
          // List<FileScanTask> fileScanTasks = bindFileScanTasksWithSpec(response.fileScanTasks());
          return getScanTasksIterable(response.planTasks(), response.fileScanTasks());
        case SUBMITTED:
          try {
            Thread.sleep(FETCH_PLANNING_SLEEP_DURATION_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while fetching plan status", e);
          }
          break;
        case FAILED:
          throw new RuntimeException(
              "Received \"failed\" status from service when fetching a table scan");
        case CANCELLED:
          throw new RuntimeException(
              String.format(
                  "Received \"cancelled\" status from service when fetching a table scan, planId: %s is invalid",
                  planId));
        default:
          throw new RuntimeException(
              String.format("Invalid planStatus during fetchPlanningResult: %s", planStatus));
      }
    }
    return null;
  }

  public List<FileScanTask> bindFileScanTasksWithSpec(List<FileScanTask> unboundFileScanTasks) {
    // TODO call this function
    if (unboundFileScanTasks == null) {
      return null;
    }
    List<FileScanTask> boundFileScanTasks = Lists.newArrayList();
    for (FileScanTask task : unboundFileScanTasks) {
      UnboundBaseFileScanTask unboundBaseFileScanTask = (UnboundBaseFileScanTask) task;
      FileScanTask boundTask = unboundBaseFileScanTask.bind(table.spec(), isCaseSensitive());
      boundFileScanTasks.add(boundTask);
    }
    return boundFileScanTasks;
  }

  public CloseableIterable<FileScanTask> getScanTasksIterable(
      List<String> planTasks, List<FileScanTask> fileScanTasks) {
    List<ScanTasksIterable> iterableOfScanTaskIterables = Lists.newArrayList();
    if (fileScanTasks != null) {
      // add this to the list for below if planTasks will also be present
      ScanTasksIterable scanTasksIterable =
          new ScanTasksIterable(
              fileScanTasks,
              client,
              resourcePaths,
              tableIdentifier,
              headers,
              planExecutor(),
              table.specs(),
              isCaseSensitive());
      iterableOfScanTaskIterables.add(scanTasksIterable);
    }
    if (planTasks != null) {
      // Use parallel iterable since planTasks are present
      for (String planTask : planTasks) {
        ScanTasksIterable iterable =
            new ScanTasksIterable(
                planTask,
                client,
                resourcePaths,
                tableIdentifier,
                headers,
                planExecutor(),
                table.specs(),
                isCaseSensitive());
        iterableOfScanTaskIterables.add(iterable);
      }
      return new ParallelIterable<>(iterableOfScanTaskIterables, planExecutor());
      // another idea is to keep concating to the original parallel iterable???
    }
    // use a single scanTasks iterable since no need to parallelize since no planTasks
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
}
