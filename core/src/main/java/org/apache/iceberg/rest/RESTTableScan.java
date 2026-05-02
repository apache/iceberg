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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.TableScanContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RESTTableScan extends DataTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(RESTTableScan.class);
  private static final long MIN_SLEEP_MS = 1000; // Initial delay
  private static final long MAX_SLEEP_MS = 60 * 1000; // Max backoff delay (1 minute)
  private static final int MAX_RETRIES = 10; // Max number of poll retries
  private static final double SCALE_FACTOR = 2.0; // Exponential scale factor
  private static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.io.ResolvingFileIO";
  private static final Cache<RESTTableScan, FileIO> FILEIO_TRACKER =
      Caffeine.newBuilder()
          .weakKeys()
          .removalListener(
              (RemovalListener<RESTTableScan, FileIO>)
                  (scan, io, cause) -> {
                    if (null != io) {
                      io.close();
                    }
                  })
          .build();

  private final RESTClient client;
  private final Map<String, String> headers;
  private final TableOperations operations;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Set<Endpoint> supportedEndpoints;
  private final ParserContext parserContext;
  private final Map<String, String> catalogProperties;
  private final Object hadoopConf;
  private String planId = null;
  private FileIO scanFileIO = null;
  private boolean useSnapshotSchema = false;

  RESTTableScan(
      Table table,
      Schema schema,
      TableScanContext context,
      RESTClient client,
      Map<String, String> headers,
      TableOperations operations,
      TableIdentifier tableIdentifier,
      ResourcePaths resourcePaths,
      Set<Endpoint> supportedEndpoints,
      Map<String, String> catalogProperties,
      Object hadoopConf) {
    super(table, schema, context);
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
    this.catalogProperties = catalogProperties;
    this.hadoopConf = hadoopConf;
  }

  @Override
  protected TableScan newRefinedScan(
      Table refinedTable, Schema refinedSchema, TableScanContext refinedContext) {
    RESTTableScan scan =
        new RESTTableScan(
            refinedTable,
            refinedSchema,
            refinedContext,
            client,
            headers,
            operations,
            tableIdentifier,
            resourcePaths,
            supportedEndpoints,
            catalogProperties,
            hadoopConf);
    scan.useSnapshotSchema = useSnapshotSchema;
    return scan;
  }

  @Override
  public TableScan useRef(String name) {
    SnapshotRef ref = table().refs().get(name);
    this.useSnapshotSchema = ref != null && ref.isTag();
    return super.useRef(name);
  }

  @Override
  public TableScan useSnapshot(long snapshotId) {
    this.useSnapshotSchema = true;
    return super.useSnapshot(snapshotId);
  }

  @Override
  public Supplier<FileIO> fileIO() {
    return () -> {
      Preconditions.checkState(
          null != scanFileIO, "FileIO is not available: planFiles() must be called first");
      return scanFileIO;
    };
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    Long startSnapshotId = context().fromSnapshotId();
    Long endSnapshotId = context().toSnapshotId();
    Long snapshotId = snapshotId();
    List<Integer> projectedFieldIds = Lists.newArrayList(TypeUtil.getProjectedIds(schema()));
    List<String> selectedColumns =
        projectedFieldIds.stream().map(schema()::findColumnName).collect(Collectors.toList());

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
            .withStatsFields(statsFields)
            .withMinRowsRequested(context().minRowsRequested());

    if (startSnapshotId != null && endSnapshotId != null) {
      builder
          .withStartSnapshotId(startSnapshotId)
          .withEndSnapshotId(endSnapshotId)
          .withUseSnapshotSchema(true);
    } else if (snapshotId != null) {
      builder.withSnapshotId(snapshotId).withUseSnapshotSchema(useSnapshotSchema);
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
    this.scanFileIO =
        !response.credentials().isEmpty() ? scanFileIO(response.credentials()) : table().io();

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

  private FileIO scanFileIO(List<Credential> storageCredentials) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder().putAll(catalogProperties);
    if (null != planId) {
      builder.put(RESTCatalogProperties.REST_SCAN_PLAN_ID, planId);
    }

    Map<String, String> properties = builder.buildKeepingLast();
    FileIO ioForScan =
        CatalogUtil.loadFileIO(
            catalogProperties.getOrDefault(CatalogProperties.FILE_IO_IMPL, DEFAULT_FILE_IO_IMPL),
            properties,
            hadoopConf,
            storageCredentials.stream()
                .map(c -> StorageCredential.create(c.prefix(), c.config()))
                .collect(Collectors.toList()));
    FILEIO_TRACKER.put(this, ioForScan);
    return ioForScan;
  }

  private CloseableIterable<FileScanTask> fetchPlanningResult() {
    long maxWaitTimeMs =
        PropertyUtil.propertyAsLong(
            catalogProperties,
            RESTCatalogProperties.REST_SCAN_PLANNING_POLL_TIMEOUT_MS,
            RESTCatalogProperties.REST_SCAN_PLANNING_POLL_TIMEOUT_MS_DEFAULT);
    Preconditions.checkArgument(
        maxWaitTimeMs > 0,
        "Invalid value for %s: %s (must be positive)",
        RESTCatalogProperties.REST_SCAN_PLANNING_POLL_TIMEOUT_MS,
        maxWaitTimeMs);

    AtomicReference<FetchPlanningResultResponse> result = new AtomicReference<>();
    try {
      Tasks.foreach(planId)
          .exponentialBackoff(MIN_SLEEP_MS, MAX_SLEEP_MS, maxWaitTimeMs, SCALE_FACTOR)
          .retry(MAX_RETRIES)
          .onlyRetryOn(NotCompleteException.class)
          .onFailure(
              (id, err) -> {
                LOG.warn("Planning failed for plan ID: {}", id, err);
                cleanupPlanResources();
              })
          .throwFailureWhenFinished()
          .run(
              id -> {
                FetchPlanningResultResponse response =
                    client.get(
                        resourcePaths.plan(tableIdentifier, id),
                        headers,
                        FetchPlanningResultResponse.class,
                        headers,
                        ErrorHandlers.planErrorHandler(),
                        parserContext);

                if (response.planStatus() == PlanStatus.SUBMITTED) {
                  throw new NotCompleteException();
                } else if (response.planStatus() != PlanStatus.COMPLETED) {
                  throw new IllegalStateException(
                      String.format(
                          "Invalid planStatus: %s for planId: %s", response.planStatus(), id));
                }

                result.set(response);
              });
    } catch (NotCompleteException e) {
      throw new RemotePlanTimeoutException(
          String.format(
              Locale.ROOT,
              "Remote scan planning for planId: %s did not complete within configured limits"
                  + " (timeout=%d ms, maxRetries=%d)",
              planId,
              maxWaitTimeMs,
              MAX_RETRIES),
          e);
    }

    FetchPlanningResultResponse response = result.get();

    this.scanFileIO =
        !response.credentials().isEmpty() ? scanFileIO(response.credentials()) : table().io();

    return scanTasksIterable(response.planTasks(), response.fileScanTasks());
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

  /** Cancels the plan on the server (if supported) and closes the plan-scoped FileIO */
  private void cleanupPlanResources() {
    cancelPlan();
    FILEIO_TRACKER.invalidate(this);
    this.scanFileIO = null;
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

  private static class NotCompleteException extends RuntimeException {}
}
