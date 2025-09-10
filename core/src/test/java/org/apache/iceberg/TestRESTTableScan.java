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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRESTTableScan {

  private RESTClient mockClient;
  private Table mockTable;
  private Schema testSchema;
  private TableScanContext mockContext;
  private Supplier<Map<String, String>> mockHeaders;
  private TableOperations mockOperations;
  private TableIdentifier tableId;
  private ResourcePaths mockResourcePaths;
  private Snapshot mockSnapshot;
  private RESTTableScan tableScan;

  @BeforeEach
  public void setUp() {
    mockClient = mock(RESTClient.class);
    mockTable = mock(Table.class);
    mockContext = mock(TableScanContext.class);
    mockHeaders = mock(Supplier.class);
    mockOperations = mock(TableOperations.class);
    mockResourcePaths = mock(ResourcePaths.class);
    mockSnapshot = mock(Snapshot.class);

    testSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    tableId = TableIdentifier.of("test_namespace", "test_table");

    when(mockHeaders.get()).thenReturn(ImmutableMap.of("Authorization", "Bearer token"));
    when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);
    when(mockSnapshot.snapshotId()).thenReturn(1L);
    when(mockContext.caseSensitive()).thenReturn(true);
    when(mockContext.fromSnapshotId()).thenReturn(null);
    when(mockContext.toSnapshotId()).thenReturn(null);
    when(mockContext.rowFilter()).thenReturn(Expressions.alwaysTrue());
    when(mockContext.ignoreResiduals()).thenReturn(false);
    when(mockContext.selectedColumns()).thenReturn(null);
    when(mockTable.specs()).thenReturn(ImmutableMap.of(0, PartitionSpec.unpartitioned()));

    tableScan =
        new RESTTableScan(
            mockTable,
            testSchema,
            mockContext,
            mockClient,
            "/v1/namespaces/test_namespace/tables/test_table",
            mockHeaders,
            mockOperations,
            tableId,
            mockResourcePaths);
  }

  @Test
  public void testPlanFilesWithCompletedStatus() throws Exception {
    FileScanTask mockTask = mock(FileScanTask.class);
    List<FileScanTask> fileScanTasks = ImmutableList.of(mockTask);

    PlanTableScanResponse mockResponse = mock(PlanTableScanResponse.class);
    when(mockResponse.planStatus()).thenReturn(PlanStatus.COMPLETED);
    when(mockResponse.fileScanTasks()).thenReturn(fileScanTasks);
    when(mockResponse.planTasks()).thenReturn(null);

    when(mockResourcePaths.planTableScan(tableId))
        .thenReturn("/v1/namespaces/test_namespace/tables/test_table/scan");
    when(mockClient.post(
            eq("/v1/namespaces/test_namespace/tables/test_table/scan"),
            any(PlanTableScanRequest.class),
            eq(PlanTableScanResponse.class),
            anyMap(),
            any(),
            any(),
            any(ParserContext.class)))
        .thenReturn(mockResponse);

    try (CloseableIterable<FileScanTask> result = tableScan.planFiles()) {
      assertThat(result).isNotNull();
      List<FileScanTask> tasks = ImmutableList.copyOf(result);
      assertThat(tasks).hasSize(1);
      assertThat(tasks.get(0)).isEqualTo(mockTask);
    }
  }

  @Test
  public void testPlanFilesWithSubmittedStatus() throws Exception {
    String planId = "test-plan-id";

    PlanTableScanResponse initialResponse = mock(PlanTableScanResponse.class);
    when(initialResponse.planStatus()).thenReturn(PlanStatus.SUBMITTED);
    when(initialResponse.planId()).thenReturn(planId);

    FetchPlanningResultResponse completedResponse = mock(FetchPlanningResultResponse.class);
    when(completedResponse.planStatus()).thenReturn(PlanStatus.COMPLETED);

    FileScanTask mockTask = mock(FileScanTask.class);
    when(completedResponse.fileScanTasks()).thenReturn(ImmutableList.of(mockTask));
    when(completedResponse.planTasks()).thenReturn(null);

    when(mockResourcePaths.planTableScan(tableId))
        .thenReturn("/v1/namespaces/test_namespace/tables/test_table/scan");
    when(mockResourcePaths.fetchPlanningResult(tableId, planId))
        .thenReturn("/v1/namespaces/test_namespace/tables/test_table/scan/" + planId);

    when(mockClient.post(
            eq("/v1/namespaces/test_namespace/tables/test_table/scan"),
            any(PlanTableScanRequest.class),
            eq(PlanTableScanResponse.class),
            anyMap(),
            any(),
            any(),
            any(ParserContext.class)))
        .thenReturn(initialResponse);

    when(mockClient.get(
            eq("/v1/namespaces/test_namespace/tables/test_table/scan/" + planId),
            eq(Map.of()),
            eq(FetchPlanningResultResponse.class),
            anyMap(),
            any(),
            any(ParserContext.class)))
        .thenReturn(completedResponse);

    try (CloseableIterable<FileScanTask> result = tableScan.planFiles()) {
      assertThat(result).isNotNull();
      List<FileScanTask> tasks = ImmutableList.copyOf(result);
      assertThat(tasks).hasSize(1);
      assertThat(tasks.get(0)).isEqualTo(mockTask);
    }
  }

  @Test
  public void testPlanFilesWithFailedStatus() {
    PlanTableScanResponse mockResponse = mock(PlanTableScanResponse.class);
    when(mockResponse.planStatus()).thenReturn(PlanStatus.FAILED);

    when(mockResourcePaths.planTableScan(tableId))
        .thenReturn("/v1/namespaces/test_namespace/tables/test_table/scan");
    when(mockClient.post(
            eq("/v1/namespaces/test_namespace/tables/test_table/scan"),
            any(PlanTableScanRequest.class),
            eq(PlanTableScanResponse.class),
            anyMap(),
            any(),
            any(),
            any(ParserContext.class)))
        .thenReturn(mockResponse);

    assertThatThrownBy(() -> tableScan.planFiles())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Received \"failed\" status from service when planning a table scan");
  }

  @Test
  public void testPlanFilesWithCancelledStatus() {
    PlanTableScanResponse mockResponse = mock(PlanTableScanResponse.class);
    when(mockResponse.planStatus()).thenReturn(PlanStatus.CANCELLED);

    when(mockResourcePaths.planTableScan(tableId))
        .thenReturn("/v1/namespaces/test_namespace/tables/test_table/scan");
    when(mockClient.post(
            eq("/v1/namespaces/test_namespace/tables/test_table/scan"),
            any(PlanTableScanRequest.class),
            eq(PlanTableScanResponse.class),
            anyMap(),
            any(),
            any(),
            any(ParserContext.class)))
        .thenReturn(mockResponse);

    assertThatThrownBy(() -> tableScan.planFiles())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Received \"cancelled\" status from service when planning a table scan");
  }

  @Test
  public void testNewRefinedScan() {
    Schema refinedSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    TableScan refinedScan = tableScan.newRefinedScan(mockTable, refinedSchema, mockContext);

    assertThat(refinedScan).isInstanceOf(RESTTableScan.class);
    assertThat(refinedScan.schema()).isEqualTo(refinedSchema);
  }

  @Test
  public void testGetScanTasksIterableWithEmptyInput() {
    CloseableIterable<FileScanTask> result = tableScan.getScanTasksIterable(null, null);
    assertThat(result).isNotNull();
    assertThat(ImmutableList.copyOf(result)).isEmpty();
  }

  @Test
  public void testGetScanTasksIterableWithFileScanTasks() {
    FileScanTask mockTask = mock(FileScanTask.class);
    List<FileScanTask> fileScanTasks = ImmutableList.of(mockTask);

    CloseableIterable<FileScanTask> result = tableScan.getScanTasksIterable(null, fileScanTasks);
    assertThat(result).isNotNull();
  }

  @Test
  public void testGetScanTasksIterableWithNullDependencies() {
    FileScanTask mockTask = mock(FileScanTask.class);
    List<FileScanTask> fileScanTasks = ImmutableList.of(mockTask);

    RESTTableScan scanWithNullClient =
        new RESTTableScan(
            mockTable,
            testSchema,
            mockContext,
            null, // null client
            "/path",
            mockHeaders,
            mockOperations,
            tableId,
            mockResourcePaths);

    assertThatThrownBy(() -> scanWithNullClient.getScanTasksIterable(null, fileScanTasks))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("RESTClient is null");
  }

  @Test
  public void testCancelPlanning() {
    when(mockResourcePaths.cancelPlanning(tableId, "test-plan-id"))
        .thenReturn("/v1/namespaces/test_namespace/tables/test_table/scan/test-plan-id/cancel");

    tableScan.cancelPlanning();

    // Should not throw any exception for no active plan
    assertThat(tableScan).isNotNull();
  }

  @Test
  public void testClose() {
    tableScan.close();
    assertThat(tableScan).isNotNull();
  }

  @Test
  public void testPlanRequestBuilding() throws Exception {
    when(mockContext.fromSnapshotId()).thenReturn(1L);
    when(mockContext.toSnapshotId()).thenReturn(2L);

    PlanTableScanResponse mockResponse = mock(PlanTableScanResponse.class);
    when(mockResponse.planStatus()).thenReturn(PlanStatus.COMPLETED);
    when(mockResponse.fileScanTasks()).thenReturn(ImmutableList.of());
    when(mockResponse.planTasks()).thenReturn(null);

    when(mockResourcePaths.planTableScan(tableId))
        .thenReturn("/v1/namespaces/test_namespace/tables/test_table/scan");
    when(mockClient.post(
            eq("/v1/namespaces/test_namespace/tables/test_table/scan"),
            any(PlanTableScanRequest.class),
            eq(PlanTableScanResponse.class),
            anyMap(),
            any(),
            any(),
            any(ParserContext.class)))
        .thenReturn(mockResponse);

    try (CloseableIterable<FileScanTask> result = tableScan.planFiles()) {
      assertThat(result).isNotNull();
    }

    verify(mockClient)
        .post(
            eq("/v1/namespaces/test_namespace/tables/test_table/scan"),
            any(PlanTableScanRequest.class),
            eq(PlanTableScanResponse.class),
            anyMap(),
            any(),
            any(),
            any(ParserContext.class));
  }

  @Test
  public void testFetchPlanningResultWithException() {
    String planId = "test-plan-id";

    PlanTableScanResponse initialResponse = mock(PlanTableScanResponse.class);
    when(initialResponse.planStatus()).thenReturn(PlanStatus.SUBMITTED);
    when(initialResponse.planId()).thenReturn(planId);

    when(mockResourcePaths.planTableScan(tableId))
        .thenReturn("/v1/namespaces/test_namespace/tables/test_table/scan");
    when(mockResourcePaths.fetchPlanningResult(tableId, planId))
        .thenReturn("/v1/namespaces/test_namespace/tables/test_table/scan/" + planId);
    when(mockResourcePaths.cancelPlanning(tableId, planId))
        .thenReturn("/v1/namespaces/test_namespace/tables/test_table/scan/" + planId + "/cancel");

    when(mockClient.post(
            eq("/v1/namespaces/test_namespace/tables/test_table/scan"),
            any(PlanTableScanRequest.class),
            eq(PlanTableScanResponse.class),
            anyMap(),
            any(),
            any(),
            any(ParserContext.class)))
        .thenReturn(initialResponse);

    when(mockClient.get(
            eq("/v1/namespaces/test_namespace/tables/test_table/scan/" + planId),
            eq(Map.of()),
            eq(FetchPlanningResultResponse.class),
            anyMap(),
            any(),
            any(ParserContext.class)))
        .thenThrow(new RuntimeException("Network error"));

    assertThatThrownBy(() -> tableScan.planFiles())
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Network error");

    verify(mockClient)
        .delete(
            eq("/v1/namespaces/test_namespace/tables/test_table/scan/" + planId + "/cancel"),
            eq(null),
            anyMap(),
            any());
  }
}
