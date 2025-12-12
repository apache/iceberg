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

import static org.apache.iceberg.TestBase.FILE_A;
import static org.apache.iceberg.TestBase.FILE_A_DELETES;
import static org.apache.iceberg.TestBase.FILE_A_EQUALITY_DELETES;
import static org.apache.iceberg.TestBase.FILE_B;
import static org.apache.iceberg.TestBase.FILE_B_DELETES;
import static org.apache.iceberg.TestBase.FILE_B_EQUALITY_DELETES;
import static org.apache.iceberg.TestBase.FILE_C;
import static org.apache.iceberg.TestBase.FILE_C_EQUALITY_DELETES;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

public class TestRESTScanPlanning {
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();
  private static final Namespace NS = Namespace.of("ns");

  private InMemoryCatalog backendCatalog;
  private Server httpServer;
  private RESTCatalogAdapter adapterForRESTServer;
  private ParserContext parserContext;
  @TempDir private Path temp;
  private RESTCatalog restCatalogWithScanPlanning;

  @BeforeEach
  public void setupCatalogs() throws Exception {
    File warehouse = temp.toFile();
    this.backendCatalog = new InMemoryCatalog();
    this.backendCatalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath()));

    adapterForRESTServer =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                if (ResourcePaths.config().equals(request.path())) {
                  return castResponse(
                      responseType,
                      ConfigResponse.builder()
                          .withEndpoints(
                              Arrays.stream(Route.values())
                                  .map(r -> Endpoint.create(r.method().name(), r.resourcePath()))
                                  .collect(Collectors.toList()))
                          .withOverrides(
                              ImmutableMap.of(
                                  RESTCatalogProperties.REST_SCAN_PLANNING_ENABLED, "true"))
                          .build());
                }
                Object body = roundTripSerialize(request.body(), "request");
                HTTPRequest req = ImmutableHTTPRequest.builder().from(request).body(body).build();
                T response = super.execute(req, responseType, errorHandler, responseHeaders);
                return roundTripSerialize(response, "response");
              }
            });

    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.addServlet(
        new ServletHolder(new RESTCatalogServlet(adapterForRESTServer)), "/*");
    servletContext.setHandler(new GzipHandler());

    this.httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    httpServer.setHandler(servletContext);
    httpServer.start();

    // Initialize catalog with scan planning enabled
    this.restCatalogWithScanPlanning = initCatalog("prod-with-scan-planning", ImmutableMap.of());
  }

  @AfterEach
  public void teardownCatalogs() throws Exception {
    if (restCatalogWithScanPlanning != null) {
      restCatalogWithScanPlanning.close();
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }

    if (httpServer != null) {
      httpServer.stop();
      httpServer.join();
    }
  }

  // ==================== Helper Methods ====================

  private RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    RESTCatalog catalog =
        new RESTCatalog(
            SessionCatalog.SessionContext.createEmpty(),
            (config) ->
                HTTPClient.builder(config)
                    .uri(config.get(CatalogProperties.URI))
                    .withHeaders(RESTUtil.configHeaders(config))
                    .build());
    catalog.setConf(new Configuration());
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO");
    catalog.initialize(
        catalogName,
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .putAll(additionalProperties)
            .build());
    return catalog;
  }

  @SuppressWarnings("unchecked")
  private <T> T roundTripSerialize(T payload, String description) {
    if (payload == null) {
      return null;
    }

    try {
      if (payload instanceof RESTMessage) {
        RESTMessage message = (RESTMessage) payload;
        ObjectReader reader = MAPPER.readerFor(message.getClass());
        if (parserContext != null && !parserContext.isEmpty()) {
          reader = reader.with(parserContext.toInjectableValues());
        }
        return reader.readValue(MAPPER.writeValueAsString(message));
      } else {
        // use Map so that Jackson doesn't try to instantiate ImmutableMap from payload.getClass()
        return (T) MAPPER.readValue(MAPPER.writeValueAsString(payload), Map.class);
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format("Failed to serialize and deserialize %s: %s", description, payload), e);
    }
  }

  private void setParserContext(Table table) {
    parserContext =
        ParserContext.builder().add("specsById", table.specs()).add("caseSensitive", false).build();
  }

  private RESTCatalog scanPlanningCatalog() {
    return restCatalogWithScanPlanning;
  }

  private void configurePlanningBehavior(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> configurator) {
    TestPlanningBehavior.Builder builder = TestPlanningBehavior.builder();
    adapterForRESTServer.setPlanningBehavior(configurator.apply(builder).build());
  }

  private Table createTableWithScanPlanning(RESTCatalog catalog, String tableName) {
    return createTableWithScanPlanning(catalog, TableIdentifier.of(NS, tableName));
  }

  private Table createTableWithScanPlanning(RESTCatalog catalog, TableIdentifier identifier) {
    catalog.createNamespace(identifier.namespace());
    return catalog.buildTable(identifier, SCHEMA).withPartitionSpec(SPEC).create();
  }

  private RESTTable restTableFor(RESTCatalog catalog, String tableName) {
    Table table = createTableWithScanPlanning(catalog, tableName);
    table.newAppend().appendFile(FILE_A).commit();
    assertThat(table).isInstanceOf(RESTTable.class);
    return (RESTTable) table;
  }

  private RESTTableScan restTableScanFor(Table table) {
    assertThat(table).isInstanceOf(RESTTable.class);
    RESTTable restTable = (RESTTable) table;
    TableScan scan = restTable.newScan();
    assertThat(scan).isInstanceOf(RESTTableScan.class);
    return (RESTTableScan) scan;
  }

  // ==================== Test Planning Behavior ====================

  /** Enum for parameterized tests to test both synchronous and asynchronous planning modes. */
  private enum PlanningMode
      implements Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> {
    SYNCHRONOUS(TestPlanningBehavior.Builder::synchronous),
    ASYNCHRONOUS(TestPlanningBehavior.Builder::asynchronous);

    private final Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> configurer;

    PlanningMode(Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> configurer) {
      this.configurer = configurer;
    }

    @Override
    public TestPlanningBehavior.Builder apply(TestPlanningBehavior.Builder builder) {
      return this.configurer.apply(builder);
    }
  }

  private static class TestPlanningBehavior implements RESTCatalogAdapter.PlanningBehavior {
    private final boolean asyncPlanning;
    private final int tasksPerPage;

    private TestPlanningBehavior(boolean asyncPlanning, int tasksPerPage) {
      this.asyncPlanning = asyncPlanning;
      this.tasksPerPage = tasksPerPage;
    }

    private static Builder builder() {
      return new Builder();
    }

    @Override
    public boolean shouldPlanTableScanAsync(Scan<?, FileScanTask, ?> scan) {
      return asyncPlanning;
    }

    @Override
    public int numberFileScanTasksPerPlanTask() {
      return tasksPerPage;
    }

    protected static class Builder {
      private boolean asyncPlanning;
      private int tasksPerPage;

      Builder asyncPlanning(boolean async) {
        asyncPlanning = async;
        return this;
      }

      Builder tasksPerPage(int tasks) {
        tasksPerPage = tasks;
        return this;
      }

      // Convenience methods for common test scenarios
      Builder synchronous() {
        return asyncPlanning(false).tasksPerPage(100);
      }

      Builder synchronousWithPagination() {
        return asyncPlanning(false).tasksPerPage(1);
      }

      Builder asynchronous() {
        return asyncPlanning(true).tasksPerPage(100);
      }

      TestPlanningBehavior build() {
        return new TestPlanningBehavior(asyncPlanning, tasksPerPage);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void scanPlanningWithAllTasksInSingleResponse(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode)
      throws IOException {
    configurePlanningBehavior(planMode);
    Table table = restTableFor(scanPlanningCatalog(), "all_tasks_table");
    setParserContext(table);

    // Verify actual data file is returned with correct count
    try (CloseableIterable<FileScanTask> iterable = table.newScan().planFiles()) {
      List<FileScanTask> tasks = Lists.newArrayList(iterable);

      assertThat(tasks).hasSize(1);
      assertThat(tasks.get(0).file().location()).isEqualTo(FILE_A.location());
      assertThat(tasks.get(0).deletes()).isEmpty();
    }
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void scanPlanningWithBatchScan(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode)
      throws IOException {
    configurePlanningBehavior(planMode);
    Table table = restTableFor(scanPlanningCatalog(), "batch_scan_table");
    setParserContext(table);

    // Verify actual data file is returned with correct count
    try (CloseableIterable<ScanTask> iterable = table.newBatchScan().planFiles()) {
      List<ScanTask> tasks = Lists.newArrayList(iterable);

      assertThat(tasks).hasSize(1);
      assertThat(tasks.get(0).asFileScanTask().file().location()).isEqualTo(FILE_A.location());
      assertThat(tasks.get(0).asFileScanTask().deletes()).isEmpty();
    }
  }

  @Test
  public void nestedPlanTaskPagination() throws IOException {
    // Configure: synchronous planning with very small pages (creates nested plan task structure)
    configurePlanningBehavior(TestPlanningBehavior.Builder::synchronousWithPagination);

    Table table = restTableFor(scanPlanningCatalog(), "nested_plan_task_table");
    // add one more files for proper pagination
    table.newFastAppend().appendFile(FILE_B).commit();
    setParserContext(table);

    // Verify actual data file is returned via nested plan task fetching with correct count
    try (CloseableIterable<FileScanTask> iterable = table.newScan().planFiles()) {
      List<FileScanTask> tasks = Lists.newArrayList(iterable);
      assertThat(tasks).hasSize(2);
      assertThat(tasks)
          .anySatisfy(task -> assertThat(task.file().location()).isEqualTo(FILE_A.location()));
      assertThat(tasks)
          .anySatisfy(task -> assertThat(task.file().location()).isEqualTo(FILE_B.location()));
      assertThat(tasks.get(0).deletes()).isEmpty();
      assertThat(tasks.get(1).deletes()).isEmpty();
    }
  }

  @Test
  public void cancelPlanMethodAvailability() {
    configurePlanningBehavior(TestPlanningBehavior.Builder::synchronousWithPagination);
    RESTTable table = restTableFor(scanPlanningCatalog(), "cancel_method_table");
    RESTTableScan restTableScan = restTableScanFor(table);

    // Test that cancelPlan method is available and callable
    // When no plan is active, it should return false
    assertThat(restTableScan.cancelPlan()).isFalse();

    // Verify the method doesn't throw exceptions when called multiple times
    assertThat(restTableScan.cancelPlan()).isFalse();
  }

  @Test
  public void iterableCloseTriggersCancel() throws IOException {
    configurePlanningBehavior(TestPlanningBehavior.Builder::asynchronous);
    RESTTable restTable = restTableFor(scanPlanningCatalog(), "iterable_close_test");
    setParserContext(restTable);

    TableScan scan = restTable.newScan();
    assertThat(scan).isInstanceOf(RESTTableScan.class);
    RESTTableScan restTableScan = (RESTTableScan) scan;

    // Get the iterable
    CloseableIterable<FileScanTask> iterable = restTableScan.planFiles();

    // call cancelPlan before closing the iterable
    boolean cancelled = restTableScan.cancelPlan();
    assertThat(cancelled).isTrue();

    // Verify we can close the iterable without exceptions
    // This tests that cancellation callbacks are properly wired through
    iterable.close();
  }

  @ParameterizedTest
  @EnumSource(MetadataTableType.class)
  public void metadataTablesWithRemotePlanning(MetadataTableType type) {
    configurePlanningBehavior(TestPlanningBehavior.Builder::synchronous);
    RESTTable table = restTableFor(scanPlanningCatalog(), "metadata_tables_test");
    table.newAppend().appendFile(FILE_B).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_B_EQUALITY_DELETES).commit();
    setParserContext(table);
    // RESTTable should be only be returned for non-metadata tables, because client would
    // not have access to metadata files for example manifests, since all it needs is file scan
    // tasks, this test just verifies that metadata tables can be scanned with RESTTable.
    Table metadataTableInstance = MetadataTableUtils.createMetadataTableInstance(table, type);
    assertThat(metadataTableInstance).isNotNull();
    if (type.equals(MetadataTableType.POSITION_DELETES)) {
      // Position deletes table only uses batch scan
      assertThat(metadataTableInstance.newBatchScan().planFiles()).isNotEmpty();
    } else {
      assertThat(metadataTableInstance.newScan().planFiles()).isNotEmpty();
    }
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void remoteScanPlanningWithEmptyTable(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode) {
    configurePlanningBehavior(planMode);
    Table table = createTableWithScanPlanning(scanPlanningCatalog(), "empty_table_test");
    setParserContext(table);
    assertThat(table.newScan().planFiles()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  @Disabled("Pruning files based on columns is not yet supported in REST scan planning")
  void remoteScanPlanningWithNonExistentColumn(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode) {
    configurePlanningBehavior(planMode);
    Table table = restTableFor(scanPlanningCatalog(), "non-existent_column");
    setParserContext(table);
    assertThat(table.newScan().select("non-existent-column").planFiles()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void incrementalScan(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode) {
    configurePlanningBehavior(planMode);
    Table table = restTableFor(scanPlanningCatalog(), "incremental_scan");
    setParserContext(table);

    // Add second file to the table
    table.newAppend().appendFile(FILE_B).commit();
    long startSnapshotId = table.currentSnapshot().snapshotId();
    // Add third file to the table
    table.newAppend().appendFile(FILE_C).commit();
    long endSnapshotId = table.currentSnapshot().snapshotId();
    assertThat(
            table
                .newIncrementalAppendScan()
                .fromSnapshotInclusive(startSnapshotId)
                .toSnapshot(endSnapshotId)
                .planFiles())
        .hasSize(2)
        .extracting(task -> task.file().location())
        .contains(FILE_C.location(), FILE_B.location());
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void remoteScanPlanningWithPositionDeletes(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode)
      throws IOException {
    configurePlanningBehavior(planMode);
    Table table = restTableFor(scanPlanningCatalog(), "position_deletes_test");
    setParserContext(table);

    // Add position deletes that correspond to FILE_A (which was added in table creation)
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();

    // Ensure we have a RESTTable with server-side planning enabled
    assertThat(table).isInstanceOf(RESTTable.class);

    // Execute scan planning - should handle position deletes correctly
    try (CloseableIterable<FileScanTask> iterable = table.newScan().planFiles()) {
      List<FileScanTask> tasks = Lists.newArrayList(iterable);

      // Verify we get tasks back (specific count depends on implementation)
      assertThat(tasks).hasSize(1); // 1 data file: FILE_A

      // Verify specific task content and delete file associations
      FileScanTask taskWithDeletes =
          assertThat(tasks)
              .filteredOn(task -> !task.deletes().isEmpty())
              .first()
              .as("Expected at least one task with delete files")
              .actual();

      assertThat(taskWithDeletes.file().location()).isEqualTo(FILE_A.location());
      assertThat(taskWithDeletes.deletes()).hasSize(1); // 1 delete file: FILE_A_DELETES
      assertThat(taskWithDeletes.deletes().get(0).location()).isEqualTo(FILE_A_DELETES.location());
    }
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void remoteScanPlanningWithEqualityDeletes(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode)
      throws IOException {
    configurePlanningBehavior(planMode);
    Table table = restTableFor(scanPlanningCatalog(), "equality_deletes_test");
    setParserContext(table);

    // Add equality deletes that correspond to FILE_A
    table.newRowDelta().addDeletes(FILE_A_EQUALITY_DELETES).commit();

    // Execute scan planning - should handle equality deletes correctly
    try (CloseableIterable<FileScanTask> iterable = table.newScan().planFiles()) {
      List<FileScanTask> tasks = Lists.newArrayList(iterable);

      // Verify the task count and file paths
      assertThat(tasks).hasSize(1); // 1 data file: FILE_A

      // Verify specific task content and equality delete file associations
      FileScanTask taskWithDeletes =
          assertThat(tasks)
              .filteredOn(task -> !task.deletes().isEmpty())
              .first()
              .as("Expected at least one task with delete files")
              .actual();

      assertThat(taskWithDeletes.file().location()).isEqualTo(FILE_A.location());
      assertThat(taskWithDeletes.deletes()).hasSize(1);
      assertThat(taskWithDeletes.deletes().get(0).location())
          .isEqualTo(FILE_A_EQUALITY_DELETES.location());
    }
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void remoteScanPlanningWithMixedDeletes(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode)
      throws IOException {
    configurePlanningBehavior(planMode);
    Table table = restTableFor(scanPlanningCatalog(), "mixed_deletes_test");
    setParserContext(table);

    // Add both position and equality deletes in separate commits
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    table
        .newRowDelta()
        .addDeletes(FILE_B_EQUALITY_DELETES)
        .commit(); // Equality deletes for different partition

    // Execute scan planning - should handle mixed delete types correctly
    try (CloseableIterable<FileScanTask> iterable = table.newScan().planFiles()) {
      List<FileScanTask> tasks = Lists.newArrayList(iterable);

      // Verify task count: FILE_A only (FILE_B_EQUALITY_DELETES is in different partition)
      assertThat(tasks).hasSize(1); // 1 data file: FILE_A

      // Verify FILE_A with position deletes (FILE_B_EQUALITY_DELETES not associated since no
      // FILE_B)
      FileScanTask fileATask =
          assertThat(tasks)
              .filteredOn(task -> task.file().location().equals(FILE_A.location()))
              .first()
              .as("Expected FILE_A in scan tasks")
              .actual();

      assertThat(fileATask.deletes()).hasSize(1);
      assertThat(fileATask.deletes().get(0).location()).isEqualTo(FILE_A_DELETES.location());
    }
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void remoteScanPlanningWithMultipleDeleteFiles(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode)
      throws IOException {
    configurePlanningBehavior(planMode);
    Table table = restTableFor(scanPlanningCatalog(), "multiple_deletes_test");
    setParserContext(table);

    // Add FILE_B and FILE_C to the table (FILE_A is already added during table creation)
    table.newAppend().appendFile(FILE_B).appendFile(FILE_C).commit();

    // Add multiple delete files corresponding to FILE_A, FILE_B, FILE_C
    table
        .newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .addDeletes(FILE_C_EQUALITY_DELETES)
        .commit();

    // Execute scan planning with multiple delete files
    try (CloseableIterable<FileScanTask> iterable = table.newScan().planFiles()) {
      List<FileScanTask> tasks = Lists.newArrayList(iterable);

      // Verify we get tasks back (should have 3 data files: FILE_A, FILE_B, FILE_C)
      assertThat(tasks).hasSize(3); // 3 data files

      // Verify FILE_A with position deletes
      FileScanTask fileATask =
          assertThat(tasks)
              .filteredOn(task -> task.file().location().equals(FILE_A.location()))
              .first()
              .as("Expected FILE_A in scan tasks")
              .actual();
      assertThat(fileATask.deletes()).isNotEmpty(); // Has delete files
      assertThat(fileATask.deletes().stream().map(DeleteFile::location))
          .contains(FILE_A_DELETES.location()); // FILE_A_DELETES is present

      // Verify FILE_B with position deletes
      FileScanTask fileBTask =
          assertThat(tasks)
              .filteredOn(task -> task.file().location().equals(FILE_B.location()))
              .first()
              .as("Expected FILE_B in scan tasks")
              .actual();
      assertThat(fileBTask.deletes()).isNotEmpty(); // Has delete files
      assertThat(fileBTask.deletes().stream().map(DeleteFile::location))
          .contains(FILE_B_DELETES.location()); // FILE_B_DELETES is present

      // Verify FILE_C with equality deletes
      FileScanTask fileCTask =
          assertThat(tasks)
              .filteredOn(task -> task.file().location().equals(FILE_C.location()))
              .first()
              .as("Expected FILE_C in scan tasks")
              .actual();
      assertThat(fileCTask.deletes()).isNotEmpty(); // Has delete files
      assertThat(fileCTask.deletes().stream().map(DeleteFile::location))
          .contains(FILE_C_EQUALITY_DELETES.location()); // FILE_C_EQUALITY_DELETES is present
    }
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void remoteScanPlanningWithDeletesAndFiltering(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode)
      throws IOException {
    configurePlanningBehavior(planMode);
    Table table = restTableFor(scanPlanningCatalog(), "deletes_filtering_test");
    setParserContext(table);

    // Add FILE_B to have more data for filtering
    table.newAppend().appendFile(FILE_B).commit();

    // Add equality delete for FILE_B
    table.newRowDelta().addDeletes(FILE_B_EQUALITY_DELETES).commit();

    // Create a filtered scan and execute scan planning with filtering and deletes
    try (CloseableIterable<FileScanTask> iterable =
        table.newScan().filter(Expressions.lessThan("id", 4)).planFiles()) {
      List<FileScanTask> tasks = Lists.newArrayList(iterable);

      // Verify scan planning works with both filtering and deletes
      assertThat(tasks).hasSize(2); // 2 data files: FILE_A, FILE_B

      // FILE_A should have no delete files
      FileScanTask fileATask =
          assertThat(tasks)
              .filteredOn(task -> task.file().location().equals(FILE_A.location()))
              .first()
              .as("Expected FILE_A in scan tasks")
              .actual();
      assertThat(fileATask.deletes()).isEmpty(); // 0 delete files for FILE_A

      // FILE_B should have FILE_B_EQUALITY_DELETES
      FileScanTask fileBTask =
          assertThat(tasks)
              .filteredOn(task -> task.file().location().equals(FILE_B.location()))
              .first()
              .as("Expected FILE_B in scan tasks")
              .actual();
      assertThat(fileBTask.deletes()).hasSize(1); // 1 delete file: FILE_B_EQUALITY_DELETES
      assertThat(fileBTask.deletes().get(0).location())
          .isEqualTo(FILE_B_EQUALITY_DELETES.location());
    }
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void remoteScanPlanningDeletesCancellation(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode)
      throws IOException {
    configurePlanningBehavior(planMode);
    Table table = restTableFor(scanPlanningCatalog(), "deletes_cancellation_test");
    setParserContext(table);

    // Add deletes to make the scenario more complex
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A_EQUALITY_DELETES).commit();

    RESTTableScan restTableScan = restTableScanFor(table);

    // Get the iterable (which may involve async planning with deletes)
    try (CloseableIterable<FileScanTask> iterable = restTableScan.planFiles();
        CloseableIterator<FileScanTask> iterator = iterable.iterator()) {
      // Test cancellation works with delete files present
      // Resources will be closed automatically
    }

    // Verify cancellation method is still accessible
    assertThat(restTableScan.cancelPlan()).isFalse(); // No active plan at this point
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  void remoteScanPlanningWithTimeTravel(
      Function<TestPlanningBehavior.Builder, TestPlanningBehavior.Builder> planMode) {
    // Test server-side scan planning with time travel (snapshot-based queries)
    // Verify that snapshot IDs are correctly passed through the REST API
    // and that historical scans return the correct files and deletes
    configurePlanningBehavior(planMode);

    // Create table and add FILE_A (snapshot 1)
    Table table = restTableFor(scanPlanningCatalog(), "snapshot_scan_test");
    setParserContext(table);
    table.refresh();
    long snapshot1Id = table.currentSnapshot().snapshotId();

    // Add FILE_B (snapshot 2)
    table.newAppend().appendFile(FILE_B).commit();
    table.refresh();
    long snapshot2Id = table.currentSnapshot().snapshotId();
    assertThat(snapshot2Id).isNotEqualTo(snapshot1Id);

    // Add FILE_C and deletes (snapshots 3 and 4)
    table.newAppend().appendFile(FILE_C).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    table.refresh();
    long snapshot4Id = table.currentSnapshot().snapshotId();
    assertThat(snapshot4Id).isNotEqualTo(snapshot2Id);

    // Test 1: Scan at snapshot 1 (should only see FILE_A, no deletes)
    TableScan scan1 = table.newScan().useSnapshot(snapshot1Id);
    CloseableIterable<FileScanTask> iterable1 = scan1.planFiles();
    List<FileScanTask> tasks1 = Lists.newArrayList(iterable1);

    assertThat(tasks1).hasSize(1); // Only FILE_A exists at snapshot 1
    assertThat(tasks1.get(0).file().location()).isEqualTo(FILE_A.location());
    assertThat(tasks1.get(0).deletes()).isEmpty(); // No deletes at snapshot 1

    // Test 2: Scan at snapshot 2 (should see FILE_A and FILE_B, no deletes)
    TableScan scan2 = table.newScan().useSnapshot(snapshot2Id);
    CloseableIterable<FileScanTask> iterable2 = scan2.planFiles();
    List<FileScanTask> tasks2 = Lists.newArrayList(iterable2);

    assertThat(tasks2).hasSize(2); // FILE_A and FILE_B exist at snapshot 2
    assertThat(tasks2)
        .map(task -> task.file().location())
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
    assertThat(tasks2).allMatch(task -> task.deletes().isEmpty()); // No deletes at snapshot 2

    // Test 3: Scan at current snapshot (should see FILE_A, FILE_B, FILE_C, and FILE_A has deletes)
    TableScan scan3 = table.newScan().useSnapshot(snapshot4Id);
    CloseableIterable<FileScanTask> iterable3 = scan3.planFiles();
    List<FileScanTask> tasks3 = Lists.newArrayList(iterable3);

    assertThat(tasks3).hasSize(3); // All 3 data files exist at snapshot 4
    assertThat(tasks3)
        .map(task -> task.file().location())
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location(), FILE_C.location());

    // Verify FILE_A has deletes at snapshot 4
    FileScanTask fileATask =
        assertThat(tasks3)
            .filteredOn(task -> task.file().location().equals(FILE_A.location()))
            .first()
            .as("Expected FILE_A in scan tasks")
            .actual();
    assertThat(fileATask.deletes()).hasSize(1); // FILE_A_DELETES present at snapshot 4
    assertThat(fileATask.deletes().get(0).location()).isEqualTo(FILE_A_DELETES.location());

    // Verify FILE_B and FILE_C have no deletes at snapshot 4
    assertThat(tasks3)
        .filteredOn(
            task ->
                task.file().location().equals(FILE_B.location())
                    || task.file().location().equals(FILE_C.location()))
        .allMatch(task -> task.deletes().isEmpty());
  }

  @ParameterizedTest
  @EnumSource(PlanningMode.class)
  public void scanPlanningWithMultiplePartitionSpecs() throws IOException {
    configurePlanningBehavior(TestPlanningBehavior.Builder::synchronous);

    RESTTable table = restTableFor(scanPlanningCatalog(), "multiple_partition_specs");
    table.newFastAppend().appendFile(FILE_B).commit();

    // Evolve partition spec to bucket by id with 8 buckets instead of 16
    table.updateSpec().removeField("data_bucket").addField(Expressions.bucket("data", 8)).commit();

    // Create data file with new partition spec (spec-id=1)
    PartitionSpec newSpec = table.spec();
    assertThat(newSpec.specId()).isEqualTo(1);

    DataFile fileWithNewSpec =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-new-spec.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket_8=3") // 8-bucket partition
            .withRecordCount(2)
            .build();

    table.newFastAppend().appendFile(fileWithNewSpec).commit();
    setParserContext(table);

    // Scan table - should return all 3 files despite different partition specs
    try (CloseableIterable<FileScanTask> iterable = table.newScan().planFiles()) {
      List<FileScanTask> tasks = Lists.newArrayList(iterable);

      // Verify all 3 files are present
      assertThat(tasks).hasSize(3);
      assertThat(tasks)
          .map(task -> task.file().location())
          .containsExactlyInAnyOrder(
              FILE_A.location(), FILE_B.location(), fileWithNewSpec.location());

      // Verify files have correct partition spec IDs
      assertThat(tasks)
          .filteredOn(
              task ->
                  task.file().location().equals(FILE_A.location())
                      || task.file().location().equals(FILE_B.location()))
          .allMatch(task -> task.spec().specId() == 0);
      assertThat(tasks)
          .filteredOn(task -> task.file().location().equals(fileWithNewSpec.location()))
          .allMatch(task -> task.spec().specId() == 1);
    }
  }

  // ==================== Endpoint Support Tests ====================

  /** Helper class to hold catalog and adapter for endpoint support tests. */
  private static class CatalogWithAdapter {
    final RESTCatalog catalog;
    final RESTCatalogAdapter adapter;

    CatalogWithAdapter(RESTCatalog catalog, RESTCatalogAdapter adapter) {
      this.catalog = catalog;
      this.adapter = adapter;
    }
  }

  // Helper: Create base catalog endpoints (namespace and table operations)
  private List<Endpoint> baseCatalogEndpoints() {
    return ImmutableList.of(
        Endpoint.V1_CREATE_NAMESPACE,
        Endpoint.V1_LOAD_NAMESPACE,
        Endpoint.V1_LIST_TABLES,
        Endpoint.V1_CREATE_TABLE,
        Endpoint.V1_LOAD_TABLE,
        Endpoint.V1_UPDATE_TABLE);
  }

  // Helper: Create endpoint list with base + specified planning endpoints
  private List<Endpoint> endpointsWithPlanning(Endpoint... planningEndpoints) {
    return ImmutableList.<Endpoint>builder()
        .addAll(baseCatalogEndpoints())
        .add(planningEndpoints)
        .build();
  }

  // Helper: Create catalog with custom endpoint support and optional planning behavior
  private CatalogWithAdapter catalogWithEndpoints(
      List<Endpoint> endpoints, TestPlanningBehavior planningBehavior) {
    RESTCatalogAdapter adapter =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                if (ResourcePaths.config().equals(request.path())) {
                  return castResponse(
                      responseType, ConfigResponse.builder().withEndpoints(endpoints).build());
                }
                return super.execute(request, responseType, errorHandler, responseHeaders);
              }
            });

    if (planningBehavior != null) {
      adapter.setPlanningBehavior(planningBehavior);
    }

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            RESTCatalogProperties.REST_SCAN_PLANNING_ENABLED,
            "true"));
    return new CatalogWithAdapter(catalog, adapter);
  }

  @Test
  public void serverDoesNotSupportPlanningEndpoint() throws IOException {
    // Server doesn't support scan planning at all - should fall back to client-side planning
    CatalogWithAdapter catalogWithAdapter = catalogWithEndpoints(baseCatalogEndpoints(), null);
    RESTCatalog catalog = catalogWithAdapter.catalog;
    Table table = createTableWithScanPlanning(catalog, "no_planning_support");
    assertThat(table).isNotInstanceOf(RESTTable.class);
    table.newAppend().appendFile(FILE_A).commit();

    // Should fall back to client-side planning when endpoint is not supported
    assertThat(table.newScan().planFiles())
        .hasSize(1)
        .first()
        .extracting(ContentScanTask::file)
        .extracting(ContentFile::location)
        .isEqualTo(FILE_A.location());
  }

  @Test
  public void serverSupportsPlanningSyncOnlyNotAsync() {
    // Server supports submit (sync) but not fetch (async polling)
    // Use ASYNC planning to trigger SUBMITTED status, which will hit the Endpoint.check()
    CatalogWithAdapter catalogWithAdapter =
        catalogWithEndpoints(
            endpointsWithPlanning(
                Endpoint.V1_SUBMIT_TABLE_SCAN_PLAN, Endpoint.V1_FETCH_TABLE_SCAN_PLAN_TASKS),
            TestPlanningBehavior.builder().asynchronous().build());

    RESTCatalog catalog = catalogWithAdapter.catalog;
    RESTTable table = restTableFor(catalog, "async_not_supported");
    setParserContext(table);

    // Should fail with UnsupportedOperationException when trying to fetch async plan result
    // because V1_FETCH_TABLE_SCAN_PLAN endpoint is not supported
    assertThatThrownBy(restTableScanFor(table)::planFiles)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Server does not support endpoint: %s", Endpoint.V1_FETCH_TABLE_SCAN_PLAN);
  }

  @Test
  public void serverSupportsPlanningButNotPagination() {
    // Server supports planning but not task pagination endpoint
    // Use synchronousWithPagination (tasksPerPage=1) to trigger pagination, which will hit
    // Endpoint.check()
    CatalogWithAdapter catalogWithAdapter =
        catalogWithEndpoints(
            endpointsWithPlanning(
                Endpoint.V1_SUBMIT_TABLE_SCAN_PLAN,
                Endpoint.V1_FETCH_TABLE_SCAN_PLAN,
                Endpoint.V1_CANCEL_TABLE_SCAN_PLAN),
            TestPlanningBehavior.builder().synchronousWithPagination().build());

    RESTCatalog catalog = catalogWithAdapter.catalog;
    RESTTable table = restTableFor(catalog, "pagination_not_supported");
    table.newAppend().appendFile(FILE_B).commit();
    setParserContext(table);
    RESTTableScan scan = restTableScanFor(table);

    // Should fail with UnsupportedOperationException when trying to fetch paginated tasks
    // because V1_FETCH_TABLE_SCAN_PLAN_TASKS endpoint is not supported
    assertThatThrownBy(scan::planFiles)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Server does not support endpoint: %s", Endpoint.V1_FETCH_TABLE_SCAN_PLAN_TASKS);
  }

  @Test
  public void serverSupportsPlanningButNotCancellation() throws IOException {
    // Server supports planning but not the cancel endpoint
    CatalogWithAdapter catalogWithAdapter =
        catalogWithEndpoints(
            endpointsWithPlanning(
                Endpoint.V1_SUBMIT_TABLE_SCAN_PLAN,
                Endpoint.V1_FETCH_TABLE_SCAN_PLAN,
                Endpoint.V1_FETCH_TABLE_SCAN_PLAN_TASKS),
            TestPlanningBehavior.builder().asynchronous().build());

    RESTCatalog catalog = catalogWithAdapter.catalog;
    RESTTable table = restTableFor(catalog, "cancellation_not_supported");
    setParserContext(table);
    RESTTableScan scan = restTableScanFor(table);

    // Get the iterable - this starts async planning
    CloseableIterable<FileScanTask> iterable = scan.planFiles();

    // Cancellation should not fail even though server doesn't support it
    // The client should handle this gracefully by returning false
    boolean cancelled = scan.cancelPlan();
    iterable.close();

    // Verify no exception was thrown - cancelPlan returns false when endpoint not supported
    assertThat(cancelled).isFalse();
  }
}
