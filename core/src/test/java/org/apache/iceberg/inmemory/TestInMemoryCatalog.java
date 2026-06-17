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
package org.apache.iceberg.inmemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestInMemoryCatalog extends CatalogTests<InMemoryCatalog> {
  private InMemoryCatalog catalog;

  @BeforeEach
  public void before() {
    this.catalog =
        initCatalog(
            "in-memory-catalog",
            ImmutableMap.<String, String>builder()
                .put(
                    CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1", "catalog-default-key1")
                .put(
                    CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2", "catalog-default-key2")
                .put(
                    CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3",
                    "catalog-default-key3")
                .put(
                    CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3",
                    "catalog-override-key3")
                .put(
                    CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4",
                    "catalog-override-key4")
                .buildOrThrow());
  }

  @Override
  protected InMemoryCatalog catalog() {
    return catalog;
  }

  @Override
  protected InMemoryCatalog initCatalog(
      String catalogName, Map<String, String> additionalProperties) {
    InMemoryCatalog cat = new InMemoryCatalog();
    cat.initialize(catalogName, additionalProperties);
    return cat;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsEmptyNamespace() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  @Test
  @Override
  public void testLoadTableWithMissingMetadataFile(@TempDir Path tempDir) throws IOException {

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TBL.namespace());
    }

    catalog.buildTable(TBL, SCHEMA).create();
    assertThat(catalog.tableExists(TBL)).as("Table should exist").isTrue();
    Table table = catalog.loadTable(TBL);
    String metadataFileLocation =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    table.io().deleteFile(metadataFileLocation);
    assertThatThrownBy(() -> catalog.loadTable(TBL))
        .isInstanceOf(NotFoundException.class)
        .hasMessage("No in-memory file found for location: " + metadataFileLocation);
    table.io().newOutputFile(metadataFileLocation).create();
  }

  @Test
  public void instanceIdSharesNamespacesAndTablesAcrossInstances() throws IOException {
    String instanceId = "shared-state-tables";
    InMemoryCatalog.clearInstanceState(instanceId);

    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.INSTANCE_ID, instanceId);

    InMemoryCatalog first = new InMemoryCatalog();
    first.initialize("first", sharedProps);
    InMemoryCatalog second = new InMemoryCatalog();
    second.initialize("second", sharedProps);

    try {
      first.createNamespace(TBL.namespace());
      first.buildTable(TBL, SCHEMA).create();

      assertThat(second.namespaceExists(TBL.namespace()))
          .as("Namespace created via first instance should be visible to second")
          .isTrue();
      assertThat(second.tableExists(TBL))
          .as("Table created via first instance should be visible to second")
          .isTrue();
      assertThat(second.loadTable(TBL).schema().asStruct())
          .as("Schema observed via second instance should match first")
          .isEqualTo(first.loadTable(TBL).schema().asStruct());

      TableIdentifier other = TableIdentifier.of(TBL.namespace(), "other");
      second.buildTable(other, SCHEMA).create();
      assertThat(first.tableExists(other))
          .as("Table created via second instance should be visible to first")
          .isTrue();

      second.dropTable(TBL, false);
      assertThat(first.tableExists(TBL))
          .as("Table dropped via second instance should disappear from first")
          .isFalse();
    } finally {
      first.close();
      second.close();
      InMemoryCatalog.clearInstanceState(instanceId);
    }
  }

  @Test
  public void instanceIdSharesViewsAcrossInstances() throws IOException {
    String instanceId = "shared-state-views";
    InMemoryCatalog.clearInstanceState(instanceId);

    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.INSTANCE_ID, instanceId);

    InMemoryCatalog first = new InMemoryCatalog();
    first.initialize("first", sharedProps);
    InMemoryCatalog second = new InMemoryCatalog();
    second.initialize("second", sharedProps);

    TableIdentifier viewIdentifier = TableIdentifier.of(TBL.namespace(), "vw");
    try {
      first.createNamespace(TBL.namespace());
      first
          .buildView(viewIdentifier)
          .withSchema(SCHEMA)
          .withDefaultNamespace(TBL.namespace())
          .withQuery("spark", "SELECT 1")
          .create();

      assertThat(second.viewExists(viewIdentifier))
          .as("View created via first instance should be visible to second")
          .isTrue();

      View viewFromSecond = second.loadView(viewIdentifier);
      assertThat(viewFromSecond.schema().asStruct())
          .as("View schema observed via second instance should match first")
          .isEqualTo(SCHEMA.asStruct());

      second.dropView(viewIdentifier);
      assertThat(first.viewExists(viewIdentifier))
          .as("View dropped via second instance should disappear from first")
          .isFalse();
    } finally {
      first.close();
      second.close();
      InMemoryCatalog.clearInstanceState(instanceId);
    }
  }

  @Test
  public void distinctInstanceIdsKeepStateIsolated() throws IOException {
    String firstId = "isolated-first";
    String secondId = "isolated-second";
    InMemoryCatalog.clearInstanceState(firstId);
    InMemoryCatalog.clearInstanceState(secondId);

    InMemoryCatalog firstCatalog = new InMemoryCatalog();
    firstCatalog.initialize("first", ImmutableMap.of(InMemoryCatalog.INSTANCE_ID, firstId));
    InMemoryCatalog secondCatalog = new InMemoryCatalog();
    secondCatalog.initialize("second", ImmutableMap.of(InMemoryCatalog.INSTANCE_ID, secondId));

    try {
      firstCatalog.createNamespace(TBL.namespace());
      firstCatalog.buildTable(TBL, SCHEMA).create();

      assertThat(secondCatalog.namespaceExists(TBL.namespace()))
          .as("Namespace from first instance must not leak to a different instance id")
          .isFalse();
      assertThat(secondCatalog.tableExists(TBL))
          .as("Table from first instance must not leak to a different instance id")
          .isFalse();
    } finally {
      firstCatalog.close();
      secondCatalog.close();
      InMemoryCatalog.clearInstanceState(firstId);
      InMemoryCatalog.clearInstanceState(secondId);
    }
  }

  @Test
  public void instancesWithoutInstanceIdKeepStateIsolated() throws IOException {
    InMemoryCatalog firstCatalog = new InMemoryCatalog();
    firstCatalog.initialize("first", ImmutableMap.of());
    InMemoryCatalog secondCatalog = new InMemoryCatalog();
    secondCatalog.initialize("second", ImmutableMap.of());

    try {
      firstCatalog.createNamespace(TBL.namespace());
      firstCatalog.buildTable(TBL, SCHEMA).create();

      assertThat(secondCatalog.namespaceExists(TBL.namespace()))
          .as("Two catalogs without an instance id must not share state")
          .isFalse();
      assertThat(secondCatalog.tableExists(TBL))
          .as("Two catalogs without an instance id must not share tables")
          .isFalse();
    } finally {
      firstCatalog.close();
      secondCatalog.close();
    }
  }

  @Test
  public void closeClearsPrivateStateButPreservesSharedState() throws IOException {
    InMemoryCatalog privateCatalog = new InMemoryCatalog();
    privateCatalog.initialize("private", ImmutableMap.of());
    privateCatalog.createNamespace(TBL.namespace());
    privateCatalog.buildTable(TBL, SCHEMA).create();
    privateCatalog.close();

    InMemoryCatalog reopened = new InMemoryCatalog();
    reopened.initialize("private", ImmutableMap.of());
    try {
      assertThat(reopened.namespaceExists(TBL.namespace()))
          .as("Private state should be dropped on close")
          .isFalse();
    } finally {
      reopened.close();
    }

    String instanceId = "shared-survives-close";
    InMemoryCatalog.clearInstanceState(instanceId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.INSTANCE_ID, instanceId);
    try {
      InMemoryCatalog firstShared = new InMemoryCatalog();
      firstShared.initialize("first", sharedProps);
      firstShared.createNamespace(TBL.namespace());
      firstShared.buildTable(TBL, SCHEMA).create();
      firstShared.close();

      InMemoryCatalog secondShared = new InMemoryCatalog();
      secondShared.initialize("second", sharedProps);
      try {
        assertThat(secondShared.tableExists(TBL))
            .as("Closing one shared instance must not drop the shared store")
            .isTrue();
      } finally {
        secondShared.close();
      }
    } finally {
      InMemoryCatalog.clearInstanceState(instanceId);
    }
  }

  @Test
  public void clearInstanceStateRemovesSharedStore() throws IOException {
    String instanceId = "clear-state";
    InMemoryCatalog.clearInstanceState(instanceId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.INSTANCE_ID, instanceId);

    InMemoryCatalog firstCatalog = new InMemoryCatalog();
    firstCatalog.initialize("first", sharedProps);
    firstCatalog.createNamespace(TBL.namespace());
    firstCatalog.buildTable(TBL, SCHEMA).create();
    firstCatalog.close();

    InMemoryCatalog.clearInstanceState(instanceId);

    InMemoryCatalog secondCatalog = new InMemoryCatalog();
    secondCatalog.initialize("second", sharedProps);
    try {
      assertThat(secondCatalog.namespaceExists(TBL.namespace()))
          .as("Shared store should be empty after clearInstanceState")
          .isFalse();
      assertThat(secondCatalog.tableExists(TBL))
          .as("Shared store should drop tables after clearInstanceState")
          .isFalse();
    } finally {
      secondCatalog.close();
      InMemoryCatalog.clearInstanceState(instanceId);
    }
  }

  @Test
  public void clearInstanceStateIsNoOpForUnknownId() {
    assertThatCode(() -> InMemoryCatalog.clearInstanceState("never-registered"))
        .as("Clearing an unknown instance id should be a safe no-op")
        .doesNotThrowAnyException();
  }

  @Test
  public void initializeIsIdempotentWhenInstanceIdRemoved() throws IOException {
    String instanceId = "idempotent-init";
    InMemoryCatalog.clearInstanceState(instanceId);

    InMemoryCatalog reusable = new InMemoryCatalog();
    try {
      reusable.initialize("reusable", ImmutableMap.of(InMemoryCatalog.INSTANCE_ID, instanceId));
      reusable.createNamespace(TBL.namespace());
      reusable.buildTable(TBL, SCHEMA).create();

      reusable.initialize("reusable", ImmutableMap.of());

      assertThat(reusable.namespaceExists(TBL.namespace()))
          .as("Re-initializing without an instance id must switch to a fresh private store")
          .isFalse();
      assertThat(reusable.tableExists(TBL))
          .as("Re-initializing without an instance id must not see the previously shared table")
          .isFalse();
    } finally {
      reusable.close();
      InMemoryCatalog.clearInstanceState(instanceId);
    }
  }

  @Test
  public void concurrentInitializeWithSameInstanceIdSharesStorage()
      throws IOException, InterruptedException, ExecutionException {
    String instanceId = "concurrent-init";
    InMemoryCatalog.clearInstanceState(instanceId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.INSTANCE_ID, instanceId);

    int parallelism = 8;
    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    List<InMemoryCatalog> catalogs = Lists.newArrayList();
    try {
      List<Future<InMemoryCatalog>> futures = Lists.newArrayList();
      for (int i = 0; i < parallelism; i++) {
        final String name = "catalog-" + i;
        futures.add(
            executor.submit(
                () -> {
                  InMemoryCatalog cat = new InMemoryCatalog();
                  cat.initialize(name, sharedProps);
                  return cat;
                }));
      }

      for (Future<InMemoryCatalog> future : futures) {
        catalogs.add(future.get());
      }

      InMemoryCatalog writer = catalogs.get(0);
      writer.createNamespace(TBL.namespace());
      writer.buildTable(TBL, SCHEMA).create();

      for (InMemoryCatalog reader : catalogs) {
        assertThat(reader.tableExists(TBL))
            .as("All instances racing to initialize with the same id should observe shared writes")
            .isTrue();
      }
    } finally {
      executor.shutdownNow();
      for (InMemoryCatalog cat : catalogs) {
        cat.close();
      }
      InMemoryCatalog.clearInstanceState(instanceId);
    }
  }
}
