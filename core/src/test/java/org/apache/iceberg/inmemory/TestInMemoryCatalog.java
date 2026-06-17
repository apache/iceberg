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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
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
  public void sharedStoreSharesNamespacesAndTablesFromFirstToSecond() throws IOException {
    String storeId = "shared-store-tables-forward";
    InMemoryCatalog.clearSharedStore(storeId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

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
    } finally {
      first.close();
      second.close();
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }

  @Test
  public void sharedStoreSharesWritesFromSecondBackToFirst() throws IOException {
    String storeId = "shared-store-tables-reverse";
    InMemoryCatalog.clearSharedStore(storeId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

    InMemoryCatalog first = new InMemoryCatalog();
    first.initialize("first", sharedProps);
    InMemoryCatalog second = new InMemoryCatalog();
    second.initialize("second", sharedProps);

    try {
      second.createNamespace(TBL.namespace());
      second.buildTable(TBL, SCHEMA).create();

      assertThat(first.tableExists(TBL))
          .as("Table created via second instance should be visible to first")
          .isTrue();

      second.dropTable(TBL, false);
      assertThat(first.tableExists(TBL))
          .as("Table dropped via second instance should disappear from first")
          .isFalse();
    } finally {
      first.close();
      second.close();
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }

  @Test
  public void sharedStoreSharesViewsAcrossInstances() throws IOException {
    String storeId = "shared-store-views";
    InMemoryCatalog.clearSharedStore(storeId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

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
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }

  @Test
  public void sharedStoreSharesRenameTableAndNamespacePropertyEdits() throws IOException {
    String storeId = "shared-store-rename-and-props";
    InMemoryCatalog.clearSharedStore(storeId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

    InMemoryCatalog first = new InMemoryCatalog();
    first.initialize("first", sharedProps);
    InMemoryCatalog second = new InMemoryCatalog();
    second.initialize("second", sharedProps);

    TableIdentifier renamed = TableIdentifier.of(TBL.namespace(), "renamed");
    try {
      first.createNamespace(TBL.namespace());
      first.buildTable(TBL, SCHEMA).create();

      first.renameTable(TBL, renamed);
      assertThat(second.tableExists(renamed))
          .as("renameTable on first should be observed at the new identifier on second")
          .isTrue();
      assertThat(second.tableExists(TBL))
          .as("renameTable on first should remove the old identifier from second")
          .isFalse();

      second.setProperties(TBL.namespace(), ImmutableMap.of("origin", "second"));
      assertThat(first.loadNamespaceMetadata(TBL.namespace()))
          .as("setProperties on second should be observable from first")
          .containsEntry("origin", "second");

      second.removeProperties(TBL.namespace(), java.util.Collections.singleton("origin"));
      assertThat(first.loadNamespaceMetadata(TBL.namespace()))
          .as("removeProperties on second should be observable from first")
          .doesNotContainKey("origin");
    } finally {
      first.close();
      second.close();
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }

  @Test
  public void sharedStoreDropNamespaceEnforcesEmptiness() throws IOException {
    String storeId = "shared-store-drop-namespace";
    InMemoryCatalog.clearSharedStore(storeId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

    InMemoryCatalog first = new InMemoryCatalog();
    first.initialize("first", sharedProps);
    InMemoryCatalog second = new InMemoryCatalog();
    second.initialize("second", sharedProps);

    try {
      first.createNamespace(TBL.namespace());
      first.buildTable(TBL, SCHEMA).create();

      assertThatThrownBy(() -> second.dropNamespace(TBL.namespace()))
          .as("dropNamespace from second must see the table created via first")
          .isInstanceOf(NamespaceNotEmptyException.class)
          .hasMessageContaining("not empty");

      first.dropTable(TBL, false);
      assertThat(second.dropNamespace(TBL.namespace()))
          .as("dropNamespace from second should succeed once the table is removed via first")
          .isTrue();
    } finally {
      first.close();
      second.close();
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }

  @Test
  public void distinctSharedStoreIdsKeepStateIsolated() throws IOException {
    String firstId = "isolated-first";
    String secondId = "isolated-second";
    InMemoryCatalog.clearSharedStore(firstId);
    InMemoryCatalog.clearSharedStore(secondId);

    InMemoryCatalog firstCatalog = new InMemoryCatalog();
    firstCatalog.initialize("first", ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, firstId));
    InMemoryCatalog secondCatalog = new InMemoryCatalog();
    secondCatalog.initialize("second", ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, secondId));

    try {
      firstCatalog.createNamespace(TBL.namespace());
      firstCatalog.buildTable(TBL, SCHEMA).create();

      assertThat(secondCatalog.namespaceExists(TBL.namespace()))
          .as("Namespace from first instance must not leak to a different shared-store id")
          .isFalse();
      assertThat(secondCatalog.tableExists(TBL))
          .as("Table from first instance must not leak to a different shared-store id")
          .isFalse();
    } finally {
      firstCatalog.close();
      secondCatalog.close();
      InMemoryCatalog.clearSharedStore(firstId);
      InMemoryCatalog.clearSharedStore(secondId);
    }
  }

  @Test
  public void instancesWithoutSharedStoreIdKeepStateIsolated() throws IOException {
    InMemoryCatalog firstCatalog = new InMemoryCatalog();
    firstCatalog.initialize("first", ImmutableMap.of());
    InMemoryCatalog secondCatalog = new InMemoryCatalog();
    secondCatalog.initialize("second", ImmutableMap.of());

    try {
      firstCatalog.createNamespace(TBL.namespace());
      firstCatalog.buildTable(TBL, SCHEMA).create();

      assertThat(secondCatalog.namespaceExists(TBL.namespace()))
          .as("Two catalogs without a shared-store id must not share namespaces")
          .isFalse();
      assertThat(secondCatalog.tableExists(TBL))
          .as("Two catalogs without a shared-store id must not share tables")
          .isFalse();
    } finally {
      firstCatalog.close();
      secondCatalog.close();
    }
  }

  @Test
  public void closeOnPrivateInstanceClearsState() throws IOException {
    InMemoryCatalog privateCatalog = new InMemoryCatalog();
    privateCatalog.initialize("private", ImmutableMap.of());
    privateCatalog.createNamespace(TBL.namespace());
    privateCatalog.buildTable(TBL, SCHEMA).create();
    privateCatalog.close();

    InMemoryCatalog reopened = new InMemoryCatalog();
    reopened.initialize("private", ImmutableMap.of());
    try {
      assertThat(reopened.namespaceExists(TBL.namespace()))
          .as("Private state should be cleared on close")
          .isFalse();
    } finally {
      reopened.close();
    }
  }

  @Test
  public void closeOnSharedInstancePreservesStore() throws IOException {
    String storeId = "shared-survives-close";
    InMemoryCatalog.clearSharedStore(storeId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);
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
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }

  @Test
  public void clearSharedStoreRemovesStateForFutureInstances() throws IOException {
    String storeId = "clear-store";
    InMemoryCatalog.clearSharedStore(storeId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

    InMemoryCatalog firstCatalog = new InMemoryCatalog();
    firstCatalog.initialize("first", sharedProps);
    firstCatalog.createNamespace(TBL.namespace());
    firstCatalog.buildTable(TBL, SCHEMA).create();
    firstCatalog.close();

    InMemoryCatalog.clearSharedStore(storeId);

    InMemoryCatalog secondCatalog = new InMemoryCatalog();
    secondCatalog.initialize("second", sharedProps);
    try {
      assertThat(secondCatalog.namespaceExists(TBL.namespace()))
          .as("Shared store should be empty for new instances after clearSharedStore")
          .isFalse();
      assertThat(secondCatalog.tableExists(TBL))
          .as("Shared store should drop tables for new instances after clearSharedStore")
          .isFalse();
    } finally {
      secondCatalog.close();
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }

  @Test
  public void clearSharedStoreLeavesLiveInstancesPointingAtTheirStore() throws IOException {
    String storeId = "clear-while-live";
    InMemoryCatalog.clearSharedStore(storeId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

    InMemoryCatalog live = new InMemoryCatalog();
    live.initialize("live", sharedProps);
    try {
      live.createNamespace(TBL.namespace());
      live.buildTable(TBL, SCHEMA).create();

      InMemoryCatalog.clearSharedStore(storeId);

      assertThat(live.tableExists(TBL))
          .as("Live instances retain a reference to the prior store after clearSharedStore")
          .isTrue();

      InMemoryCatalog freshlyOpened = new InMemoryCatalog();
      freshlyOpened.initialize("fresh", sharedProps);
      try {
        assertThat(freshlyOpened.tableExists(TBL))
            .as("Newly initialized instances see a fresh store after clearSharedStore")
            .isFalse();
      } finally {
        freshlyOpened.close();
      }
    } finally {
      live.close();
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }

  @Test
  public void clearSharedStoreIsNoOpForUnknownId() {
    assertThatCode(() -> InMemoryCatalog.clearSharedStore("never-registered"))
        .as("Clearing an unknown shared-store id should be a safe no-op")
        .doesNotThrowAnyException();
  }

  @Test
  public void emptySharedStoreIdValuePartitionsItsOwnStore() throws IOException {
    InMemoryCatalog.clearSharedStore("");
    Map<String, String> emptyKeyProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, "");

    InMemoryCatalog first = new InMemoryCatalog();
    first.initialize("first", emptyKeyProps);
    InMemoryCatalog second = new InMemoryCatalog();
    second.initialize("second", emptyKeyProps);

    try {
      first.createNamespace(TBL.namespace());
      first.buildTable(TBL, SCHEMA).create();

      assertThat(second.tableExists(TBL))
          .as("Two instances using the empty-string shared-store id should share that store")
          .isTrue();
    } finally {
      first.close();
      second.close();
      InMemoryCatalog.clearSharedStore("");
    }
  }

  @Test
  public void reinitializeWithoutSharedStoreIdSwitchesToPrivateStore() throws IOException {
    String storeId = "reinit-to-private";
    InMemoryCatalog.clearSharedStore(storeId);

    InMemoryCatalog reusable = new InMemoryCatalog();
    try {
      reusable.initialize("reusable", ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId));
      reusable.createNamespace(TBL.namespace());
      reusable.buildTable(TBL, SCHEMA).create();

      reusable.initialize("reusable", ImmutableMap.of());

      assertThat(reusable.namespaceExists(TBL.namespace()))
          .as("Reinitializing without a shared-store id must switch to a fresh private store")
          .isFalse();
      assertThat(reusable.tableExists(TBL))
          .as("Reinitializing without a shared-store id must not see the previously shared table")
          .isFalse();
    } finally {
      reusable.close();
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }

  @Test
  public void concurrentInitializeWithSameSharedStoreIdConvergesOnOneStore()
      throws InterruptedException, ExecutionException {
    String storeId = "concurrent-init";
    InMemoryCatalog.clearSharedStore(storeId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

    int parallelism = 8;
    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    List<InMemoryCatalog> catalogs = Lists.newArrayList();
    boolean threw = true;
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<InMemoryCatalog>> futures = Lists.newArrayList();
      for (int i = 0; i < parallelism; i++) {
        String name = "catalog-" + i;
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  InMemoryCatalog cat = new InMemoryCatalog();
                  cat.initialize(name, sharedProps);
                  return cat;
                }));
      }

      start.countDown();
      for (Future<InMemoryCatalog> future : futures) {
        catalogs.add(future.get(30, TimeUnit.SECONDS));
      }

      InMemoryCatalog writer = catalogs.get(0);
      writer.createNamespace(TBL.namespace());
      writer.buildTable(TBL, SCHEMA).create();

      for (InMemoryCatalog reader : catalogs) {
        assertThat(reader.tableExists(TBL))
            .as("All instances racing to initialize on the same shared-store id share state")
            .isTrue();
      }
      threw = false;
    } catch (java.util.concurrent.TimeoutException e) {
      throw new RuntimeException("Concurrent initialize timed out", e);
    } finally {
      executor.shutdownNow();
      for (InMemoryCatalog cat : catalogs) {
        try {
          cat.close();
        } catch (IOException closeError) {
          if (!threw) {
            throw new RuntimeException(closeError);
          }
        }
      }
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }

  @Test
  public void concurrentCreateOfSameTableThroughSharedStoreSerializes()
      throws InterruptedException, ExecutionException {
    String storeId = "concurrent-create";
    InMemoryCatalog.clearSharedStore(storeId);
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

    Namespace namespace = Namespace.of("ns");
    TableIdentifier shared = TableIdentifier.of(namespace, "racey");

    int parallelism = 6;
    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    List<InMemoryCatalog> catalogs = Lists.newArrayList();
    boolean threw = true;
    try {
      for (int i = 0; i < parallelism; i++) {
        InMemoryCatalog cat = new InMemoryCatalog();
        cat.initialize("racer-" + i, sharedProps);
        catalogs.add(cat);
      }
      catalogs.get(0).createNamespace(namespace);

      CountDownLatch start = new CountDownLatch(1);
      AtomicInteger successes = new AtomicInteger();
      AtomicInteger alreadyExists = new AtomicInteger();
      List<Future<?>> futures = Lists.newArrayList();
      for (InMemoryCatalog cat : catalogs) {
        futures.add(
            executor.submit(
                () -> {
                  try {
                    start.await();
                    cat.buildTable(shared, SCHEMA).create();
                    successes.incrementAndGet();
                  } catch (AlreadyExistsException expected) {
                    alreadyExists.incrementAndGet();
                  } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                  }
                  return null;
                }));
      }

      start.countDown();
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }

      assertThat(successes.get())
          .as("Exactly one racer should successfully create the shared table")
          .isEqualTo(1);
      assertThat(alreadyExists.get())
          .as("Every other racer should observe the AlreadyExistsException")
          .isEqualTo(parallelism - 1);
      assertThat(catalogs.get(0).tableExists(shared))
          .as("The shared table should be visible from any racer")
          .isTrue();
      threw = false;
    } catch (java.util.concurrent.TimeoutException e) {
      throw new RuntimeException("Concurrent create timed out", e);
    } finally {
      executor.shutdownNow();
      for (InMemoryCatalog cat : catalogs) {
        try {
          cat.close();
        } catch (IOException closeError) {
          if (!threw) {
            throw new RuntimeException(closeError);
          }
        }
      }
      InMemoryCatalog.clearSharedStore(storeId);
    }
  }
}
