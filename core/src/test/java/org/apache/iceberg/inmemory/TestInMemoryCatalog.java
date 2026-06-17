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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestInMemoryCatalog extends CatalogTests<InMemoryCatalog> {
  private InMemoryCatalog catalog;
  private final List<InMemoryCatalog> trackedCatalogs = Lists.newArrayList();
  private final Set<String> trackedSharedStoreIds = Sets.newHashSet();

  @AfterEach
  public void cleanupTrackedResources() throws IOException {
    IOException firstFailure = null;
    for (InMemoryCatalog tracked : trackedCatalogs) {
      try {
        tracked.close();
      } catch (IOException e) {
        if (firstFailure == null) {
          firstFailure = e;
        }
      }
    }
    trackedCatalogs.clear();
    for (String storeId : trackedSharedStoreIds) {
      InMemoryCatalog.clearSharedStore(storeId);
    }
    trackedSharedStoreIds.clear();
    if (firstFailure != null) {
      throw firstFailure;
    }
  }

  /**
   * Initializes a fresh {@link InMemoryCatalog} and registers it for {@link AfterEach} close. The
   * registration is best-effort: if the test asserts before reaching the registration call the
   * normal JVM cleanup still applies.
   */
  private InMemoryCatalog newTrackedCatalog(String name, Map<String, String> properties) {
    InMemoryCatalog tracked = new InMemoryCatalog();
    tracked.initialize(name, properties);
    trackedCatalogs.add(tracked);
    return tracked;
  }

  /**
   * Marks a shared-store ID for {@link AfterEach} cleanup and clears any leftover state from a
   * previous run before returning the same id. Returning the id keeps tests one-liner.
   */
  private String reservedSharedStoreId(String storeId) {
    InMemoryCatalog.clearSharedStore(storeId);
    trackedSharedStoreIds.add(storeId);
    return storeId;
  }

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
  public void sharedStoreSharesNamespacesAndTablesFromFirstToSecond() {
    Map<String, String> sharedProps =
        ImmutableMap.of(
            InMemoryCatalog.SHARED_STORE_ID, reservedSharedStoreId("shared-store-tables-forward"));
    InMemoryCatalog first = newTrackedCatalog("first", sharedProps);
    InMemoryCatalog second = newTrackedCatalog("second", sharedProps);

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
  }

  @Test
  public void sharedStoreSharesWritesFromSecondBackToFirst() {
    Map<String, String> sharedProps =
        ImmutableMap.of(
            InMemoryCatalog.SHARED_STORE_ID, reservedSharedStoreId("shared-store-tables-reverse"));
    InMemoryCatalog first = newTrackedCatalog("first", sharedProps);
    InMemoryCatalog second = newTrackedCatalog("second", sharedProps);

    second.createNamespace(TBL.namespace());
    second.buildTable(TBL, SCHEMA).create();
    assertThat(first.tableExists(TBL))
        .as("Table created via second instance should be visible to first")
        .isTrue();

    second.dropTable(TBL, false);
    assertThat(first.tableExists(TBL))
        .as("Table dropped via second instance should disappear from first")
        .isFalse();
  }

  @Test
  public void sharedStoreSharesViewsAcrossInstances() {
    Map<String, String> sharedProps =
        ImmutableMap.of(
            InMemoryCatalog.SHARED_STORE_ID, reservedSharedStoreId("shared-store-views"));
    InMemoryCatalog first = newTrackedCatalog("first", sharedProps);
    InMemoryCatalog second = newTrackedCatalog("second", sharedProps);

    TableIdentifier viewIdentifier = TableIdentifier.of(TBL.namespace(), "vw");
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
  }

  @Test
  public void sharedStoreSharesRenameTable() {
    Map<String, String> sharedProps =
        ImmutableMap.of(
            InMemoryCatalog.SHARED_STORE_ID, reservedSharedStoreId("shared-store-rename"));
    InMemoryCatalog first = newTrackedCatalog("first", sharedProps);
    InMemoryCatalog second = newTrackedCatalog("second", sharedProps);

    TableIdentifier renamed = TableIdentifier.of(TBL.namespace(), "renamed");
    first.createNamespace(TBL.namespace());
    first.buildTable(TBL, SCHEMA).create();

    first.renameTable(TBL, renamed);
    assertThat(second.tableExists(renamed))
        .as("renameTable on first should be observed at the new identifier on second")
        .isTrue();
    assertThat(second.tableExists(TBL))
        .as("renameTable on first should remove the old identifier from second")
        .isFalse();
  }

  @Test
  public void sharedStoreSharesSetProperties() {
    Map<String, String> sharedProps =
        ImmutableMap.of(
            InMemoryCatalog.SHARED_STORE_ID, reservedSharedStoreId("shared-store-set-props"));
    InMemoryCatalog first = newTrackedCatalog("first", sharedProps);
    InMemoryCatalog second = newTrackedCatalog("second", sharedProps);

    first.createNamespace(TBL.namespace());

    second.setProperties(TBL.namespace(), ImmutableMap.of("origin", "second"));
    assertThat(first.loadNamespaceMetadata(TBL.namespace()))
        .as("setProperties on second should be observable from first")
        .containsEntry("origin", "second");
  }

  @Test
  public void sharedStoreSharesRemoveProperties() {
    Map<String, String> sharedProps =
        ImmutableMap.of(
            InMemoryCatalog.SHARED_STORE_ID, reservedSharedStoreId("shared-store-remove-props"));
    InMemoryCatalog first = newTrackedCatalog("first", sharedProps);
    InMemoryCatalog second = newTrackedCatalog("second", sharedProps);

    first.createNamespace(TBL.namespace(), ImmutableMap.of("origin", "first"));

    second.removeProperties(TBL.namespace(), Collections.singleton("origin"));
    assertThat(first.loadNamespaceMetadata(TBL.namespace()))
        .as("removeProperties on second should be observable from first")
        .doesNotContainKey("origin");
  }

  @Test
  public void sharedStoreDropNamespaceEnforcesEmptiness() {
    Map<String, String> sharedProps =
        ImmutableMap.of(
            InMemoryCatalog.SHARED_STORE_ID, reservedSharedStoreId("shared-store-drop-namespace"));
    InMemoryCatalog first = newTrackedCatalog("first", sharedProps);
    InMemoryCatalog second = newTrackedCatalog("second", sharedProps);

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
  }

  @Test
  public void dropNamespaceRejectsSiblingPrefixCollisions() {
    InMemoryCatalog cat = newTrackedCatalog("prefix-collision", ImmutableMap.of());
    Namespace parent = Namespace.of("a", "b");
    Namespace siblingPrefix = Namespace.of("a", "bb");
    Namespace nestedUnderSibling = Namespace.of("a", "bb", "c");
    cat.createNamespace(Namespace.of("a"));
    cat.createNamespace(parent);
    cat.createNamespace(siblingPrefix);
    cat.createNamespace(nestedUnderSibling);

    assertThat(cat.dropNamespace(parent))
        .as("dropNamespace must succeed for an empty namespace despite a sibling-prefix neighbor")
        .isTrue();

    assertThatThrownBy(() -> cat.dropNamespace(siblingPrefix))
        .as("dropNamespace must reject a parent that still has nested children")
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessageContaining("child namespace");

    assertThat(cat.dropNamespace(nestedUnderSibling)).isTrue();
    assertThat(cat.dropNamespace(siblingPrefix))
        .as("Once nested namespace is removed, the sibling parent drops cleanly")
        .isTrue();
  }

  @Test
  public void distinctSharedStoreIdsKeepStateIsolated() {
    InMemoryCatalog firstCatalog =
        newTrackedCatalog(
            "first",
            ImmutableMap.of(
                InMemoryCatalog.SHARED_STORE_ID, reservedSharedStoreId("isolated-first")));
    InMemoryCatalog secondCatalog =
        newTrackedCatalog(
            "second",
            ImmutableMap.of(
                InMemoryCatalog.SHARED_STORE_ID, reservedSharedStoreId("isolated-second")));

    firstCatalog.createNamespace(TBL.namespace());
    firstCatalog.buildTable(TBL, SCHEMA).create();

    assertThat(secondCatalog.namespaceExists(TBL.namespace()))
        .as("Namespace from first instance must not leak to a different shared-store ID")
        .isFalse();
    assertThat(secondCatalog.tableExists(TBL))
        .as("Table from first instance must not leak to a different shared-store ID")
        .isFalse();
  }

  @Test
  public void instancesWithoutSharedStoreIdKeepStateIsolated() {
    InMemoryCatalog firstCatalog = newTrackedCatalog("first", ImmutableMap.of());
    InMemoryCatalog secondCatalog = newTrackedCatalog("second", ImmutableMap.of());

    firstCatalog.createNamespace(TBL.namespace());
    firstCatalog.buildTable(TBL, SCHEMA).create();

    assertThat(secondCatalog.namespaceExists(TBL.namespace()))
        .as("Two catalogs without a shared-store ID must not share namespaces")
        .isFalse();
    assertThat(secondCatalog.tableExists(TBL))
        .as("Two catalogs without a shared-store ID must not share tables")
        .isFalse();
  }

  @Test
  public void closeOnPrivateInstanceClearsState() throws IOException {
    InMemoryCatalog privateCatalog = new InMemoryCatalog();
    privateCatalog.initialize("private", ImmutableMap.of());
    privateCatalog.createNamespace(TBL.namespace());
    privateCatalog.buildTable(TBL, SCHEMA).create();
    assertThat(privateCatalog.namespaceExists(TBL.namespace()))
        .as("Sanity: namespace must be present before close()")
        .isTrue();

    privateCatalog.close();

    assertThat(privateCatalog.namespaceExists(TBL.namespace()))
        .as("close() on a private instance must clear its store entries")
        .isFalse();
    assertThat(privateCatalog.tableExists(TBL))
        .as("close() on a private instance must clear its table entries")
        .isFalse();
  }

  @Test
  public void closeOnSharedInstancePreservesStore() throws IOException {
    Map<String, String> sharedProps =
        ImmutableMap.of(
            InMemoryCatalog.SHARED_STORE_ID, reservedSharedStoreId("shared-survives-close"));

    InMemoryCatalog firstShared = new InMemoryCatalog();
    firstShared.initialize("first", sharedProps);
    firstShared.createNamespace(TBL.namespace());
    firstShared.buildTable(TBL, SCHEMA).create();
    firstShared.close();

    InMemoryCatalog secondShared = newTrackedCatalog("second", sharedProps);
    assertThat(secondShared.tableExists(TBL))
        .as("Closing one shared instance must not drop the shared store")
        .isTrue();
  }

  @Test
  public void clearSharedStoreRemovesStateForFutureInstances() throws IOException {
    String storeId = reservedSharedStoreId("clear-store");
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

    InMemoryCatalog firstCatalog = new InMemoryCatalog();
    firstCatalog.initialize("first", sharedProps);
    firstCatalog.createNamespace(TBL.namespace());
    firstCatalog.buildTable(TBL, SCHEMA).create();
    firstCatalog.close();

    InMemoryCatalog.clearSharedStore(storeId);

    InMemoryCatalog secondCatalog = newTrackedCatalog("second", sharedProps);
    assertThat(secondCatalog.namespaceExists(TBL.namespace()))
        .as("Shared store should be empty for new instances after clearSharedStore")
        .isFalse();
    assertThat(secondCatalog.tableExists(TBL))
        .as("Shared store should drop tables for new instances after clearSharedStore")
        .isFalse();
  }

  @Test
  public void clearSharedStoreLeavesLiveInstancesPointingAtTheirStore() {
    String storeId = reservedSharedStoreId("clear-while-live");
    Map<String, String> sharedProps = ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId);

    InMemoryCatalog live = newTrackedCatalog("live", sharedProps);
    live.createNamespace(TBL.namespace());
    live.buildTable(TBL, SCHEMA).create();

    InMemoryCatalog.clearSharedStore(storeId);

    assertThat(live.tableExists(TBL))
        .as("Live instances retain a reference to the prior store after clearSharedStore")
        .isTrue();

    InMemoryCatalog freshlyOpened = newTrackedCatalog("fresh", sharedProps);
    assertThat(freshlyOpened.tableExists(TBL))
        .as("Newly initialized instances see a fresh store after clearSharedStore")
        .isFalse();
  }

  @Test
  public void clearSharedStoreIsNoOpForUnknownId() {
    assertThatCode(() -> InMemoryCatalog.clearSharedStore("never-registered"))
        .as("Clearing an unknown shared-store ID should be a safe no-op")
        .doesNotThrowAnyException();
  }

  @Test
  public void emptyStringSharedStoreIdIsRejected() {
    assertThatThrownBy(
            () -> newTrackedCatalog("empty", ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, "")))
        .as("Empty-string shared-store ID is rejected to prevent silent shared-mode misconfig")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(InMemoryCatalog.SHARED_STORE_ID);
  }

  @Test
  public void whitespaceSharedStoreIdIsRejected() {
    assertThatThrownBy(
            () ->
                newTrackedCatalog(
                    "whitespace", ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, "   ")))
        .as("Whitespace-only shared-store ID is rejected to prevent silent shared-mode misconfig")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(InMemoryCatalog.SHARED_STORE_ID);
  }

  @Test
  public void reinitializeWithoutSharedStoreIdSwitchesToPrivateStore() {
    String storeId = reservedSharedStoreId("reinit-to-private");
    InMemoryCatalog reusable =
        newTrackedCatalog("reusable", ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, storeId));
    reusable.createNamespace(TBL.namespace());
    reusable.buildTable(TBL, SCHEMA).create();

    reusable.initialize("reusable", ImmutableMap.of());

    assertThat(reusable.namespaceExists(TBL.namespace()))
        .as("Reinitializing without a shared-store ID must switch to a fresh private store")
        .isFalse();
    assertThat(reusable.tableExists(TBL))
        .as("Reinitializing without a shared-store ID must not see the previously shared table")
        .isFalse();
  }

  @Test
  public void reinitializeAcrossSharedStoreIdsRetargetsBackingStore() {
    String first = reservedSharedStoreId("reinit-source");
    String second = reservedSharedStoreId("reinit-target");

    InMemoryCatalog reusable =
        newTrackedCatalog("reusable", ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, first));
    reusable.createNamespace(TBL.namespace());
    reusable.buildTable(TBL, SCHEMA).create();

    reusable.initialize("reusable", ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, second));

    assertThat(reusable.tableExists(TBL))
        .as("Reinitializing onto a different shared-store ID must not carry state across")
        .isFalse();

    InMemoryCatalog observerOnFirst =
        newTrackedCatalog(
            "observer-first", ImmutableMap.of(InMemoryCatalog.SHARED_STORE_ID, first));
    assertThat(observerOnFirst.tableExists(TBL))
        .as("The original shared store must still hold state after the source instance moves away")
        .isTrue();
  }

  @Test
  public void concurrentInitializeWithSameSharedStoreIdConvergesOnOneStore()
      throws InterruptedException, ExecutionException {
    String storeId = reservedSharedStoreId("concurrent-init");
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
            .as("All instances racing to initialize on the same shared-store ID share state")
            .isTrue();
      }
      threw = false;
    } catch (TimeoutException e) {
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
    }
  }

  @Test
  public void concurrentCreateOfSameTableThroughSharedStoreSerializes()
      throws InterruptedException, ExecutionException {
    String storeId = reservedSharedStoreId("concurrent-create");
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
      AtomicInteger unexpected = new AtomicInteger();
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
                  } catch (InterruptedException expected) {
                    Thread.currentThread().interrupt();
                  } catch (RuntimeException unexpectedError) {
                    unexpected.incrementAndGet();
                    throw unexpectedError;
                  }
                  return null;
                }));
      }

      start.countDown();
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }

      assertThat(unexpected.get())
          .as("No racer should fail with anything other than AlreadyExistsException")
          .isEqualTo(0);
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
    } catch (TimeoutException e) {
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
    }
  }
}
