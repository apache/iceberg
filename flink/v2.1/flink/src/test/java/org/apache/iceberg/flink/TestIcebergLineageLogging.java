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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader.CatalogTableLoader;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ResourceLock(Resources.SYSTEM_ERR)
class TestIcebergLineageLogging {
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "table");
  private static final String FULL_TABLE_NAME = "catalog." + IDENTIFIER;

  @Test
  void datasetFailureDoesNotLogCredentials() {
    CatalogTableLoader loader = mock(CatalogTableLoader.class);
    when(loader.tableIdentifier()).thenThrow(credentialFailure());

    String log =
        captureLogs(
            () -> assertThat(IcebergLineageUtil.datasetsOf(loader, FULL_TABLE_NAME)).isEmpty());

    assertSafeLog(log, "Could not resolve Iceberg lineage for " + FULL_TABLE_NAME);
  }

  @Test
  void prefixCaptureFailureDoesNotLogCredentials() throws IOException {
    RESTCatalog catalog = mock(RESTCatalog.class);
    when(catalog.properties()).thenThrow(credentialFailure());
    CatalogLoader catalogLoader = mock(CatalogLoader.class);
    when(catalogLoader.loadCatalog()).thenReturn(catalog);

    try (TableLoader loader = TableLoader.fromCatalog(catalogLoader, IDENTIFIER)) {
      loader.open();
      String log = captureLogs(() -> assertThat(IcebergLineageUtil.restPrefixOf(loader)).isNull());

      assertSafeLog(log, "Could not read the REST catalog prefix from the open catalog");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void prefixFallbackFailureDoesNotLogCredentials(boolean failReadingProperties) {
    CatalogLoader catalogLoader = restCatalogLoader();
    if (failReadingProperties) {
      when(catalogLoader.properties()).thenThrow(credentialFailure());
    } else {
      when(catalogLoader.loadCatalog()).thenThrow(credentialFailure());
    }
    TableLoader loader = TableLoader.fromCatalog(catalogLoader, IDENTIFIER);

    String log =
        captureLogs(
            () -> {
              List<LineageDataset> datasets =
                  IcebergLineageUtil.datasetsOf(loader, FULL_TABLE_NAME);
              assertThat(datasets).hasSize(1);
              DatasetConfigFacet facet =
                  (DatasetConfigFacet) datasets.get(0).facets().get(IcebergLineageUtil.FACET_NAME);
              assertThat(facet.config())
                  .containsEntry(IcebergLineageUtil.CONFIG_NAMESPACE, "db")
                  .containsEntry(IcebergLineageUtil.CONFIG_TABLE, "table")
                  .doesNotContainKey(IcebergLineageUtil.CONFIG_CATALOG_PREFIX);
            });

    assertSafeLog(log, "Could not resolve the REST catalog prefix; reporting lineage without it");
  }

  @Test
  void catalogCloseFailureDoesNotLogCredentials() throws IOException {
    RESTCatalog catalog = mock(RESTCatalog.class);
    when(catalog.properties()).thenReturn(Map.of("prefix", "native-prefix"));
    doThrow(credentialFailure()).when(catalog).close();
    CatalogLoader catalogLoader = restCatalogLoader();
    when(catalogLoader.loadCatalog()).thenReturn(catalog);
    TableLoader loader = TableLoader.fromCatalog(catalogLoader, IDENTIFIER);

    String log =
        captureLogs(
            () -> {
              List<LineageDataset> datasets =
                  IcebergLineageUtil.datasetsOf(loader, FULL_TABLE_NAME);
              assertThat(datasets).hasSize(1);
              DatasetConfigFacet facet =
                  (DatasetConfigFacet) datasets.get(0).facets().get(IcebergLineageUtil.FACET_NAME);
              assertThat(facet.config())
                  .containsEntry(IcebergLineageUtil.CONFIG_CATALOG_PREFIX, "native-prefix");
            });

    verify(catalog).close();
    assertSafeLog(log, "Failed to close the catalog opened to resolve lineage");
  }

  private static CatalogLoader restCatalogLoader() {
    CatalogLoader loader = mock(CatalogLoader.class);
    when(loader.properties())
        .thenReturn(
            Map.of(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST));
    when(loader.clone()).thenReturn(loader);
    return loader;
  }

  private static IllegalStateException credentialFailure() {
    IllegalStateException failure =
        new IllegalStateException(
            "https://catalog.example?token=message-canary",
            new IllegalArgumentException("jdbc:postgresql://host/db?password=cause-canary"));
    failure.addSuppressed(new IOException("https://user:suppressed-canary@catalog.example"));
    return failure;
  }

  private static String captureLogs(Runnable action) {
    PrintStream original = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    try (PrintStream stream = new PrintStream(captured, true, StandardCharsets.UTF_8)) {
      System.setErr(stream);
      action.run();
    } finally {
      System.setErr(original);
    }

    return captured.toString(StandardCharsets.UTF_8);
  }

  private static void assertSafeLog(String log, String diagnostic) {
    assertThat(log)
        .contains(diagnostic, "IllegalStateException")
        .doesNotContain("message-canary", "cause-canary", "suppressed-canary");
  }
}
