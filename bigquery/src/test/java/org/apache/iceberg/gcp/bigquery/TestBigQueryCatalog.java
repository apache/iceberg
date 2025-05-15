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
package org.apache.iceberg.gcp.bigquery;

import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_BIGQUERY;
import static org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog.PROJECT_ID;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestBigQueryCatalog extends CatalogTests<BigQueryMetastoreCatalog> {
  @TempDir private File tempFolder;
  private BigQueryMetastoreCatalog catalog;

  @BeforeEach
  public void before() throws Exception {
    catalog = initCatalog("catalog-name", ImmutableMap.of());
  }

  @AfterEach
  public void after() throws Exception {
    // Drop all tables in all datasets
    List<Namespace> namespaces = catalog().listNamespaces();
    for (Namespace namespace : namespaces) {
      List<TableIdentifier> tables = catalog().listTables(namespace);
      for (TableIdentifier table : tables) {
        try {
          catalog().dropTable(table, true);
        } catch (NoSuchTableException e) {
          // Table might be dropped by a previous iteration, ignore
        }
      }

      // Drop all datasets (except any initial ones)
      try {
        catalog().dropNamespace(namespace);
      } catch (NoSuchNamespaceException e) {
        // Namespace might be dropped by a previous iteration, ignore
      }
    }
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    return false;
  }

  @Override
  protected boolean supportsNamesWithDot() {
    return false;
  }

  @Override
  public BigQueryMetastoreCatalog catalog() {
    return catalog;
  }

  @Override
  protected BigQueryMetastoreCatalog initCatalog(
      String catalogName, Map<String, String> additionalProperties) {

    String warehouseLocation = tempFolder.toPath().resolve("hive-warehouse").toString();
    FakeBigQueryMetastoreClient fakeBigQueryClient = new FakeBigQueryMetastoreClient();

    Map<String, String> properties =
        Map.of(
            ICEBERG_CATALOG_TYPE,
            ICEBERG_CATALOG_TYPE_BIGQUERY,
            PROJECT_ID,
            "project-id",
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation,
            CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1",
            "catalog-default-key1",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2",
            "catalog-default-key2",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3",
            "catalog-default-key3",
            CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3",
            "catalog-override-key3",
            CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4",
            "catalog-override-key4");

    BigQueryMetastoreCatalog tmpCatalog = new BigQueryMetastoreCatalog();
    tmpCatalog.initialize(
        catalogName,
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .putAll(additionalProperties)
            .build(),
        "project-id",
        "us-central1",
        fakeBigQueryClient);

    return tmpCatalog;
  }

  // TODO: BigQuery Metastore does not support V3 Spec yet.
  @Override
  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void createTableTransaction(int formatVersion) {
    super.createTableTransaction(formatVersion);
  }

  @Disabled("BigQuery Metastore does not support V3 Spec yet.")
  @Test
  public void testCreateTableWithDefaultColumnValue() {}

  @Disabled("BigQuery Metastore does not support multi layer namespaces")
  @Test
  public void testLoadMetadataTable() {}

  @Disabled("BigQuery Metastore does not support rename tables")
  @Test
  public void testRenameTable() {
    super.testRenameTable();
  }

  @Disabled("BigQuery Metastore does not support rename tables")
  @Test
  public void testRenameTableDestinationTableAlreadyExists() {
    super.testRenameTableDestinationTableAlreadyExists();
  }

  @Disabled("BigQuery Metastore does not support rename tables")
  @Test
  public void renameTableNamespaceMissing() {
    super.renameTableNamespaceMissing();
  }

  @Disabled("BigQuery Metastore does not support rename tables")
  @Test
  public void testRenameTableMissingSourceTable() {
    super.testRenameTableMissingSourceTable();
  }
}
