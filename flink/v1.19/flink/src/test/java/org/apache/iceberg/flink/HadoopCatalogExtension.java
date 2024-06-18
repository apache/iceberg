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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class HadoopCatalogExtension
    implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback, AfterEachCallback {
  protected final String database;
  protected final String tableName;

  protected Path temporaryFolder;
  protected Catalog catalog;
  protected CatalogLoader catalogLoader;
  protected String warehouse;
  protected TableLoader tableLoader;

  public HadoopCatalogExtension(String database, String tableName) {
    this.database = database;
    this.tableName = tableName;
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    this.temporaryFolder = Files.createTempDirectory("junit5_hadoop_catalog-");
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    FileUtils.deleteDirectory(temporaryFolder.toFile());
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    assertThat(temporaryFolder).exists().isDirectory();
    this.warehouse = "file:" + temporaryFolder + "/" + UUID.randomUUID();
    this.catalogLoader =
        CatalogLoader.hadoop(
            "hadoop",
            new Configuration(),
            ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse));
    this.catalog = catalogLoader.loadCatalog();
    this.tableLoader =
        TableLoader.fromCatalog(catalogLoader, TableIdentifier.of(database, tableName));
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    try {
      catalog.dropTable(TableIdentifier.of(database, tableName));
      ((HadoopCatalog) catalog).close();
      tableLoader.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close catalog resource");
    }
  }

  public TableLoader tableLoader() {
    return tableLoader;
  }

  public Catalog catalog() {
    return catalog;
  }

  public CatalogLoader catalogLoader() {
    return catalogLoader;
  }

  public String warehouse() {
    return warehouse;
  }
}
