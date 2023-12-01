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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class HadoopCatResource implements BeforeEachCallback, AfterEachCallback {
  protected final Path temporaryFolder;
  protected final String database;
  protected final String tableName;

  protected Catalog catalog;
  protected CatalogLoader catalogLoader;
  protected String warehouse;
  protected TableLoader tableLoader;

  public HadoopCatResource(Path temporaryFolder, String database, String tableName) {
    this.temporaryFolder = temporaryFolder;
    this.database = database;
    this.tableName = tableName;
  }

  @Override
  public void beforeEach(ExtensionContext context) throws IOException {
    File warehouseFile = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouseFile.delete());
    // before variables
    this.warehouse = "file:" + warehouseFile;
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
  public void afterEach(ExtensionContext context) {
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
