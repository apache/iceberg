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
package org.apache.iceberg.spark;

import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_REST;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.RCKUtils;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.util.PropertyUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestBaseWithCatalog extends TestBase {
  protected static File warehouse = null;
  protected static RESTCatalogServer restServer;
  protected static RESTCatalog restCatalog;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      },
    };
  }

  @BeforeAll
  public static void setUpAll() throws IOException {
    TestBaseWithCatalog.warehouse = File.createTempFile("warehouse", null);
    assertThat(warehouse.delete()).isTrue();
    startRESTServer();
  }

  @AfterAll
  public static void tearDownAll() throws IOException {
    if (warehouse != null && warehouse.exists()) {
      Path warehousePath = new Path(warehouse.getAbsolutePath());
      FileSystem fs = warehousePath.getFileSystem(hiveConf);
      assertThat(fs.delete(warehousePath, true)).as("Failed to delete " + warehousePath).isTrue();
    }
    stopRESTServer();
  }

  private static void startRESTServer() {
    try {
      restServer = new RESTCatalogServer();
      // prevent using already-in-use port when testing
      System.setProperty("rest.port", String.valueOf(MetaStoreUtils.findFreePort()));
      System.setProperty(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath());
      // In-memory sqlite database by default is private to the connection that created it.
      // If more than 1 jdbc connection backed by in-memory sqlite is created behind one
      // JdbcCatalog, then different jdbc connections could provide different views of table
      // status even belonging to the same catalog. Reference:
      // https://www.sqlite.org/inmemorydb.html
      System.setProperty(CatalogProperties.CLIENT_POOL_SIZE, "1");
      restServer.start(false);
      restCatalog = RCKUtils.initCatalogClient();
      System.clearProperty("rest.port");
      System.clearProperty(CatalogProperties.WAREHOUSE_LOCATION);
      System.clearProperty(CatalogProperties.CLIENT_POOL_SIZE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void stopRESTServer() {
    try {
      if (restCatalog != null) {
        restCatalog.close();
      }
      if (restServer != null) {
        restServer.stop();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @TempDir protected java.nio.file.Path temp;

  @Parameter(index = 0)
  protected String catalogName;

  @Parameter(index = 1)
  protected String implementation;

  @Parameter(index = 2)
  protected Map<String, String> catalogConfig;

  protected Catalog validationCatalog;
  protected SupportsNamespaces validationNamespaceCatalog;
  protected TableIdentifier tableIdent = TableIdentifier.of(Namespace.of("default"), "table");
  protected String tableName;

  private void configureValidationCatalog() {
    if (catalogConfig.containsKey(ICEBERG_CATALOG_TYPE)) {
      switch (catalogConfig.get(ICEBERG_CATALOG_TYPE)) {
        case ICEBERG_CATALOG_TYPE_HADOOP:
          this.validationCatalog =
              new HadoopCatalog(spark.sessionState().newHadoopConf(), "file:" + warehouse);
          break;
        case ICEBERG_CATALOG_TYPE_REST:
          this.validationCatalog = restCatalog;
          break;
        case ICEBERG_CATALOG_TYPE_HIVE:
          this.validationCatalog = catalog;
          break;
        default:
          throw new IllegalArgumentException("Unknown catalog type");
      }
    } else if (catalogConfig.containsKey(CATALOG_IMPL)) {
      switch (catalogConfig.get(CATALOG_IMPL)) {
        case "org.apache.iceberg.inmemory.InMemoryCatalog":
          this.validationCatalog = new InMemoryCatalog();
          break;
        default:
          throw new IllegalArgumentException("Unknown catalog impl");
      }
    }
    this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;
  }

  @BeforeEach
  public void before() {
    configureValidationCatalog();

    spark.conf().set("spark.sql.catalog." + catalogName, implementation);
    catalogConfig.forEach(
        (key, value) -> spark.conf().set("spark.sql.catalog." + catalogName + "." + key, value));

    if ("hadoop".equalsIgnoreCase(catalogConfig.get("type"))) {
      spark.conf().set("spark.sql.catalog." + catalogName + ".warehouse", "file:" + warehouse);
    }

    if (!catalogName.equals("spark_catalog")) {
      spark.conf().set("spark.sql.default.catalog", catalogName);
    }

    this.tableName =
        (catalogName.equals("spark_catalog") ? "" : catalogName + ".") + "default.table";

    sql("CREATE NAMESPACE IF NOT EXISTS default");
  }

  protected String tableName(String name) {
    return (catalogName.equals("spark_catalog") ? "" : catalogName + ".") + "default." + name;
  }

  protected String commitTarget() {
    return tableName;
  }

  protected String selectTarget() {
    return tableName;
  }

  protected boolean cachingCatalogEnabled() {
    return PropertyUtil.propertyAsBoolean(
        catalogConfig, CatalogProperties.CACHE_ENABLED, CatalogProperties.CACHE_ENABLED_DEFAULT);
  }

  protected void configurePlanningMode(PlanningMode planningMode) {
    configurePlanningMode(tableName, planningMode);
  }

  protected void configurePlanningMode(String table, PlanningMode planningMode) {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s', '%s' '%s')",
        table,
        TableProperties.DATA_PLANNING_MODE,
        planningMode.modeName(),
        TableProperties.DELETE_PLANNING_MODE,
        planningMode.modeName());
  }
}
