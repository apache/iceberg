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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.rest.RESTServerRule;
import org.apache.iceberg.util.PropertyUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class SparkTestBaseWithCatalog extends SparkTestBase {
  protected static File warehouse = null;

  @ClassRule
  public static final RESTServerRule REST_SERVER_RULE =
      new RESTServerRule(
          Map.of(
              RESTCatalogServer.REST_PORT,
              RESTServerRule.FREE_PORT,
              // In-memory sqlite database by default is private to the connection that created it.
              // If more than 1 jdbc connection backed by in-memory sqlite is created behind one
              // JdbcCatalog, then different jdbc connections could provide different views of table
              // status even belonging to the same catalog. Reference:
              // https://www.sqlite.org/inmemorydb.html
              CatalogProperties.CLIENT_POOL_SIZE,
              "1"));

  protected static RESTCatalog restCatalog;

  @BeforeClass
  public static void createWarehouse() throws IOException {
    SparkTestBaseWithCatalog.warehouse = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouse.delete());
    restCatalog = REST_SERVER_RULE.client();
  }

  @AfterClass
  public static void dropWarehouse() throws IOException {
    if (warehouse != null && warehouse.exists()) {
      Path warehousePath = new Path(warehouse.getAbsolutePath());
      FileSystem fs = warehousePath.getFileSystem(hiveConf);
      Assert.assertTrue("Failed to delete " + warehousePath, fs.delete(warehousePath, true));
    }
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  protected final String catalogName;
  protected final Map<String, String> catalogConfig;
  protected Catalog validationCatalog;
  protected SupportsNamespaces validationNamespaceCatalog;
  protected final TableIdentifier tableIdent = TableIdentifier.of(Namespace.of("default"), "table");
  protected final String tableName;

  public SparkTestBaseWithCatalog() {
    this(SparkCatalogConfig.HADOOP);
  }

  public SparkTestBaseWithCatalog(SparkCatalogConfig config) {
    this(config.catalogName(), config.implementation(), config.properties());
  }

  public SparkTestBaseWithCatalog(
      String catalogName, String implementation, Map<String, String> config) {
    this.catalogName = catalogName;
    this.catalogConfig = config;
    configureValidationCatalog();

    spark.conf().set("spark.sql.catalog." + catalogName, implementation);
    config.forEach(
        (key, value) -> spark.conf().set("spark.sql.catalog." + catalogName + "." + key, value));

    if ("hadoop".equalsIgnoreCase(config.get("type"))) {
      spark.conf().set("spark.sql.catalog." + catalogName + ".warehouse", "file:" + warehouse);
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
}
