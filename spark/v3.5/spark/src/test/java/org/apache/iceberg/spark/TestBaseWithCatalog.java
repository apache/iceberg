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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.iceberg.util.PropertyUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestBaseWithCatalog extends TestBase {
  protected static File warehouse = null;

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
  public static void createWarehouse() throws IOException {
    TestBaseWithCatalog.warehouse = File.createTempFile("warehouse", null);
    Assertions.assertThat(warehouse.delete()).isTrue();
  }

  @AfterAll
  public static void dropWarehouse() throws IOException {
    if (warehouse != null && warehouse.exists()) {
      Path warehousePath = new Path(warehouse.getAbsolutePath());
      FileSystem fs = warehousePath.getFileSystem(hiveConf);
      Assertions.assertThat(fs.delete(warehousePath, true))
          .as("Failed to delete " + warehousePath)
          .isTrue();
    }
  }

  @TempDir protected File temp;

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

  @BeforeEach
  public void before() {
    this.validationCatalog =
        catalogName.equals("testhadoop")
            ? new HadoopCatalog(spark.sessionState().newHadoopConf(), "file:" + warehouse)
            : catalog;
    this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;

    spark.conf().set("spark.sql.catalog." + catalogName, implementation);
    catalogConfig.forEach(
        (key, value) -> spark.conf().set("spark.sql.catalog." + catalogName + "." + key, value));

    if (catalogConfig.get("type").equalsIgnoreCase("hadoop")) {
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
}
