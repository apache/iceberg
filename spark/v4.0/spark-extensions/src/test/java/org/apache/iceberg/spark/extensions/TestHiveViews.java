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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestHiveViews extends ExtensionsTestBase {
  private static final Namespace NAMESPACE = Namespace.of("default");
  private static final String SPARK_CATALOG = "spark_catalog";
  private final String tableName = "table";

  @BeforeEach
  @Override
  public void before() {
    super.before();
    spark.conf().set("spark.sql.defaultCatalog", catalogName);
    sql("USE %s", catalogName);
    sql("CREATE NAMESPACE IF NOT EXISTS %s", NAMESPACE);
    sql(
        "CREATE TABLE IF NOT EXISTS %s.%s (id INT, data STRING)%s",
        NAMESPACE, tableName, catalogName.equals(SPARK_CATALOG) ? " USING iceberg" : "");
    sql("USE %s.%s", catalogName, NAMESPACE);
  }

  @AfterEach
  public void removeTable() {
    sql("USE %s", catalogName);
    sql("DROP TABLE IF EXISTS %s.%s", NAMESPACE, tableName);

    // reset spark session catalog
    spark.sessionState().catalogManager().reset();
    spark.conf().unset("spark.sql.catalog.spark_catalog");
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        ImmutableMap.builder()
            .putAll(SparkCatalogConfig.HIVE.properties())
            .put(CatalogProperties.CACHE_ENABLED, "false")
            .build()
      }
    };
  }

  @TestTemplate
  public void describeExtendedHiveCatalogView() {
    String viewName = viewName("describeExtendedView");
    String sql = String.format("SELECT id, data FROM %s WHERE id <= 3", tableName);

    sql(
        "CREATE VIEW %s (new_id COMMENT 'ID', new_data COMMENT 'DATA') COMMENT 'view comment' TBLPROPERTIES('hive.metastore.table.owner' = 'view_owner') AS %s",
        viewName, sql);
    String location = viewCatalog().loadView(TableIdentifier.of(NAMESPACE, viewName)).location();
    assertThat(sql("DESCRIBE EXTENDED %s", viewName))
        .contains(
            row("new_id", "int", "ID"),
            row("new_data", "string", "DATA"),
            row("", "", ""),
            row("# Detailed View Information", "", ""),
            row("Comment", "view comment", ""),
            row("Owner", "view_owner", ""),
            row("View Catalog and Namespace", String.format("%s.%s", catalogName, NAMESPACE), ""),
            row("View Query Output Columns", "[id, data]", ""),
            row(
                "View Properties",
                String.format(
                    "['format-version' = '1', 'location' = '%s', 'provider' = 'iceberg']",
                    location),
                ""));
  }

  @TestTemplate
  public void createAndDescribeHiveCatalogViewInDefaultNamespace() {
    String viewName = viewName("createViewInDefaultNamespace");
    String sql = String.format("SELECT id, data FROM %s WHERE id <= 3", tableName);

    sql(
        "CREATE VIEW %s (id, data) TBLPROPERTIES('hive.metastore.table.owner' = 'view_owner') AS %s",
        viewName, sql);
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, viewName);
    View view = viewCatalog().loadView(identifier);
    assertThat(view.currentVersion().defaultCatalog()).isNull();
    assertThat(view.name()).isEqualTo(ViewUtil.fullViewName(catalogName, identifier));
    assertThat(view.currentVersion().defaultNamespace()).isEqualTo(NAMESPACE);

    String location = viewCatalog().loadView(identifier).location();
    assertThat(sql("DESCRIBE EXTENDED %s.%s", NAMESPACE, viewName))
        .contains(
            row("id", "int", ""),
            row("data", "string", ""),
            row("", "", ""),
            row("# Detailed View Information", "", ""),
            row("Comment", "", ""),
            row("Owner", "view_owner", ""),
            row("View Catalog and Namespace", String.format("%s.%s", catalogName, NAMESPACE), ""),
            row("View Query Output Columns", "[id, data]", ""),
            row(
                "View Properties",
                String.format(
                    "['format-version' = '1', 'location' = '%s', 'provider' = 'iceberg']",
                    location),
                ""));
  }

  @TestTemplate
  public void createAndDescribeViewHiveCatalogWithoutCurrentNamespace() {
    String viewName = viewName("createViewWithoutCurrentNamespace");
    Namespace namespace = Namespace.of("test_namespace");
    String sql = String.format("SELECT id, data FROM %s WHERE id <= 3", tableName);

    sql("CREATE NAMESPACE IF NOT EXISTS %s", namespace);
    sql(
        "CREATE VIEW %s.%s (id, data) TBLPROPERTIES('hive.metastore.table.owner' = 'view_owner') AS %s",
        namespace, viewName, sql);
    TableIdentifier identifier = TableIdentifier.of(namespace, viewName);
    View view = viewCatalog().loadView(identifier);
    assertThat(view.currentVersion().defaultCatalog()).isNull();
    assertThat(view.name()).isEqualTo(ViewUtil.fullViewName(catalogName, identifier));
    assertThat(view.currentVersion().defaultNamespace()).isEqualTo(NAMESPACE);

    String location = viewCatalog().loadView(identifier).location();
    assertThat(sql("DESCRIBE EXTENDED %s.%s", namespace, viewName))
        .contains(
            row("id", "int", ""),
            row("data", "string", ""),
            row("", "", ""),
            row("# Detailed View Information", "", ""),
            row("Comment", "", ""),
            row("Owner", "view_owner", ""),
            row("View Catalog and Namespace", String.format("%s.%s", catalogName, namespace), ""),
            row("View Query Output Columns", "[id, data]", ""),
            row(
                "View Properties",
                String.format(
                    "['format-version' = '1', 'location' = '%s', 'provider' = 'iceberg']",
                    location),
                ""));
  }

  private String viewName(String viewName) {
    return viewName + new Random().nextInt(1000000);
  }

  protected ViewCatalog viewCatalog() {
    Catalog icebergCatalog = Spark3Util.loadIcebergCatalog(spark, catalogName);
    assertThat(icebergCatalog).isInstanceOf(ViewCatalog.class);
    return (ViewCatalog) icebergCatalog;
  }
}
