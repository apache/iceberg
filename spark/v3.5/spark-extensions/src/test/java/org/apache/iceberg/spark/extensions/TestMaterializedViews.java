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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.MaterializedViewUtil;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestMaterializedViews extends SparkExtensionsTestBase {
  private static final Namespace NAMESPACE = Namespace.of("default");
  private final String tableName = "table";
  private final String materializedViewName = "materialized_view";

  @Before
  public void before() {
    spark.conf().set("spark.sql.defaultCatalog", catalogName);
    sql("USE %s", catalogName);
    sql("CREATE NAMESPACE IF NOT EXISTS %s", NAMESPACE);
    sql("CREATE TABLE %s (id INT, data STRING)", tableName);
  }

  @After
  public void removeTable() {
    sql("USE %s", catalogName);
    sql("DROP VIEW IF EXISTS %s", materializedViewName);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK_WITH_VIEWS.catalogName(),
        SparkCatalogConfig.SPARK_WITH_VIEWS.implementation(),
        SparkCatalogConfig.SPARK_WITH_VIEWS.properties()
      }
    };
  }

  public TestMaterializedViews(
      String catalog, String implementation, Map<String, String> properties) {
    super(catalog, implementation, properties);
  }

  @Test
  public void assertReadFromStorageTableWhenFresh() throws IOException {
    File location = Files.createTempDirectory("materialized-view-test").toFile();
    sql("DROP VIEW IF EXISTS %s", materializedViewName);
    sql(
        "CREATE MATERIALIZED VIEW %s TBLPROPERTIES ('location' = '%s') AS SELECT id, data FROM %s",
        materializedViewName, location.getAbsolutePath(), tableName);

    // Assert that number of records in the materialized view is the same as the number of records
    // in the table
    assertThat(sql("SELECT * FROM %s", materializedViewName).size())
        .isEqualTo(sql("SELECT * FROM %s", tableName).size());

    // Assert that the catalog loadView method returns NoSuchViewException because the view is fresh
    assertThatThrownBy(
            () ->
                sparkViewCatalog()
                    .loadView(Identifier.of(new String[] {"default"}, materializedViewName)))
        .isInstanceOf(NoSuchViewException.class);

    // Assert that the catalog loadTable method returns the materialized view storage table
    try {
      assertThat(
              sparkTableCatalog()
                  .loadTable(Identifier.of(new String[] {"default"}, materializedViewName))
                  .name())
          .isEqualTo(
              icebergViewCatalog()
                  .loadView(TableIdentifier.of("default", materializedViewName))
                  .properties()
                  .get(MaterializedViewUtil.MATERIALIZED_VIEW_STORAGE_LOCATION_PROPERTY_KEY));
    } catch (NoSuchTableException e) {
      fail("Materialized view storage table not found");
    }
  }

  @Test
  public void assertNotReadFromStorageTableWhenStale() throws IOException {
    File location = Files.createTempDirectory("materialized-view-test").toFile();
    sql(
        "CREATE MATERIALIZED VIEW %s TBLPROPERTIES ('location' = '%s') AS SELECT id, data FROM %s",
        materializedViewName, location.getAbsolutePath(), tableName);

    // Insert one row to the table so the materialized view becomes stale
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);

    // Assert that number of records in the materialized view is the same as the number of records
    // in the table
    assertThat(sql("SELECT * FROM %s", materializedViewName).size())
        .isEqualTo(sql("SELECT * FROM %s", tableName).size());

    // Assert that the catalog loadView method returns the view object
    try {
      assertThat(
              sparkViewCatalog()
                  .loadView(Identifier.of(new String[] {"default"}, materializedViewName))
                  .name())
          .isEqualTo(
              icebergViewCatalog()
                  .loadView(TableIdentifier.of("default", materializedViewName))
                  .name());
    } catch (NoSuchViewException e) {
      fail("Materialized view not found");
    }

    // Assert that the catalog loadTable fails with NoSuchTableException because the view is stale
    assertThatThrownBy(
            () ->
                sparkTableCatalog()
                    .loadTable(Identifier.of(new String[] {"default"}, materializedViewName)))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  public void assertShowTablesDoesNotShowStorageTable() throws IOException {
    File location = Files.createTempDirectory("materialized-view-test").toFile();
    sql(
        "CREATE MATERIALIZED VIEW %s TBLPROPERTIES ('location' = '%s') AS SELECT id, data FROM %s",
        materializedViewName, location.getAbsolutePath(), tableName);

    // Assert that the storage table is not shown in the list of tables
    assertThat(sql("SHOW TABLES").size() == 2);
  }

  private ViewCatalog sparkViewCatalog() {
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog(catalogName);
    return (ViewCatalog) catalogPlugin;
  }

  private TableCatalog sparkTableCatalog() {
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog(catalogName);
    return (TableCatalog) catalogPlugin;
  }

  private org.apache.iceberg.catalog.ViewCatalog icebergViewCatalog() {
    Catalog icebergCatalog = Spark3Util.loadIcebergCatalog(spark, catalogName);
    assertThat(icebergCatalog).isInstanceOf(org.apache.iceberg.catalog.ViewCatalog.class);
    return (org.apache.iceberg.catalog.ViewCatalog) icebergCatalog;
  }

  // TODO Add DROP MATERIALIZED VIEW test
  // TODO Assert materialized view creation fails when the location is not provided
  // TODO Test cannot replace a materialized view with a new version
}
