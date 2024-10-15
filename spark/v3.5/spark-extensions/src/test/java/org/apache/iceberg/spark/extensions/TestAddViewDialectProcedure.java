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

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestAddViewDialectProcedure extends ExtensionsTestBase {
  private static final Namespace NAMESPACE = Namespace.of("default");
  private static final String VIEW_NAME = "view_year";
  private static final TableIdentifier VIEW_IDENTIFIER = TableIdentifier.of(NAMESPACE, VIEW_NAME);

  @BeforeEach
  public void before() {
    super.before();
    spark.conf().set("spark.sql.defaultCatalog", catalogName);
    sql("USE %s", catalogName);
    sql("CREATE NAMESPACE IF NOT EXISTS %s", NAMESPACE);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP VIEW IF EXISTS %s", VIEW_NAME);
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK_WITH_VIEWS.catalogName(),
        SparkCatalogConfig.SPARK_WITH_VIEWS.implementation(),
        SparkCatalogConfig.SPARK_WITH_VIEWS.properties()
      }
    };
  }

  private ViewCatalog viewCatalog() {
    Catalog icebergCatalog = Spark3Util.loadIcebergCatalog(spark, catalogName);
    assertThat(icebergCatalog).isInstanceOf(ViewCatalog.class);
    return (ViewCatalog) icebergCatalog;
  }

  @TestTemplate
  public void testAddNewViewDialect() {
    String sparkViewSQL =
        String.format("SELECT * FROM %s WHERE year(order_date) = year(current_date())", tableName);
    ViewRepresentation sparkRep =
        ImmutableSQLViewRepresentation.builder().dialect("spark").sql(sparkViewSQL).build();

    String trinoViewSQL =
        String.format(
            "SELECT * FROM %s WHERE extract(year FROM order_date) = extract(year FROM current_date))",
            tableName);
    ViewRepresentation trinoRep =
        ImmutableSQLViewRepresentation.builder().dialect("trino").sql(trinoViewSQL).build();

    sql("CREATE TABLE %s (order_id int NOT NULL, order_date date) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, DATE '2024-10-08')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, DATE '2025-10-08')", tableName);

    sql("CREATE VIEW %s AS %s", VIEW_NAME, sparkViewSQL);

    ViewCatalog viewCatalog = viewCatalog();
    View view = viewCatalog.loadView(VIEW_IDENTIFIER);
    int oldVersion = view.currentVersion().versionId();
    assertThat(view.currentVersion().representations()).containsExactly(sparkRep);

    List<Object[]> output =
        sql(
            "CALL %s.system.add_view_dialect('%s', '%s', '%s')",
            catalogName, VIEW_IDENTIFIER, "trino", trinoViewSQL);

    ViewVersion newVersion = viewCatalog.loadView(VIEW_IDENTIFIER).currentVersion();
    assertThat(output).containsExactly(new Object[] {newVersion.versionId()});
    assertThat(newVersion.versionId()).isNotEqualTo(oldVersion);
    assertThat(newVersion.representations()).containsExactlyInAnyOrder(sparkRep, trinoRep);

    // test adding multiple SQL for same dialect
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.add_view_dialect('%s', '%s', '%s')",
                    catalogName, VIEW_IDENTIFIER, "spark", "another sql"))
        .hasMessageContaining("Invalid view version: Cannot add multiple queries for dialect spark")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @TestTemplate
  public void testInvalidArgs() {
    String sparkViewSQL =
        String.format("SELECT * FROM %s WHERE year(order_date) = year(current_date())", tableName);

    sql("CREATE TABLE %s (order_id int NOT NULL, order_date date) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, DATE '2024-10-08')", tableName);

    sql("CREATE VIEW %s AS %s", VIEW_NAME, sparkViewSQL);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.add_view_dialect('%s', '%s', '%s')",
                    catalogName, VIEW_IDENTIFIER + "_invalid", "trino", "foo"))
        .hasMessageContaining(
            "Couldn't load view 'default.view_year_invalid' in catalog 'spark_with_views'")
        .isInstanceOf(RuntimeException.class);

    assertThatThrownBy(
            () ->
                sql("CALL %s.system.add_view_dialect('', '%s', '%s')", catalogName, "trino", "foo"))
        .hasMessageContaining("Cannot handle an empty identifier for argument view")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.add_view_dialect('%s', '', '%s')",
                    catalogName, VIEW_IDENTIFIER, "foo"))
        .hasMessageContaining("dialect should not be empty")
        .isInstanceOf(RuntimeException.class);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.add_view_dialect('%s', '%s', '')",
                    catalogName, VIEW_IDENTIFIER, "foo"))
        .hasMessageContaining("sql should not be empty")
        .isInstanceOf(RuntimeException.class);
  }
}
