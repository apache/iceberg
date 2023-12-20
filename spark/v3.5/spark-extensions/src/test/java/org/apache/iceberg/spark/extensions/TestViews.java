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
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestViews extends SparkExtensionsTestBase {
  private static final Namespace NAMESPACE = Namespace.of("default");
  private final String tableName = "table";

  @Before
  public void before() {
    spark.conf().set("spark.sql.defaultCatalog", catalogName);
    sql("CREATE NAMESPACE IF NOT EXISTS %s", NAMESPACE);
    sql("CREATE TABLE %s (id INT, data STRING)", tableName);
  }

  @After
  public void removeTable() {
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

  public TestViews(String catalog, String implementation, Map<String, String> properties) {
    super(catalog, implementation, properties);
  }

  @Test
  public void readFromView() throws NoSuchTableException {
    insertRows(10);
    String viewName = "simpleView";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", String.format("SELECT id FROM %s", tableName))
        // use non-existing column name to make sure only the SQL definition for spark is loaded
        .withQuery("trino", String.format("SELECT non_existing FROM %s", tableName))
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    List<Object[]> expected =
        IntStream.rangeClosed(1, 10).mapToObj(this::row).collect(Collectors.toList());

    assertThat(sql("SELECT * FROM %s", viewName))
        .hasSize(10)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void readFromTrinoView() throws NoSuchTableException {
    insertRows(10);
    String viewName = "trinoView";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("trino", String.format("SELECT id FROM %s", tableName))
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    List<Object[]> expected =
        IntStream.rangeClosed(1, 10).mapToObj(this::row).collect(Collectors.toList());

    // there's no explicit view defined for spark, so it will fall back to the defined trino view
    assertThat(sql("SELECT * FROM %s", viewName))
        .hasSize(10)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void readFromMultipleViews() throws NoSuchTableException {
    insertRows(6);
    String viewName = "firstView";
    String secondView = "secondView";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", String.format("SELECT id FROM %s WHERE id <= 3", tableName))
        .withDefaultNamespace(NAMESPACE)
        .withSchema(schema)
        .create();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, secondView))
        .withQuery("spark", String.format("SELECT id FROM %s WHERE id > 3", tableName))
        .withDefaultNamespace(NAMESPACE)
        .withSchema(schema)
        .create();

    assertThat(sql("SELECT * FROM %s", viewName))
        .hasSize(3)
        .containsExactlyInAnyOrder(row(1), row(2), row(3));

    assertThat(sql("SELECT * FROM %s", secondView))
        .hasSize(3)
        .containsExactlyInAnyOrder(row(4), row(5), row(6));
  }

  @Test
  public void readFromViewUsingNonExistingTable() throws NoSuchTableException {
    insertRows(10);
    String viewName = "viewWithNonExistingTable";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", "SELECT id FROM non_existing")
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    assertThatThrownBy(() -> sql("SELECT * FROM %s", viewName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            String.format(
                "The table or view `%s`.`%s`.`non_existing` cannot be found",
                catalogName, NAMESPACE));
  }

  @Test
  public void readFromViewUsingNonExistingColumn() throws NoSuchTableException {
    insertRows(10);
    String viewName = "viewWithNonExistingColumn";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", String.format("SELECT non_existing FROM %s", tableName))
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    assertThatThrownBy(() -> sql("SELECT * FROM %s", viewName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "A column or function parameter with name `non_existing` cannot be resolved");
  }

  @Test
  public void readFromViewHiddenByTempView() throws NoSuchTableException {
    insertRows(10);
    String viewName = "viewHiddenByTempView";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    sql("CREATE TEMPORARY VIEW %s AS SELECT id FROM %s WHERE id <= 5", viewName, tableName);

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", String.format("SELECT id FROM %s WHERE id > 5", tableName))
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    List<Object[]> expected =
        IntStream.rangeClosed(1, 5).mapToObj(this::row).collect(Collectors.toList());

    assertThat(sql("SELECT * FROM %s", viewName))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void readFromViewHiddenByGlobalTempView() throws NoSuchTableException {
    insertRows(10);
    String viewName = "viewHiddenByGlobalTempView";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    sql("CREATE GLOBAL TEMPORARY VIEW %s AS SELECT id FROM %s WHERE id <= 5", viewName, tableName);

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", String.format("SELECT id FROM %s WHERE id > 5", tableName))
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    List<Object[]> expected =
        IntStream.rangeClosed(1, 5).mapToObj(this::row).collect(Collectors.toList());

    // FIXME: this should return the ids from the global temp view
    assertThat(sql("SELECT * FROM %s", viewName))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void readFromViewReferencingAnotherView() throws NoSuchTableException {
    insertRows(10);
    String firstView = "viewBeingReferencedInAnotherView";
    String viewReferencingOtherView = "viewReferencingOtherView";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, firstView))
        .withQuery("spark", String.format("SELECT id FROM %s", tableName))
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewReferencingOtherView))
        .withQuery("spark", String.format("SELECT id FROM %s", firstView))
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    List<Object[]> expected =
        IntStream.rangeClosed(1, 10).mapToObj(this::row).collect(Collectors.toList());

    assertThat(sql("SELECT * FROM %s", viewReferencingOtherView))
        .hasSize(10)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void readFromViewReferencingTempView() throws NoSuchTableException {
    insertRows(10);
    String tempView = "tempViewBeingReferencedInAnotherView";
    String viewReferencingTempView = "viewReferencingTempView";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    sql("CREATE TEMPORARY VIEW %s AS SELECT id FROM %s WHERE id <= 5", tempView, tableName);

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewReferencingTempView))
        .withQuery("spark", String.format("SELECT id FROM %s", tempView))
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    List<Object[]> expected =
        IntStream.rangeClosed(1, 5).mapToObj(this::row).collect(Collectors.toList());

    assertThat(sql("SELECT * FROM %s", tempView))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(expected);

    // FIXME: this should work
    assertThat(sql("SELECT * FROM %s", viewReferencingTempView))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void readFromViewWithCTE() throws NoSuchTableException {
    insertRows(10);
    String viewName = "viewWithCTE";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", String.format("SELECT * FROM (SELECT max(id) FROM %s)", tableName))
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    assertThat(sql("SELECT * FROM %s", viewName)).hasSize(1).containsExactly(row(10));
  }

  private ViewCatalog viewCatalog() {
    Catalog icebergCatalog = Spark3Util.loadIcebergCatalog(spark, catalogName);
    assertThat(icebergCatalog).isInstanceOf(ViewCatalog.class);
    return (ViewCatalog) icebergCatalog;
  }

  private Catalog tableCatalog() {
    return Spark3Util.loadIcebergCatalog(spark, catalogName);
  }

  private void insertRows(int numRows) throws NoSuchTableException {
    List<SimpleRecord> records = Lists.newArrayListWithCapacity(numRows);
    for (int i = 1; i <= numRows; i++) {
      records.add(new SimpleRecord(i, UUID.randomUUID().toString()));
    }

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
  }
}
