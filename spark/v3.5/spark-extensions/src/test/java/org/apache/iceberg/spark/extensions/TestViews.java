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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
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
    sql("USE %s", catalogName);
    sql("CREATE NAMESPACE IF NOT EXISTS %s", NAMESPACE);
    sql("CREATE TABLE %s (id INT, data STRING)", tableName);
  }

  @After
  public void removeTable() {
    sql("USE %s", catalogName);
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
    String sql = String.format("SELECT id FROM %s", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        // use non-existing column name to make sure only the SQL definition for spark is loaded
        .withQuery("trino", String.format("SELECT non_existing FROM %s", tableName))
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
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
    String sql = String.format("SELECT id FROM %s", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("trino", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
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

    sql("CREATE VIEW %s AS SELECT id FROM %s WHERE id <= 3", viewName, tableName);
    sql("CREATE VIEW %s AS SELECT id FROM %s WHERE id > 3", secondView, tableName);

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
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

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
  public void readFromViewUsingNonExistingTableColumn() throws NoSuchTableException {
    insertRows(10);
    String viewName = "viewWithNonExistingColumn";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = new Schema(Types.NestedField.required(1, "non_existing", Types.LongType.get()));

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
  public void readFromViewUsingInvalidSQL() throws NoSuchTableException {
    insertRows(10);
    String viewName = "viewWithInvalidSQL";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = tableCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName)).schema();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", "invalid SQL")
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    assertThatThrownBy(() -> sql("SELECT * FROM %s", viewName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            String.format("The view `%s` cannot be displayed due to invalid view text", viewName));
  }

  @Test
  public void readFromViewWithStaleSchema() throws NoSuchTableException {
    insertRows(10);
    String viewName = "staleView";
    String sql = String.format("SELECT id, data FROM %s", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    // drop a column the view depends on
    // note that this tests `data` because it has an invalid ordinal
    sql("ALTER TABLE %s DROP COLUMN data", tableName);

    // reading from the view should now fail
    assertThatThrownBy(() -> sql("SELECT * FROM %s", viewName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("A column or function parameter with name `data` cannot be resolved");
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

    // returns the results from the TEMP VIEW
    assertThat(sql("SELECT * FROM %s", viewName))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void readFromViewWithGlobalTempView() throws NoSuchTableException {
    insertRows(10);
    String viewName = "viewWithGlobalTempView";
    String sql = String.format("SELECT id FROM %s WHERE id > 5", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    sql("CREATE GLOBAL TEMPORARY VIEW %s AS SELECT id FROM %s WHERE id <= 5", viewName, tableName);

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    // GLOBAL TEMP VIEWS are stored in a global_temp namespace
    assertThat(sql("SELECT * FROM global_temp.%s", viewName))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.rangeClosed(1, 5).mapToObj(this::row).collect(Collectors.toList()));

    assertThat(sql("SELECT * FROM %s", viewName))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.rangeClosed(6, 10).mapToObj(this::row).collect(Collectors.toList()));
  }

  @Test
  public void readFromViewReferencingAnotherView() throws NoSuchTableException {
    insertRows(10);
    String firstView = "viewBeingReferencedInAnotherView";
    String viewReferencingOtherView = "viewReferencingOtherView";
    String firstSQL = String.format("SELECT id FROM %s WHERE id <= 5", tableName);
    String secondSQL = String.format("SELECT id FROM %s WHERE id > 4", firstView);

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, firstView))
        .withQuery("spark", firstSQL)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(firstSQL))
        .create();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewReferencingOtherView))
        .withQuery("spark", secondSQL)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(secondSQL))
        .create();

    assertThat(sql("SELECT * FROM %s", viewReferencingOtherView))
        .hasSize(1)
        .containsExactly(row(5));
  }

  @Test
  public void createViewReferencingTempView() throws NoSuchTableException {
    insertRows(10);
    String tempView = "tempViewBeingReferencedInAnotherView";
    String viewReferencingTempView = "viewReferencingTempView";
    String sql = String.format("SELECT id FROM %s", tempView);

    sql("CREATE TEMPORARY VIEW %s AS SELECT id FROM %s WHERE id <= 5", tempView, tableName);

    // creating a view that references a TEMP VIEW shouldn't be possible
    assertThatThrownBy(
            () -> sql("CREATE VIEW %s AS SELECT id FROM %s", viewReferencingTempView, tempView))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Cannot create the persistent object")
        .hasMessageContaining(viewReferencingTempView)
        .hasMessageContaining("of the type VIEW because it references to the temporary object")
        .hasMessageContaining(tempView);
  }

  @Test
  public void readFromViewReferencingAnotherViewHiddenByTempView() throws NoSuchTableException {
    insertRows(10);
    String innerViewName = "inner_view";
    String outerViewName = "outer_view";
    String innerViewSQL = String.format("SELECT * FROM %s WHERE id > 5", tableName);
    String outerViewSQL = String.format("SELECT id FROM %s", innerViewName);

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, innerViewName))
        .withQuery("spark", innerViewSQL)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(innerViewSQL))
        .create();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, outerViewName))
        .withQuery("spark", outerViewSQL)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(outerViewSQL))
        .create();

    // create a temporary view that conflicts with the inner view to verify the inner name is
    // resolved using the catalog and namespace defaults from the outer view
    sql("CREATE TEMPORARY VIEW %s AS SELECT id FROM %s WHERE id <= 5", innerViewName, tableName);

    // ensure that the inner view resolution uses the view namespace and catalog
    sql("USE spark_catalog");

    List<Object[]> tempViewRows =
        IntStream.rangeClosed(1, 5).mapToObj(this::row).collect(Collectors.toList());

    assertThat(sql("SELECT * FROM %s", innerViewName))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(tempViewRows);

    List<Object[]> expectedViewRows =
        IntStream.rangeClosed(6, 10).mapToObj(this::row).collect(Collectors.toList());

    assertThat(sql("SELECT * FROM %s.%s.%s", catalogName, NAMESPACE, outerViewName))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(expectedViewRows);
  }

  @Test
  public void createViewReferencingGlobalTempView() throws NoSuchTableException {
    insertRows(10);
    String globalTempView = "globalTempViewBeingReferenced";
    String viewReferencingTempView = "viewReferencingGlobalTempView";

    sql(
        "CREATE GLOBAL TEMPORARY VIEW %s AS SELECT id FROM %s WHERE id <= 5",
        globalTempView, tableName);

    // creating a view that references a GLOBAL TEMP VIEW shouldn't be possible
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE VIEW %s AS SELECT id FROM global_temp.%s",
                    viewReferencingTempView, globalTempView))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Cannot create the persistent object")
        .hasMessageContaining(viewReferencingTempView)
        .hasMessageContaining("of the type VIEW because it references to the temporary object")
        .hasMessageContaining(globalTempView);
  }

  @Test
  public void readFromViewWithCTE() throws NoSuchTableException {
    insertRows(10);
    String viewName = "viewWithCTE";
    String sql =
        String.format(
            "WITH max_by_data AS (SELECT max(id) as max FROM %s) "
                + "SELECT max, count(1) AS count FROM max_by_data GROUP BY max",
            tableName);

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    assertThat(sql("SELECT * FROM %s", viewName)).hasSize(1).containsExactly(row(10, 1L));
  }

  @Test
  public void rewriteFunctionIdentifier() {
    String viewName = "rewriteFunctionIdentifier";
    String sql = "SELECT iceberg_version() AS version";

    assertThatThrownBy(() -> sql(sql))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Cannot resolve function")
        .hasMessageContaining("iceberg_version");

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = new Schema(Types.NestedField.required(1, "version", Types.StringType.get()));

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(Namespace.of("system"))
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    assertThat(sql("SELECT * FROM %s", viewName))
        .hasSize(1)
        .containsExactly(row(IcebergBuild.version()));
  }

  @Test
  public void builtinFunctionIdentifierNotRewritten() {
    String viewName = "builtinFunctionIdentifierNotRewritten";
    String sql = "SELECT trim('  abc   ') AS result";

    ViewCatalog viewCatalog = viewCatalog();
    Schema schema = new Schema(Types.NestedField.required(1, "result", Types.StringType.get()));

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(Namespace.of("system"))
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    assertThat(sql("SELECT * FROM %s", viewName)).hasSize(1).containsExactly(row("abc"));
  }

  @Test
  public void rewriteFunctionIdentifierWithNamespace() {
    String viewName = "rewriteFunctionIdentifierWithNamespace";
    String sql = "SELECT system.bucket(100, 'a') AS bucket_result, 'a' AS value";

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(Namespace.of("system"))
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    sql("USE spark_catalog");

    assertThatThrownBy(() -> sql(sql))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Cannot resolve function")
        .hasMessageContaining("`system`.`bucket`");

    assertThat(sql("SELECT * FROM %s.%s.%s", catalogName, NAMESPACE, viewName))
        .hasSize(1)
        .containsExactly(row(50, "a"));
  }

  @Test
  public void fullFunctionIdentifier() {
    String viewName = "fullFunctionIdentifier";
    String sql =
        String.format(
            "SELECT %s.system.bucket(100, 'a') AS bucket_result, 'a' AS value", catalogName);

    sql("USE spark_catalog");

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(Namespace.of("system"))
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    assertThat(sql("SELECT * FROM %s.%s.%s", catalogName, NAMESPACE, viewName))
        .hasSize(1)
        .containsExactly(row(50, "a"));
  }

  @Test
  public void fullFunctionIdentifierNotRewrittenLoadFailure() {
    String viewName = "fullFunctionIdentifierNotRewrittenLoadFailure";
    String sql = "SELECT spark_catalog.system.bucket(100, 'a') AS bucket_result, 'a' AS value";

    // avoid namespace failures
    sql("USE spark_catalog");
    sql("CREATE NAMESPACE IF NOT EXISTS system");
    sql("USE %s", catalogName);

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "bucket_result", Types.IntegerType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()));

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(Namespace.of("system"))
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    // verify the v1 error message
    assertThatThrownBy(() -> sql("SELECT * FROM %s", viewName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("The function `system`.`bucket` cannot be found");
  }

  private Schema schema(String sql) {
    return SparkSchemaUtil.convert(spark.sql(sql).schema());
  }

  private ViewCatalog viewCatalog() {
    Catalog icebergCatalog = Spark3Util.loadIcebergCatalog(spark, catalogName);
    assertThat(icebergCatalog).isInstanceOf(ViewCatalog.class);
    return (ViewCatalog) icebergCatalog;
  }

  private Catalog tableCatalog() {
    return Spark3Util.loadIcebergCatalog(spark, catalogName);
  }

  @Test
  public void renameView() throws NoSuchTableException {
    insertRows(10);
    String viewName = viewName("originalView");
    String renamedView = viewName("renamedView");
    String sql = String.format("SELECT id FROM %s", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    sql("ALTER VIEW %s RENAME TO %s", viewName, renamedView);

    List<Object[]> expected =
        IntStream.rangeClosed(1, 10).mapToObj(this::row).collect(Collectors.toList());
    assertThat(sql("SELECT * FROM %s", renamedView))
        .hasSize(10)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void renameViewHiddenByTempView() throws NoSuchTableException {
    insertRows(10);
    String viewName = viewName("originalView");
    String renamedView = viewName("renamedView");
    String sql = String.format("SELECT id FROM %s WHERE id > 5", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    sql("CREATE TEMPORARY VIEW %s AS SELECT id FROM %s WHERE id <= 5", viewName, tableName);

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    // renames the TEMP VIEW
    sql("ALTER VIEW %s RENAME TO %s", viewName, renamedView);
    assertThat(sql("SELECT * FROM %s", renamedView))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.rangeClosed(1, 5).mapToObj(this::row).collect(Collectors.toList()));

    // original view still exists with its name
    assertThat(viewCatalog.viewExists(TableIdentifier.of(NAMESPACE, viewName))).isTrue();
    assertThat(viewCatalog.viewExists(TableIdentifier.of(NAMESPACE, renamedView))).isFalse();
    assertThat(sql("SELECT * FROM %s", viewName))
        .hasSize(5)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.rangeClosed(6, 10).mapToObj(this::row).collect(Collectors.toList()));

    // will rename the Iceberg view
    sql("ALTER VIEW %s RENAME TO %s", viewName, renamedView);
    assertThat(viewCatalog.viewExists(TableIdentifier.of(NAMESPACE, renamedView))).isTrue();
  }

  @Test
  public void renameViewToDifferentTargetCatalog() {
    String viewName = viewName("originalView");
    String renamedView = viewName("renamedView");
    String sql = String.format("SELECT id FROM %s", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    assertThatThrownBy(() -> sql("ALTER VIEW %s RENAME TO spark_catalog.%s", viewName, renamedView))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Cannot move view between catalogs: from=spark_with_views and to=spark_catalog");
  }

  @Test
  public void renameNonExistingView() {
    assertThatThrownBy(() -> sql("ALTER VIEW non_existing RENAME TO target"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("The table or view `non_existing` cannot be found");
  }

  @Test
  public void renameViewTargetAlreadyExistsAsView() {
    String viewName = viewName("renameViewSource");
    String target = viewName("renameViewTarget");
    String sql = String.format("SELECT id FROM %s", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, target))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    assertThatThrownBy(() -> sql("ALTER VIEW %s RENAME TO %s", viewName, target))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            String.format("Cannot create view default.%s because it already exists", target));
  }

  @Test
  public void renameViewTargetAlreadyExistsAsTable() {
    String viewName = viewName("renameViewSource");
    String target = viewName("renameViewTarget");
    String sql = String.format("SELECT id FROM %s", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    sql("CREATE TABLE %s (id INT, data STRING)", target);
    assertThatThrownBy(() -> sql("ALTER VIEW %s RENAME TO %s", viewName, target))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            String.format("Cannot create view default.%s because it already exists", target));
  }

  @Test
  public void dropView() {
    String viewName = "viewToBeDropped";
    String sql = String.format("SELECT id FROM %s", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, viewName);
    viewCatalog
        .buildView(identifier)
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    assertThat(viewCatalog.viewExists(identifier)).isTrue();

    sql("DROP VIEW %s", viewName);
    assertThat(viewCatalog.viewExists(identifier)).isFalse();
  }

  @Test
  public void dropNonExistingView() {
    assertThatThrownBy(() -> sql("DROP VIEW non_existing"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("The view %s.%s cannot be found", NAMESPACE, "non_existing");
  }

  @Test
  public void dropViewIfExists() {
    String viewName = "viewToBeDropped";
    String sql = String.format("SELECT id FROM %s", tableName);

    ViewCatalog viewCatalog = viewCatalog();

    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, viewName);
    viewCatalog
        .buildView(identifier)
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema(sql))
        .create();

    assertThat(viewCatalog.viewExists(identifier)).isTrue();

    sql("DROP VIEW IF EXISTS %s", viewName);
    assertThat(viewCatalog.viewExists(identifier)).isFalse();

    assertThatNoException().isThrownBy(() -> sql("DROP VIEW IF EXISTS %s", viewName));
  }

  /** The purpose of this test is mainly to make sure that normal view deletion isn't messed up */
  @Test
  public void dropGlobalTempView() {
    String globalTempView = "globalViewToBeDropped";
    sql("CREATE GLOBAL TEMPORARY VIEW %s AS SELECT id FROM %s", globalTempView, tableName);
    assertThat(v1SessionCatalog().getGlobalTempView(globalTempView).isDefined()).isTrue();

    sql("DROP VIEW global_temp.%s", globalTempView);
    assertThat(v1SessionCatalog().getGlobalTempView(globalTempView).isDefined()).isFalse();
  }

  /** The purpose of this test is mainly to make sure that normal view deletion isn't messed up */
  @Test
  public void dropTempView() {
    String tempView = "tempViewToBeDropped";
    sql("CREATE TEMPORARY VIEW %s AS SELECT id FROM %s", tempView, tableName);
    assertThat(v1SessionCatalog().getTempView(tempView).isDefined()).isTrue();

    sql("DROP VIEW %s", tempView);
    assertThat(v1SessionCatalog().getTempView(tempView).isDefined()).isFalse();
  }

  /** The purpose of this test is mainly to make sure that normal view deletion isn't messed up */
  @Test
  public void dropV1View() {
    String v1View = "v1ViewToBeDropped";
    sql("USE spark_catalog");
    sql("CREATE NAMESPACE IF NOT EXISTS %s", NAMESPACE);
    sql("CREATE TABLE %s (id INT, data STRING)", tableName);
    sql("CREATE VIEW %s AS SELECT id FROM %s", v1View, tableName);
    sql("USE %s", catalogName);
    assertThat(
            v1SessionCatalog()
                .tableExists(new org.apache.spark.sql.catalyst.TableIdentifier(v1View)))
        .isTrue();

    sql("DROP VIEW spark_catalog.%s.%s", NAMESPACE, v1View);
    assertThat(
            v1SessionCatalog()
                .tableExists(new org.apache.spark.sql.catalyst.TableIdentifier(v1View)))
        .isFalse();
  }

  private SessionCatalog v1SessionCatalog() {
    return spark.sessionState().catalogManager().v1SessionCatalog();
  }

  private String viewName(String viewName) {
    return viewName + new Random().nextInt(1000000);
  }

  @Test
  public void createViewIfNotExists() {
    String viewName = "viewThatAlreadyExists";
    sql("CREATE VIEW %s AS SELECT id FROM %s", viewName, tableName);

    assertThatThrownBy(() -> sql("CREATE VIEW %s AS SELECT id FROM %s", viewName, tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            String.format(
                "Cannot create view %s.%s because it already exists", NAMESPACE, viewName));

    // using IF NOT EXISTS should work
    assertThatNoException()
        .isThrownBy(
            () -> sql("CREATE VIEW IF NOT EXISTS %s AS SELECT id FROM %s", viewName, tableName));
  }

  @Test
  public void createViewUsingNonExistingTable() {
    assertThatThrownBy(() -> sql("CREATE VIEW %s AS SELECT id FROM %s", "viewName", "non_existing"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("The table or view `non_existing` cannot be found");
  }

  @Test
  public void createViewColumnMismatch() {
    String viewName = "viewWithMismatchedColumns";

    assertThatThrownBy(
            () -> sql("CREATE VIEW %s (id, data) AS SELECT id FROM %s", viewName, tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(String.format("Cannot create view `%s`.`%s`", NAMESPACE, viewName))
        .hasMessageContaining("not enough data columns")
        .hasMessageContaining("View columns: `id`, `data`")
        .hasMessageContaining("Data columns: `id`");

    assertThatThrownBy(
            () -> sql("CREATE VIEW %s (id) AS SELECT id, data FROM %s", viewName, tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(String.format("Cannot create view `%s`.`%s`", NAMESPACE, viewName))
        .hasMessageContaining("too many data columns")
        .hasMessageContaining("View columns: `id`")
        .hasMessageContaining("Data columns: `id`, `data`");
  }

  @Test
  public void createViewWithColumnAliases() throws NoSuchTableException {
    insertRows(6);
    String viewName = "viewWithColumnAliases";

    sql(
        "CREATE VIEW %s (new_id COMMENT 'ID', new_data COMMENT 'DATA') AS SELECT id, data FROM %s WHERE id <= 3",
        viewName, tableName);

    assertThat(sql("SELECT new_id FROM %s", viewName))
        .hasSize(3)
        .containsExactlyInAnyOrder(row(1), row(2), row(3));

    sql("DROP VIEW %s", viewName);

    sql(
        "CREATE VIEW %s (new_id, new_data) AS SELECT id, data FROM %s WHERE id <= 3",
        viewName, tableName);

    assertThat(sql("SELECT new_id FROM %s", viewName))
        .hasSize(3)
        .containsExactlyInAnyOrder(row(1), row(2), row(3));
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
