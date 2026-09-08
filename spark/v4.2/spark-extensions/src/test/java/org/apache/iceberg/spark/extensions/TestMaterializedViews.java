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
import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.MaterializedViewUtil;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkMaterializedView;
import org.apache.iceberg.view.RefreshState;
import org.apache.iceberg.view.RefreshStateParser;
import org.apache.iceberg.view.SourceTableState;
import org.apache.iceberg.view.SourceViewState;
import org.apache.iceberg.view.View;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.RelationCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMaterializedViews extends ExtensionsTestBase {
  private static final String QUERY_COLUMN_NAMES = "spark.query-column-names";

  private static final Namespace NAMESPACE = Namespace.of("default");
  private final String tableName = "table";
  private final String materializedViewName = "materialized_view";

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    Map<String, String> properties =
        Maps.newHashMap(SparkCatalogConfig.SPARK_WITH_MATERIALIZED_VIEWS.properties());
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "file:" + getTempWarehouseDir());
    properties.put(CatalogProperties.CATALOG_IMPL, InMemoryCatalogWithLocalFileIO.class.getName());
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK_WITH_MATERIALIZED_VIEWS.catalogName(),
        SparkCatalogConfig.SPARK_WITH_MATERIALIZED_VIEWS.implementation(),
        properties
      }
    };
  }

  private static String getTempWarehouseDir() {
    try {
      File tempDir = Files.createTempDirectory("warehouse-").toFile();
      tempDir.deleteOnExit();
      return tempDir.getAbsolutePath();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
  @Override
  public void before() {
    // Set up a simple InMemoryCatalog as validation catalog to avoid base class
    // configureValidationCatalog() failing on our custom catalog-impl.
    this.validationCatalog = new InMemoryCatalog();
    this.validationNamespaceCatalog =
        (org.apache.iceberg.catalog.SupportsNamespaces) validationCatalog;

    spark.conf().set("spark.sql.catalog." + catalogName, implementation);
    catalogConfig.forEach(
        (key, value) -> spark.conf().set("spark.sql.catalog." + catalogName + "." + key, value));

    sql("CREATE NAMESPACE IF NOT EXISTS default");
    spark.conf().set("spark.sql.defaultCatalog", catalogName);
    sql("USE %s", catalogName);
    sql("CREATE NAMESPACE IF NOT EXISTS %s", NAMESPACE);
    sql("CREATE TABLE %s (id INT, data STRING)", tableName);
  }

  @AfterEach
  public void removeTable() {
    sql("USE %s", catalogName);
    sql("DROP VIEW IF EXISTS %s", materializedViewName);
    sql("DROP VIEW IF EXISTS %s", "source_view");
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testStorageTableFieldOnViewVersion() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    View view = loadIcebergView();
    // storage-table should be set on the view version, not as a property
    assertThat(view.currentVersion().storageTable()).isNotNull();
    assertThat(view.currentVersion().storageTable().name())
        .isEqualTo(materializedViewName + "__storage");
    assertThat(view.currentVersion().storageTable().namespace()).isEqualTo(NAMESPACE);
  }

  @TestTemplate
  public void testCreateOrReplaceViewOverMaterializedViewIsRejected() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CREATE OR REPLACE VIEW %s AS SELECT id FROM %s",
                    materializedViewName, tableName))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Cannot replace materialized view")
        .hasMessageContaining("drop the materialized view and create it again");

    // The materialized view is untouched: it still references its storage table.
    assertThat(loadIcebergView().currentVersion().storageTable()).isNotNull();
  }

  @TestTemplate
  public void testQueryColumnNamesUseTheViewPropertyKeys() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    Map<String, String> props = loadIcebergView().properties();
    assertThat(props).doesNotContainKey("queryColumnNames");
    assertThat(props).containsKey("spark.query-column-names-json");
    assertThat(props).containsEntry("spark.query-column-names", "id,data");
  }

  @TestTemplate
  public void testRefreshFailsWhenQueryNoLongerProducesBoundColumns() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    sql(
        "CREATE MATERIALIZED VIEW %s (first, second) AS SELECT * FROM %s",
        materializedViewName, tableName);

    // Renaming a source column leaves "first" paired with a name the query no longer produces.
    // Reading a plain view in this state reports an incompatible schema change, so the refresh
    // reports the columns it cannot read rather than filling them from the query's output order.
    sql("ALTER TABLE %s RENAME COLUMN id TO ident", tableName);

    assertThatThrownBy(() -> sql("REFRESH MATERIALIZED VIEW %s", materializedViewName))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("query does not produce column(s) [id] that the view reads");
  }

  @TestTemplate
  public void testRefreshReadsQueryColumnsByName() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    sql(
        "CREATE MATERIALIZED VIEW %s (first, second) AS SELECT * FROM %s",
        materializedViewName, tableName);

    // Reordering the source columns changes the order of the query's output, but each view
    // column takes its values from the query column it was paired with, so "first" still reads id.
    sql("ALTER TABLE %s ALTER COLUMN data FIRST", tableName);
    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);

    assertThat(sql("SELECT first FROM %s", materializedViewName))
        .containsExactlyInAnyOrder(row(1), row(2), row(3));
  }

  @TestTemplate
  public void testRefreshUsesViewColumnNamesWhenQueryColumnNamesAreNotRecorded() {
    sql("DROP TABLE IF EXISTS source_table");
    sql("CREATE TABLE source_table (x STRING, y STRING)");
    sql("INSERT INTO source_table VALUES ('x1', 'y1'), ('x2', 'y2')");

    // A materialized view created outside Spark records no query column names, because that is a
    // Spark property. Such a view is read by resolving its own column names against the query.
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.optional(
                1, "x", org.apache.iceberg.types.Types.StringType.get()),
            org.apache.iceberg.types.Types.NestedField.optional(
                2, "y", org.apache.iceberg.types.Types.StringType.get()));
    sql("CREATE TABLE external_mv__storage (x STRING, y STRING)");
    sparkCatalog()
        .icebergViewCatalog()
        .buildView(TableIdentifier.of(NAMESPACE, "external_mv"))
        .withQuery("spark", "SELECT * FROM source_table")
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .withStorageTableIdentifier(TableIdentifier.of(NAMESPACE, "external_mv__storage"))
        .create();

    assertThat(loadIcebergView("external_mv").properties())
        .doesNotContainKey("spark.query-column-names");

    sql("REFRESH MATERIALIZED VIEW external_mv");
    assertThat(sql("SELECT x, y FROM external_mv ORDER BY x"))
        .containsExactly(row("x1", "y1"), row("x2", "y2"));

    // Reordering the source columns must not change which column each view column reads, so the
    // materialized view keeps agreeing with the view that it materializes.
    sql("ALTER TABLE source_table ALTER COLUMN y FIRST");
    sql("REFRESH MATERIALIZED VIEW external_mv");
    assertThat(sql("SELECT x, y FROM external_mv ORDER BY x"))
        .containsExactly(row("x1", "y1"), row("x2", "y2"));

    sql("DROP TABLE IF EXISTS source_table");
  }

  private void assertMaterializedViewMatchesView(
      String materializedView,
      String view,
      String firstColumn,
      String secondColumn,
      boolean expectedFresh,
      Object[]... expectedRows) {
    boolean fresh;
    try {
      fresh =
          sparkTableCatalog()
                  .loadTable(Identifier.of(new String[] {NAMESPACE.toString()}, materializedView))
              instanceof SparkMaterializedView;
    } catch (NoSuchTableException e) {
      fresh = false;
    }

    assertThat(fresh)
        .as("%s should be %s", materializedView, expectedFresh ? "fresh" : "stale")
        .isEqualTo(expectedFresh);

    String query = "SELECT %s, %s FROM %s ORDER BY %s";
    assertThat(sql(query, firstColumn, secondColumn, materializedView, firstColumn))
        .as("%s should read the columns its own schema names", materializedView)
        .containsExactly(expectedRows);
    assertThat(sql(query, firstColumn, secondColumn, view, firstColumn))
        .as("%s should agree with the view that %s materializes", view, materializedView)
        .containsExactly(expectedRows);
  }

  /**
   * A materialized view must return what the view it materializes returns, whether it is read from
   * its storage table or from its query, and whether or not Spark recorded the query's column
   * names. Reordering the source table's columns must not change any of that.
   */
  @TestTemplate
  public void testMaterializedViewMatchesViewAcrossFreshnessAndSourceReorder() {
    sql("DROP TABLE IF EXISTS src");
    sql("CREATE TABLE src (x STRING, y STRING)");
    sql("INSERT INTO src VALUES ('x1', 'y1')");

    // Created through Spark, so the query's column names are recorded. The aliases differ from
    // the query's column names, so a refresh that ignored the recorded names would be visible.
    sql("CREATE VIEW v_named (a, b) AS SELECT * FROM src");
    sql("CREATE MATERIALIZED VIEW mv_named (a, b) AS SELECT * FROM src");

    // Created outside Spark, so no query column names are recorded.
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.optional(
                1, "x", org.apache.iceberg.types.Types.StringType.get()),
            org.apache.iceberg.types.Types.NestedField.optional(
                2, "y", org.apache.iceberg.types.Types.StringType.get()));
    sql("CREATE TABLE mv_unnamed__storage (x STRING, y STRING)");
    sparkCatalog()
        .icebergViewCatalog()
        .buildView(TableIdentifier.of(NAMESPACE, "mv_unnamed"))
        .withQuery("spark", "SELECT * FROM src")
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .withStorageTableIdentifier(TableIdentifier.of(NAMESPACE, "mv_unnamed__storage"))
        .create();
    sparkCatalog()
        .icebergViewCatalog()
        .buildView(TableIdentifier.of(NAMESPACE, "v_unnamed"))
        .withQuery("spark", "SELECT * FROM src")
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();

    assertThat(loadIcebergView("mv_named").properties()).containsEntry(QUERY_COLUMN_NAMES, "x,y");
    assertThat(loadIcebergView("mv_unnamed").properties()).doesNotContainKey(QUERY_COLUMN_NAMES);

    sql("REFRESH MATERIALIZED VIEW mv_named");
    sql("REFRESH MATERIALIZED VIEW mv_unnamed");
    assertMaterializedViewMatchesView("mv_named", "v_named", "a", "b", true, row("x1", "y1"));
    assertMaterializedViewMatchesView("mv_unnamed", "v_unnamed", "x", "y", true, row("x1", "y1"));

    // Writing to the source makes both materialized views stale, so they are read from their
    // queries rather than from their storage tables.
    sql("INSERT INTO src VALUES ('x2', 'y2')");
    assertMaterializedViewMatchesView(
        "mv_named", "v_named", "a", "b", false, row("x1", "y1"), row("x2", "y2"));
    assertMaterializedViewMatchesView(
        "mv_unnamed", "v_unnamed", "x", "y", false, row("x1", "y1"), row("x2", "y2"));

    // Reordering the source's columns does not write a snapshot, so both materialized views stay
    // fresh and keep serving the rows their storage tables already hold.
    sql("REFRESH MATERIALIZED VIEW mv_named");
    sql("REFRESH MATERIALIZED VIEW mv_unnamed");
    sql("ALTER TABLE src ALTER COLUMN y FIRST");
    assertMaterializedViewMatchesView(
        "mv_named", "v_named", "a", "b", true, row("x1", "y1"), row("x2", "y2"));
    assertMaterializedViewMatchesView(
        "mv_unnamed", "v_unnamed", "x", "y", true, row("x1", "y1"), row("x2", "y2"));

    sql("INSERT INTO src (x, y) VALUES ('x3', 'y3')");
    assertMaterializedViewMatchesView(
        "mv_named", "v_named", "a", "b", false, row("x1", "y1"), row("x2", "y2"), row("x3", "y3"));
    assertMaterializedViewMatchesView(
        "mv_unnamed",
        "v_unnamed",
        "x",
        "y",
        false,
        row("x1", "y1"),
        row("x2", "y2"),
        row("x3", "y3"));

    // Refreshing after the reorder looks the recorded names up in the query's output again, now
    // that the query's output order no longer matches the order the columns were paired in.
    sql("REFRESH MATERIALIZED VIEW mv_named");
    sql("REFRESH MATERIALIZED VIEW mv_unnamed");
    assertMaterializedViewMatchesView(
        "mv_named", "v_named", "a", "b", true, row("x1", "y1"), row("x2", "y2"), row("x3", "y3"));
    assertMaterializedViewMatchesView(
        "mv_unnamed",
        "v_unnamed",
        "x",
        "y",
        true,
        row("x1", "y1"),
        row("x2", "y2"),
        row("x3", "y3"));

    sql("DROP VIEW IF EXISTS mv_named");
    sql("DROP VIEW IF EXISTS mv_unnamed");
    sql("DROP VIEW IF EXISTS v_named");
    sql("DROP VIEW IF EXISTS v_unnamed");
    sql("DROP TABLE IF EXISTS src");
  }

  @TestTemplate
  public void testStaleReadReadsQueryColumnsByName() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    sql(
        "CREATE MATERIALIZED VIEW %s (first, second) AS SELECT * FROM %s",
        materializedViewName, tableName);

    // Reordering the source columns changes the order of the query's output. A stale materialized
    // view is read by running that query, and each of its columns takes its values from the query
    // column it was paired with, so "first" still reads id.
    sql("ALTER TABLE %s ALTER COLUMN data FIRST", tableName);

    assertThat(sql("SELECT first, second FROM %s ORDER BY first", materializedViewName))
        .containsExactly(row(1, "a"), row(2, "b"), row(3, "c"));
  }

  @TestTemplate
  public void testColumnAliasesAreRespectedWhenStaleAndWhenFresh() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    sql(
        "CREATE MATERIALIZED VIEW %s (first, second) AS SELECT id, data FROM %s",
        materializedViewName, tableName);
    String storageTableName = materializedViewName + "__storage";

    // A stale materialized view is read through its definition, so the query runs and its columns
    // are named by the aliases.
    assertThat(spark.table(materializedViewName).schema().fieldNames())
        .containsExactly("first", "second");
    assertThat(sql("SELECT first, second FROM %s ORDER BY first", materializedViewName))
        .containsExactly(row(1, "a"), row(2, "b"), row(3, "c"));
    assertThat(analyzedPlan("SELECT first FROM " + materializedViewName))
        .contains("default." + tableName)
        .doesNotContain(storageTableName);

    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);

    // A fresh materialized view is read from its storage table instead of by running the query,
    // so its plan is a relation scan that does not reference the source table. The columns keep
    // the same names and values, so which of the two answers the query is not observable.
    assertThat(spark.table(materializedViewName).schema().fieldNames())
        .containsExactly("first", "second");
    assertThat(sql("SELECT first, second FROM %s ORDER BY first", materializedViewName))
        .containsExactly(row(1, "a"), row(2, "b"), row(3, "c"));
    assertThat(analyzedPlan("SELECT first FROM " + materializedViewName))
        .contains("RelationV2")
        .doesNotContain("default." + tableName);

    // The storage table names its columns the same way when it is read on its own.
    assertThat(sql("SELECT first, second FROM %s ORDER BY first", storageTableName))
        .containsExactly(row(1, "a"), row(2, "b"), row(3, "c"));
  }

  private String analyzedPlan(String query) {
    return spark.sql(query).queryExecution().analyzed().treeString();
  }

  @TestTemplate
  public void testColumnAliasesNameTheViewColumns() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    sql(
        "CREATE MATERIALIZED VIEW %s (first, second COMMENT 'second column')"
            + " AS SELECT id, data FROM %s",
        materializedViewName, tableName);

    View view = loadIcebergView();
    assertThat(view.schema().columns())
        .map(org.apache.iceberg.types.Types.NestedField::name)
        .containsExactly("first", "second");
    assertThat(view.schema().findField("second").doc()).isEqualTo("second column");

    // The aliases name the view; the query keeps its own output column names.
    assertThat(view.properties()).containsEntry("spark.query-column-names", "id,data");

    // The storage table materializes the view's columns, so it carries the aliases too.
    org.apache.iceberg.Table storageTable =
        sparkCatalog().icebergCatalog().loadTable(view.currentVersion().storageTable());
    assertThat(storageTable.schema().columns())
        .map(org.apache.iceberg.types.Types.NestedField::name)
        .containsExactly("first", "second");

    // Refreshing writes the query, whose columns are named id and data, into a storage table
    // whose columns are named by the aliases.
    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);
    assertThat(sql("SELECT first, second FROM %s ORDER BY first", materializedViewName))
        .containsExactly(row(1, "a"), row(2, "b"), row(3, "c"));
  }

  @TestTemplate
  public void testCreateOrReplaceIsRejected() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CREATE OR REPLACE MATERIALIZED VIEW %s AS SELECT id FROM %s",
                    materializedViewName, tableName))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Cannot replace materialized view")
        .hasMessageContaining("Drop the materialized view and create it again");
  }

  @TestTemplate
  public void testNeverRefreshedMvIsNotFresh() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    // A newly created MV has no snapshots on its storage table, so it's not fresh.
    // loadView should succeed (returns stale view)
    try {
      assertThat(sparkViewCatalog().loadView(viewIdentifier()))
          .isInstanceOf(org.apache.spark.sql.connector.catalog.View.class);
    } catch (NoSuchViewException e) {
      fail("Materialized view not found");
    }
  }

  @TestTemplate
  public void testReadFromStorageTableWhenFresh() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    simulateRefresh();

    // Fresh MV: loadTable should return SparkMaterializedView
    try {
      assertThat(sparkTableCatalog().loadTable(viewIdentifier()))
          .isInstanceOf(SparkMaterializedView.class);
    } catch (NoSuchTableException e) {
      fail("Fresh materialized view should be loadable as a table");
    }

    // Fresh MV: loadRelation routes to the storage table rather than the view definition
    try {
      assertThat(sparkRelationCatalog().loadRelation(viewIdentifier()))
          .isInstanceOf(SparkMaterializedView.class);
    } catch (NoSuchTableException e) {
      fail("Fresh materialized view should be resolvable as a relation");
    }

    // Fresh MV: loadView still returns the view definition instead of signalling via an exception
    try {
      assertThat(sparkViewCatalog().loadView(viewIdentifier()))
          .isInstanceOf(org.apache.spark.sql.connector.catalog.View.class);
    } catch (NoSuchViewException e) {
      fail("Materialized view not found");
    }
  }

  @TestTemplate
  public void testFallbackToViewWhenStale() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    simulateRefresh();

    // Insert more data to invalidate the refresh
    sql("INSERT INTO %s VALUES (3, 'c')", tableName);

    // Stale MV: loadView should return a plain Spark view (falls back to query execution)
    try {
      assertThat(sparkViewCatalog().loadView(viewIdentifier()))
          .isInstanceOf(org.apache.spark.sql.connector.catalog.View.class);
    } catch (NoSuchViewException e) {
      fail("Stale materialized view should be loadable as a view");
    }

    // Stale MV: loadRelation routes to the view definition, not the storage table
    try {
      assertThat(sparkRelationCatalog().loadRelation(viewIdentifier()))
          .isNotInstanceOf(SparkMaterializedView.class);
    } catch (NoSuchTableException e) {
      fail("Stale materialized view should be resolvable as a relation");
    }

    // Stale MV: loadTable should not resolve to the MV's storage table
    assertThatThrownBy(() -> sparkTableCatalog().loadTable(viewIdentifier()))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining(materializedViewName);
  }

  @TestTemplate
  public void testStorageTableCreatedBeforeMvMetadata() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    // The storage table should exist
    String storageTableName =
        MaterializedViewUtil.getDefaultMaterializedViewStorageTableIdentifier(
                Identifier.of(new String[] {NAMESPACE.toString()}, materializedViewName))
            .name();
    assertThat(sql("SHOW TABLES"))
        .anySatisfy(row -> assertThat(row[1]).isEqualTo(storageTableName));
  }

  @TestTemplate
  public void testDefaultStorageTableNaming() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    // Default naming should be <name>__storage
    String expectedStorageTableName = materializedViewName + "__storage";
    assertThat(sql("SHOW TABLES"))
        .anySatisfy(row -> assertThat(row[1]).isEqualTo(expectedStorageTableName));
  }

  @TestTemplate
  public void testStoredAsClause() {
    String customTableName = "custom_table_name";
    sql(
        "CREATE MATERIALIZED VIEW %s STORED AS '%s' AS SELECT id, data FROM %s",
        materializedViewName, customTableName, tableName);

    // Assert that the storage table with the custom name is in the list of tables
    assertThat(sql("SHOW TABLES")).anySatisfy(row -> assertThat(row[1]).isEqualTo(customTableName));
  }

  @TestTemplate
  public void testRefreshMaterializedView() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    // Refresh the materialized view
    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);

    // After refresh, the MV should be fresh and loadable as a table
    try {
      assertThat(sparkTableCatalog().loadTable(viewIdentifier()))
          .isInstanceOf(SparkMaterializedView.class);
    } catch (NoSuchTableException e) {
      fail("Refreshed materialized view should be loadable as a table");
    }

    // Verify the storage table has data
    View view = loadIcebergView();
    String storageTableRef =
        String.format(
            "%s.%s.%s", catalogName, NAMESPACE, view.currentVersion().storageTable().name());
    assertThat(sql("SELECT * FROM %s", storageTableRef)).hasSize(2);
  }

  @TestTemplate
  public void testRefreshMaterializedViewUpdatesData() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    // First refresh
    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);

    // Insert more data
    sql("INSERT INTO %s VALUES (3, 'c')", tableName);

    // Before second refresh, the MV should be stale
    try {
      assertThat(sparkViewCatalog().loadView(viewIdentifier()))
          .isInstanceOf(org.apache.spark.sql.connector.catalog.View.class);
    } catch (NoSuchViewException e) {
      fail("Stale materialized view should be loadable as a view");
    }

    // Second refresh
    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);

    // After refresh, the MV should be fresh again
    try {
      assertThat(sparkTableCatalog().loadTable(viewIdentifier()))
          .isInstanceOf(SparkMaterializedView.class);
    } catch (NoSuchTableException e) {
      fail("Refreshed materialized view should be loadable as a table");
    }

    // Verify the storage table has all 3 rows
    View view = loadIcebergView();
    String storageTableRef =
        String.format(
            "%s.%s.%s", catalogName, NAMESPACE, view.currentVersion().storageTable().name());
    assertThat(sql("SELECT * FROM %s", storageTableRef)).hasSize(3);
  }

  @TestTemplate
  public void testRefreshRecordsNestedViewState() {
    String sourceViewName = "source_view";
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", sourceViewName, tableName);
    sql(
        "CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s",
        materializedViewName, sourceViewName);

    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);

    View sourceView = loadIcebergView(sourceViewName);
    RefreshState refreshState = loadRefreshState();

    // The refresh state should record both the nested source view and the base table it
    // resolves to, since the analyzed query plan fully expands the view chain.
    assertThat(refreshState.sourceStates()).hasSize(2);

    SourceViewState viewState =
        refreshState.sourceStates().stream()
            .filter(SourceViewState.class::isInstance)
            .map(SourceViewState.class::cast)
            .findFirst()
            .orElseGet(() -> fail("Refresh state should record the nested source view"));
    assertThat(viewState.name()).isEqualTo(sourceViewName);
    assertThat(viewState.namespace()).isEqualTo(Arrays.asList(NAMESPACE.levels()));
    assertThat(viewState.uuid()).isEqualTo(sourceView.uuid().toString());
    assertThat(viewState.versionId()).isEqualTo(sourceView.currentVersion().versionId());

    SourceTableState tableState =
        refreshState.sourceStates().stream()
            .filter(SourceTableState.class::isInstance)
            .map(SourceTableState.class::cast)
            .findFirst()
            .orElseGet(() -> fail("Refresh state should record the underlying base table"));
    assertThat(tableState.name()).isEqualTo(tableName);

    sql("DROP VIEW IF EXISTS %s", sourceViewName);
  }

  @TestTemplate
  public void testStaleWhenNestedViewChanges() {
    String sourceViewName = "source_view";
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    sql("CREATE VIEW %s AS SELECT id, data FROM %s WHERE id <= 2", sourceViewName, tableName);
    sql(
        "CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s",
        materializedViewName, sourceViewName);

    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);

    // Freshly refreshed: loadable as a table
    try {
      assertThat(sparkTableCatalog().loadTable(viewIdentifier()))
          .isInstanceOf(SparkMaterializedView.class);
    } catch (NoSuchTableException e) {
      fail("Refreshed materialized view should be loadable as a table");
    }

    // Replace the nested view's definition without touching the base table. This bumps the
    // source view's version but leaves the underlying base table's snapshot unchanged.
    sql(
        "CREATE OR REPLACE VIEW %s AS SELECT id, data FROM %s WHERE id <= 1",
        sourceViewName, tableName);

    // The MV should now be stale because its nested source view changed versions, even
    // though the underlying base table's snapshot did not change.
    try {
      assertThat(sparkViewCatalog().loadView(viewIdentifier()))
          .isInstanceOf(org.apache.spark.sql.connector.catalog.View.class);
    } catch (NoSuchViewException e) {
      fail("Materialized view with a stale nested view should be loadable as a view");
    }
    assertThatThrownBy(() -> sparkTableCatalog().loadTable(viewIdentifier()))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining(materializedViewName);

    sql("DROP VIEW IF EXISTS %s", sourceViewName);
  }

  @TestTemplate
  public void testStaleWhenSourceViewIsRecreated() {
    String sourceViewName = "source_view";
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
    sql("CREATE VIEW %s AS SELECT id, data FROM %s WHERE id <= 2", sourceViewName, tableName);
    sql(
        "CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s",
        materializedViewName, sourceViewName);

    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);
    int refreshedVersionId = loadIcebergView(sourceViewName).currentVersion().versionId();

    // Drop and recreate the source view with an unrelated definition. Version ids restart at 1,
    // so the recreated view reports the same version that the refresh recorded and only the
    // view's UUID identifies it as a different view.
    sql("DROP VIEW %s", sourceViewName);
    sql("CREATE VIEW %s AS SELECT id, data FROM %s WHERE id > 2", sourceViewName, tableName);
    assertThat(loadIcebergView(sourceViewName).currentVersion().versionId())
        .isEqualTo(refreshedVersionId);

    assertThatThrownBy(() -> sparkTableCatalog().loadTable(viewIdentifier()))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining(materializedViewName);

    sql("DROP VIEW IF EXISTS %s", sourceViewName);
  }

  @TestTemplate
  public void testStaleWhenEmptySourceTableIsRecreated() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);
    try {
      assertThat(sparkTableCatalog().loadTable(viewIdentifier()))
          .isInstanceOf(SparkMaterializedView.class);
    } catch (NoSuchTableException e) {
      fail("Refreshed materialized view should be loadable as a table");
    }

    // Drop and recreate the empty source table. The recorded and the current state both report
    // no snapshot, so only the table's UUID identifies it as a different table.
    sql("DROP TABLE %s", tableName);
    sql("CREATE TABLE %s (id INT, data STRING)", tableName);

    assertThatThrownBy(() -> sparkTableCatalog().loadTable(viewIdentifier()))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining(materializedViewName);
  }

  @TestTemplate
  public void testCrossCatalogSourceTable() {
    String otherCatalogName =
        "other_catalog_" + java.util.UUID.randomUUID().toString().replace("-", "");
    String sourceTableName = "cross_catalog_source";
    configureCatalog(otherCatalogName);

    sql("CREATE NAMESPACE IF NOT EXISTS %s.%s", otherCatalogName, NAMESPACE);
    sql(
        "CREATE TABLE %s.%s.%s (id INT, data STRING)",
        otherCatalogName, NAMESPACE, sourceTableName);
    sql(
        "INSERT INTO %s.%s.%s VALUES (1, 'a'), (2, 'b')",
        otherCatalogName, NAMESPACE, sourceTableName);

    sql(
        "CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s.%s.%s",
        materializedViewName, otherCatalogName, NAMESPACE, sourceTableName);
    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);

    // The source table lives in another catalog, so the refresh must record it with that
    // catalog name in order for freshness to be checkable at all.
    RefreshState refreshState = loadRefreshState();
    assertThat(refreshState.sourceStates()).hasSize(1);
    SourceTableState tableState = (SourceTableState) refreshState.sourceStates().get(0);
    assertThat(tableState.name()).isEqualTo(sourceTableName);
    assertThat(tableState.catalog()).isEqualTo(otherCatalogName);

    sql("DROP TABLE IF EXISTS %s.%s.%s", otherCatalogName, NAMESPACE, sourceTableName);
  }

  @TestTemplate
  public void testCrossCatalogSourceTableChangeMakesMvStale() {
    String otherCatalogName =
        "other_catalog_" + java.util.UUID.randomUUID().toString().replace("-", "");
    String sourceTableName = "cross_catalog_source";
    configureCatalog(otherCatalogName);

    sql("CREATE NAMESPACE IF NOT EXISTS %s.%s", otherCatalogName, NAMESPACE);
    sql(
        "CREATE TABLE %s.%s.%s (id INT, data STRING)",
        otherCatalogName, NAMESPACE, sourceTableName);
    sql(
        "INSERT INTO %s.%s.%s VALUES (1, 'a'), (2, 'b')",
        otherCatalogName, NAMESPACE, sourceTableName);

    sql(
        "CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s.%s.%s",
        materializedViewName, otherCatalogName, NAMESPACE, sourceTableName);
    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);

    // Changing the cross-catalog source must make the materialized view stale.
    sql("INSERT INTO %s.%s.%s VALUES (3, 'c')", otherCatalogName, NAMESPACE, sourceTableName);

    try {
      assertThat(sparkRelationCatalog().loadRelation(viewIdentifier()))
          .isNotInstanceOf(SparkMaterializedView.class);
    } catch (NoSuchTableException e) {
      fail("Stale materialized view should be resolvable as a relation");
    }

    assertThat(sql("SELECT * FROM %s.%s.%s", catalogName, NAMESPACE, materializedViewName))
        .hasSize(3);

    sql("DROP TABLE IF EXISTS %s.%s.%s", otherCatalogName, NAMESPACE, sourceTableName);
  }

  private void configureCatalog(String name) {
    Map<String, String> properties =
        Maps.newHashMap(SparkCatalogConfig.SPARK_WITH_MATERIALIZED_VIEWS.properties());
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "file:" + getTempWarehouseDir());
    properties.put(CatalogProperties.CATALOG_IMPL, InMemoryCatalogWithLocalFileIO.class.getName());
    spark.conf().set("spark.sql.catalog." + name, implementation);
    properties.forEach(
        (key, value) -> spark.conf().set("spark.sql.catalog." + name + "." + key, value));
  }

  /**
   * Verifies that a source reached through the session catalog is tracked.
   *
   * <p>SparkSessionCatalog is a sibling of SparkCatalog rather than a subclass, so a type test
   * against SparkCatalog would skip these sources at refresh, leaving the materialized view with no
   * recorded sources and therefore permanently fresh.
   */
  @TestTemplate
  public void testSourceTableInSessionCatalog() {
    String sourceTableName = "session_catalog_source";
    configureSessionCatalog();

    sql(
        "CREATE TABLE IF NOT EXISTS spark_catalog.%s.%s (id INT, data STRING) USING iceberg",
        NAMESPACE, sourceTableName);
    sql("INSERT INTO spark_catalog.%s.%s VALUES (1, 'a'), (2, 'b')", NAMESPACE, sourceTableName);

    sql(
        "CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM spark_catalog.%s.%s",
        materializedViewName, NAMESPACE, sourceTableName);
    sql("REFRESH MATERIALIZED VIEW %s", materializedViewName);

    RefreshState refreshState = loadRefreshState();
    assertThat(refreshState.sourceStates()).hasSize(1);
    SourceTableState tableState = (SourceTableState) refreshState.sourceStates().get(0);
    assertThat(tableState.name()).isEqualTo(sourceTableName);
    assertThat(tableState.catalog()).isEqualTo("spark_catalog");

    // Changing the source in the session catalog must make the materialized view stale.
    sql("INSERT INTO spark_catalog.%s.%s VALUES (3, 'c')", NAMESPACE, sourceTableName);

    try {
      assertThat(sparkRelationCatalog().loadRelation(viewIdentifier()))
          .isNotInstanceOf(SparkMaterializedView.class);
    } catch (NoSuchTableException e) {
      fail("Stale materialized view should be resolvable as a relation");
    }

    sql("DROP TABLE IF EXISTS spark_catalog.%s.%s", NAMESPACE, sourceTableName);
  }

  private void configureSessionCatalog() {
    spark.conf().set("spark.sql.catalog.spark_catalog", SparkSessionCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
    spark.conf().set("spark.sql.catalog.spark_catalog.default-namespace", "default");
    spark.conf().set("spark.sql.catalog.spark_catalog.cache-enabled", "false");
  }

  private RefreshState loadRefreshState() {
    View view = loadIcebergView();
    org.apache.iceberg.catalog.TableIdentifier storageTableId =
        view.currentVersion().storageTable();
    org.apache.iceberg.Table storageTable =
        sparkCatalog().icebergCatalog().loadTable(storageTableId);
    String refreshStateJson =
        storageTable.currentSnapshot().summary().get(RefreshState.REFRESH_STATE_SUMMARY_KEY);
    return RefreshStateParser.fromJson(refreshStateJson);
  }

  private void simulateRefresh() {
    View view = loadIcebergView();
    org.apache.iceberg.catalog.TableIdentifier storageTableId =
        view.currentVersion().storageTable();

    org.apache.iceberg.Table baseTable =
        sparkCatalog().icebergCatalog().loadTable(TableIdentifier.of(NAMESPACE, tableName));

    // Get the base table's current snapshot ID
    long baseSnapshotId =
        (Long)
            sql(
                    "SELECT snapshot_id FROM %s.%s.%s.snapshots ORDER BY committed_at DESC LIMIT 1",
                    catalogName, NAMESPACE, tableName)
                .get(0)[0];

    // Build refresh state matching the current view version and source table state
    RefreshState refreshState =
        new RefreshState(
            view.currentVersion().versionId(),
            Arrays.<org.apache.iceberg.view.SourceState>asList(
                new SourceTableState(
                    tableName,
                    Arrays.asList(NAMESPACE.levels()),
                    null,
                    baseTable.uuid().toString(),
                    baseSnapshotId,
                    null)),
            System.currentTimeMillis());
    String refreshStateJson = RefreshStateParser.toJson(refreshState);

    // Write data to storage table with refresh-state in the snapshot summary
    String storageTableRef =
        String.format("%s.%s.%s", catalogName, NAMESPACE, storageTableId.name());
    try {
      spark
          .sql(String.format("SELECT id, data FROM %s.%s.%s", catalogName, NAMESPACE, tableName))
          .writeTo(storageTableRef)
          .option("snapshot-property." + RefreshState.REFRESH_STATE_SUMMARY_KEY, refreshStateJson)
          .append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException("Storage table not found during simulated refresh", e);
    }
  }

  private ViewCatalog sparkViewCatalog() {
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog(catalogName);
    return (ViewCatalog) catalogPlugin;
  }

  private TableCatalog sparkTableCatalog() {
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog(catalogName);
    return (TableCatalog) catalogPlugin;
  }

  private RelationCatalog sparkRelationCatalog() {
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog(catalogName);
    return (RelationCatalog) catalogPlugin;
  }

  private Identifier viewIdentifier() {
    return Identifier.of(new String[] {NAMESPACE.toString()}, materializedViewName);
  }

  private SparkCatalog sparkCatalog() {
    return (SparkCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  private View loadIcebergView() {
    return loadIcebergView(materializedViewName);
  }

  private View loadIcebergView(String viewName) {
    org.apache.iceberg.catalog.ViewCatalog icebergViewCatalog = sparkCatalog().icebergViewCatalog();
    return icebergViewCatalog.loadView(TableIdentifier.of(NAMESPACE, viewName));
  }

  // Required to be public since it is loaded by org.apache.iceberg.CatalogUtil.loadCatalog
  public static class InMemoryCatalogWithLocalFileIO extends InMemoryCatalog {
    private FileIO localFileIO;

    @Override
    public void initialize(String name, Map<String, String> properties) {
      super.initialize(name, properties);
      localFileIO = new LocalFileIO();
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return new InMemoryTableOperations(localFileIO, tableIdentifier);
    }

    @Override
    protected InMemoryCatalog.InMemoryViewOperations newViewOps(TableIdentifier identifier) {
      return new InMemoryViewOperations(localFileIO, identifier);
    }
  }

  private static class LocalFileIO implements FileIO {

    private static String stripFilePrefix(String path) {
      return path.startsWith("file:") ? path.substring(5) : path;
    }

    @Override
    public InputFile newInputFile(String path) {
      return org.apache.iceberg.Files.localInput(stripFilePrefix(path));
    }

    @Override
    public OutputFile newOutputFile(String path) {
      String stripped = stripFilePrefix(path);
      java.io.File parent = new java.io.File(stripped).getParentFile();
      if (!parent.isDirectory()) {
        parent.mkdirs();
      }
      return org.apache.iceberg.Files.localOutput(stripped);
    }

    @Override
    public void deleteFile(String path) {
      if (!new File(stripFilePrefix(path)).delete()) {
        throw new RuntimeIOException("Failed to delete file: " + path);
      }
    }
  }
}
