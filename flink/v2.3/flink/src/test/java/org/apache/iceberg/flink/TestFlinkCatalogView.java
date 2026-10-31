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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkCatalogView extends CatalogTestBase {

  private static final String TABLE_NAME = "test_table";
  private static final String VIEW_NAME = "test_view";

  private static final Schema VIEW_SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final Schema PROJECTED_VIEW_SCHEMA =
      new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));

  @Parameters(name = "catalogType={0}, baseNamespace={1}")
  protected static List<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {CatalogType.HIVE, Namespace.empty()},
        new Object[] {CatalogType.HADOOP, Namespace.empty()},
        new Object[] {CatalogType.REST, Namespace.empty()},
        new Object[] {CatalogType.REST, Namespace.of("l0", "l1")});
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not implement ViewCatalog").isFalse();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql("CREATE TABLE %s (id BIGINT, data STRING)", TABLE_NAME);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", TABLE_NAME);
  }

  @AfterEach
  public void cleanNamespaces() {
    if (validationCatalog instanceof ViewCatalog) {
      ViewCatalog viewCatalog = (ViewCatalog) validationCatalog;
      viewCatalog.listViews(icebergNamespace).forEach(viewCatalog::dropView);
    }

    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    dropDatabase(flinkDatabase, true);
    super.clean();
  }

  private ViewCatalog viewCatalog() {
    assertThat(validationCatalog).isInstanceOf(ViewCatalog.class);
    return (ViewCatalog) validationCatalog;
  }

  private View createView(String dialect, String query) {
    return viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, VIEW_NAME))
        .withSchema(VIEW_SCHEMA)
        .withDefaultNamespace(icebergNamespace)
        .withQuery(dialect, query)
        .create();
  }

  private List<Row> expectedRows() {
    return Lists.newArrayList(Row.of(1L, "a"), Row.of(2L, "b"), Row.of(3L, "c"));
  }

  private static ResolvedCatalogView simpleResolvedView() {
    ResolvedSchema resolvedSchema = FlinkSchemaUtil.toResolvedSchema(VIEW_SCHEMA);
    // Flink's Schema class clashes with the imported Iceberg Schema, so it is qualified once here
    CatalogView view =
        CatalogView.of(
            org.apache.flink.table.api.Schema.newBuilder()
                .fromResolvedSchema(resolvedSchema)
                .build(),
            null,
            "SELECT 1",
            "SELECT 1",
            Maps.newHashMap());
    return new ResolvedCatalogView(view, resolvedSchema);
  }

  private static ResolvedCatalogView withOptions(CatalogView current, Map<String, String> options) {
    return new ResolvedCatalogView(
        CatalogView.of(
            current.getUnresolvedSchema(),
            current.getComment(),
            current.getOriginalQuery(),
            current.getExpandedQuery(),
            options),
        FlinkSchemaUtil.toResolvedSchema(VIEW_SCHEMA));
  }

  @TestTemplate
  public void testSelectFromView() {
    createView("flink", "SELECT id, data FROM test_table");
    assertSameElements(expectedRows(), sql("SELECT * FROM %s", VIEW_NAME));
  }

  @TestTemplate
  public void testSelectFromProjectedView() {
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, VIEW_NAME))
        .withSchema(PROJECTED_VIEW_SCHEMA)
        .withDefaultNamespace(icebergNamespace)
        .withQuery("flink", "SELECT id FROM test_table")
        .create();
    assertSameElements(
        Lists.newArrayList(Row.of(1L), Row.of(2L), Row.of(3L)), sql("SELECT * FROM %s", VIEW_NAME));
  }

  @TestTemplate
  public void testSelectFromViewWithQualifiedQuery() {
    createView(
        "flink", String.format("SELECT id, data FROM %s.%s.test_table", catalogName, DATABASE));
    assertSameElements(expectedRows(), sql("SELECT * FROM %s", VIEW_NAME));
  }

  @TestTemplate
  public void testSelectViewFromDifferentDatabase() {
    // unqualified references in the stored SQL must resolve against the view's own database,
    // not the session's current database (Flink expands views with the view's schema path)
    createView("flink", "SELECT id, data FROM test_table");
    sql("CREATE DATABASE %s.db2", catalogName);
    sql("USE db2");
    try {
      assertSameElements(
          expectedRows(), sql("SELECT * FROM %s.%s.%s", catalogName, DATABASE, VIEW_NAME));
    } finally {
      sql("USE %s", DATABASE);
      dropDatabase(catalogName + ".db2", true);
    }
  }

  @TestTemplate
  public void testViewReferencingAnotherView() {
    createView("flink", "SELECT id, data FROM test_table");
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, "second_view"))
        .withSchema(PROJECTED_VIEW_SCHEMA)
        .withDefaultNamespace(icebergNamespace)
        .withQuery("flink", "SELECT id FROM " + VIEW_NAME)
        .create();
    assertSameElements(
        Lists.newArrayList(Row.of(1L), Row.of(2L), Row.of(3L)), sql("SELECT * FROM second_view"));
  }

  @TestTemplate
  public void testSqlForFallsBackToAnotherDialect() throws Exception {
    // BaseView#sqlFor returns the first SQL representation when no "flink" one exists,
    // e.g. a view created by Spark
    createView("spark", "SELECT id, data FROM test_table");

    CatalogView catalogView =
        (CatalogView)
            getTableEnv()
                .getCatalog(catalogName)
                .get()
                .getTable(new ObjectPath(DATABASE, VIEW_NAME));
    assertThat(catalogView.getOriginalQuery()).isEqualTo("SELECT id, data FROM test_table");

    // ANSI SQL that both engines understand is directly usable
    assertSameElements(expectedRows(), sql("SELECT * FROM %s", VIEW_NAME));
  }

  @TestTemplate
  public void testSqlForPrefersExactDialectMatch() throws Exception {
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, VIEW_NAME))
        .withSchema(VIEW_SCHEMA)
        .withDefaultNamespace(icebergNamespace)
        .withQuery("spark", "SELECT id, data FROM spark_only_table")
        .withQuery("flink", "SELECT id, data FROM test_table")
        .create();

    CatalogView catalogView =
        (CatalogView)
            getTableEnv()
                .getCatalog(catalogName)
                .get()
                .getTable(new ObjectPath(DATABASE, VIEW_NAME));
    assertThat(catalogView.getOriginalQuery()).isEqualTo("SELECT id, data FROM test_table");
    assertSameElements(expectedRows(), sql("SELECT * FROM %s", VIEW_NAME));
  }

  @TestTemplate
  public void testViewCommentAndProperties() throws Exception {
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, VIEW_NAME))
        .withSchema(VIEW_SCHEMA)
        .withDefaultNamespace(icebergNamespace)
        .withQuery("flink", "SELECT id, data FROM test_table")
        .withProperty(ViewProperties.COMMENT, "view comment")
        .withProperty("key1", "value1")
        .create();

    CatalogBaseTable catalogView =
        getTableEnv().getCatalog(catalogName).get().getTable(new ObjectPath(DATABASE, VIEW_NAME));
    assertThat(catalogView.getComment()).isEqualTo("view comment");
    assertThat(catalogView.getOptions())
        .containsEntry("key1", "value1")
        .doesNotContainKey(ViewProperties.COMMENT);
  }

  @TestTemplate
  public void testListViews() throws Exception {
    assertThat(sql("SHOW VIEWS")).isEmpty();
    createView("flink", "SELECT id, data FROM test_table");
    assertThat(sql("SHOW VIEWS")).containsExactly(Row.of(VIEW_NAME));
    assertThat(getTableEnv().getCatalog(catalogName).get().listViews(DATABASE))
        .containsExactly(VIEW_NAME);
  }

  @TestTemplate
  public void testListTablesIncludesViews() {
    createView("flink", "SELECT id, data FROM test_table");
    // Flink's Catalog#listTables contract covers both tables and views
    assertThat(sql("SHOW TABLES")).containsExactlyInAnyOrder(Row.of(TABLE_NAME), Row.of(VIEW_NAME));
    assertThat(sql("SHOW VIEWS")).containsExactly(Row.of(VIEW_NAME));
  }

  @TestTemplate
  public void testTableExistsForView() {
    createView("flink", "SELECT id, data FROM test_table");
    Catalog flinkCatalog = getTableEnv().getCatalog(catalogName).get();
    assertThat(flinkCatalog.tableExists(new ObjectPath(DATABASE, VIEW_NAME))).isTrue();
    assertThat(flinkCatalog.tableExists(new ObjectPath(DATABASE, TABLE_NAME))).isTrue();
    assertThat(flinkCatalog.tableExists(new ObjectPath(DATABASE, "nonexistent"))).isFalse();
  }

  @TestTemplate
  public void testViewNotExist() {
    assertThatThrownBy(
            () ->
                getTableEnv()
                    .getCatalog(catalogName)
                    .get()
                    .getTable(new ObjectPath(DATABASE, "nonexistent")))
        .isInstanceOf(TableNotExistException.class)
        .hasMessageContaining("Table (or view) db.nonexistent does not exist");
    assertThatThrownBy(() -> sql("SELECT * FROM nonexistent"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Object 'nonexistent' not found");
  }

  @TestTemplate
  public void testMetadataTableNotRoutedToViewPath() {
    createView("flink", "SELECT id, data FROM test_table");
    // metadata table access must keep working and never hit the view branch
    assertThat(sql("SELECT * FROM %s$snapshots", TABLE_NAME)).isNotEmpty();
    assertThatThrownBy(
            () ->
                getTableEnv()
                    .getCatalog(catalogName)
                    .get()
                    .getTable(new ObjectPath(DATABASE, "nonexistent$snapshots")))
        .isInstanceOf(TableNotExistException.class)
        .hasMessageContaining("Table (or view) db.nonexistent$snapshots does not exist");
    assertThat(
            getTableEnv()
                .getCatalog(catalogName)
                .get()
                .tableExists(new ObjectPath(DATABASE, "nonexistent$snapshots")))
        .isFalse();
  }

  @TestTemplate
  public void testDescribeView() {
    createView("flink", "SELECT id, data FROM test_table");
    assertThat(sql("DESCRIBE %s", VIEW_NAME))
        .extracting(row -> row.getField(0))
        .containsExactly("id", "data");
  }

  @TestTemplate
  public void testViewWithDifferentDefaultNamespaceIsRejected() {
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, VIEW_NAME))
        .withSchema(VIEW_SCHEMA)
        .withDefaultNamespace(Namespace.of("some_other_db"))
        .withQuery("flink", "SELECT id, data FROM test_table")
        .create();

    assertThatThrownBy(() -> sql("SELECT * FROM %s", VIEW_NAME))
        .rootCause()
        .hasMessageContaining("default-namespace")
        .hasMessageContaining("some_other_db");
  }

  @TestTemplate
  public void testViewWithDifferentDefaultCatalogIsRejected() {
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, VIEW_NAME))
        .withSchema(VIEW_SCHEMA)
        .withDefaultCatalog("some_other_catalog")
        .withDefaultNamespace(icebergNamespace)
        .withQuery("flink", "SELECT id, data FROM test_table")
        .create();

    assertThatThrownBy(() -> sql("SELECT * FROM %s", VIEW_NAME))
        .rootCause()
        .hasMessageContaining("default-catalog")
        .hasMessageContaining("some_other_catalog");
  }

  @TestTemplate
  public void testViewWithMatchingDefaultsIsReadable() {
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, VIEW_NAME))
        .withSchema(VIEW_SCHEMA)
        .withDefaultCatalog(catalogName)
        .withDefaultNamespace(icebergNamespace)
        .withQuery("flink", "SELECT id, data FROM test_table")
        .create();

    assertSameElements(expectedRows(), sql("SELECT * FROM %s", VIEW_NAME));
  }

  @TestTemplate
  public void testCreateViewViaSql() {
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);

    assertSameElements(expectedRows(), sql("SELECT * FROM %s", VIEW_NAME));

    View view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    // the expanded query is stored: every reference is fully qualified, so resolution does not
    // depend on the reader's session
    assertThat(view.sqlFor("flink").sql())
        .containsIgnoringCase(
            String.format("FROM `%s`.`%s`.`%s`", catalogName, DATABASE, TABLE_NAME));
    assertThat(view.currentVersion().defaultNamespace()).isEqualTo(icebergNamespace);
    assertThat(view.currentVersion().defaultCatalog()).isEqualTo(catalogName);
    assertThat(view.schema().columns())
        .extracting(Types.NestedField::name)
        .containsExactly("id", "data");
  }

  @TestTemplate
  public void testCreateViewWithCommentAndColumnList() {
    sql(
        "CREATE VIEW %s (view_id, view_data) COMMENT 'a view comment' AS SELECT id, data FROM %s",
        VIEW_NAME, TABLE_NAME);

    View view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    assertThat(view.properties()).containsEntry(ViewProperties.COMMENT, "a view comment");
    assertThat(view.schema().columns())
        .extracting(Types.NestedField::name)
        .containsExactly("view_id", "view_data");
  }

  @TestTemplate
  public void testCreateViewIfNotExists() {
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);
    // IF NOT EXISTS is silent
    sql("CREATE VIEW IF NOT EXISTS %s AS SELECT id FROM %s", VIEW_NAME, TABLE_NAME);
    // the view was not replaced: it still exists with its original query
    View view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    assertThat(view.sqlFor("flink").sql()).containsIgnoringCase("data");

    // without IF NOT EXISTS, creation fails
    assertThatThrownBy(() -> sql("CREATE VIEW %s AS SELECT id FROM %s", VIEW_NAME, TABLE_NAME))
        .hasMessageContaining("Could not execute CreateTable")
        .cause()
        .isInstanceOf(TableAlreadyExistException.class)
        .hasMessageContaining(VIEW_NAME);
  }

  @TestTemplate
  public void testCreateViewOverExistingTableFails() {
    assertThatThrownBy(() -> sql("CREATE VIEW %s AS SELECT id FROM %s", TABLE_NAME, TABLE_NAME))
        .hasMessageContaining("Could not execute CreateTable")
        .cause()
        .isInstanceOf(TableAlreadyExistException.class)
        .hasMessageContaining(TABLE_NAME);

    // the table was not touched by the failed attempt
    assertSameElements(expectedRows(), sql("SELECT * FROM %s", TABLE_NAME));
  }

  @TestTemplate
  public void testCreateViewWithMetadataTableNameFails() {
    assertThatThrownBy(() -> sql("CREATE VIEW shadow$snapshots AS SELECT id FROM %s", TABLE_NAME))
        .hasMessageContaining("Could not execute CreateTable")
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot create view")
        .hasMessageContaining("metadata table");

    assertThat(sql("SHOW VIEWS")).isEmpty();
  }

  @TestTemplate
  public void testCreateViewWithUnqualifiedCrossDatabaseReference() {
    // the stored expanded query fully qualifies the reference at creation time, so the view
    // resolves against the table the creator saw, regardless of the reader's session database
    sql("CREATE DATABASE %s.db2", catalogName);
    sql("USE db2");
    try {
      sql("CREATE TABLE cross_t (id BIGINT)");
      sql("INSERT INTO cross_t VALUES (7)");
      sql("CREATE VIEW %s.%s.cross_view AS SELECT id FROM cross_t", catalogName, DATABASE);

      assertSameElements(
          Lists.newArrayList(Row.of(7L)),
          sql("SELECT * FROM %s.%s.cross_view", catalogName, DATABASE));

      // reading from the view's own database, where an unqualified cross_t would not resolve
      sql("USE %s", DATABASE);
      assertSameElements(Lists.newArrayList(Row.of(7L)), sql("SELECT * FROM cross_view"));
    } finally {
      sql("USE %s", DATABASE);
      sql("DROP TABLE IF EXISTS %s.db2.cross_t", catalogName);
      dropDatabase(catalogName + ".db2", true);
    }
  }

  @TestTemplate
  public void testCreateViewWithQualifiedCrossDatabaseReference() {
    // an explicitly qualified reference is stored as written, so it resolves the same way from
    // any session
    sql("CREATE DATABASE %s.db2", catalogName);
    try {
      sql("CREATE TABLE %s.db2.other_t (id BIGINT)", catalogName);
      sql("INSERT INTO %s.db2.other_t VALUES (9)", catalogName);
      sql("CREATE VIEW cross_view AS SELECT id FROM db2.other_t");

      assertSameElements(Lists.newArrayList(Row.of(9L)), sql("SELECT * FROM cross_view"));
      // the result does not depend on the reader's session database
      sql("USE db2");
      assertSameElements(
          Lists.newArrayList(Row.of(9L)),
          sql("SELECT * FROM %s.%s.cross_view", catalogName, DATABASE));
    } finally {
      sql("USE %s", DATABASE);
      sql("DROP TABLE IF EXISTS %s.db2.other_t", catalogName);
      dropDatabase(catalogName + ".db2", true);
    }
  }

  @TestTemplate
  public void testCreateViewNotSupportedByCatalog() {
    // a catalog without view support (HadoopCatalog) rejects CREATE VIEW with a clear error
    String noViewCatalog = catalogName + "_noviews";
    sql(
        "CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='file://%s/noviews')",
        noViewCatalog, warehouseRoot());
    try {
      sql("CREATE DATABASE %s.no_view_db", noViewCatalog);
      assertThatThrownBy(
              () ->
                  sql(
                      "CREATE VIEW %s.no_view_db.unsupported_view AS SELECT id, data FROM %s",
                      noViewCatalog, TABLE_NAME))
          .hasMessageContaining("Could not execute CreateTable")
          .cause()
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("Creating a view is not supported by catalog");
    } finally {
      dropDatabase(noViewCatalog + ".no_view_db", true);
      dropCatalog(noViewCatalog, true);
    }
  }

  @TestTemplate
  public void testDropView() {
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);
    assertThat(sql("SHOW VIEWS")).containsExactly(Row.of(VIEW_NAME));

    sql("DROP VIEW %s", VIEW_NAME);
    assertThat(sql("SHOW VIEWS")).isEmpty();
    assertThat(viewCatalog().viewExists(TableIdentifier.of(icebergNamespace, VIEW_NAME))).isFalse();
  }

  @TestTemplate
  public void testDropViewIfExists() {
    sql("DROP VIEW IF EXISTS nonexistent_view");
    assertThatThrownBy(() -> sql("DROP VIEW nonexistent_view"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("View with identifier")
        .hasMessageContaining("nonexistent_view")
        .hasMessageContaining("does not exist");
  }

  @TestTemplate
  public void testDropNonexistentViewThroughCatalogApi() {
    // SQL never reaches the catalog for a missing object; the catalog API reports it itself
    assertThatThrownBy(
            () ->
                getTableEnv()
                    .getCatalog(catalogName)
                    .get()
                    .dropTable(new ObjectPath(DATABASE, "nonexistent_view"), false))
        .isInstanceOf(TableNotExistException.class)
        .hasMessageContaining("nonexistent_view");
  }

  @TestTemplate
  public void testRenameView() {
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);
    sql("ALTER VIEW %s RENAME TO renamed_view", VIEW_NAME);

    assertThat(sql("SHOW VIEWS")).containsExactly(Row.of("renamed_view"));
    assertSameElements(expectedRows(), sql("SELECT * FROM renamed_view"));
  }

  @TestTemplate
  public void testRenameViewToMetadataTableNameFails() {
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);

    assertThatThrownBy(() -> sql("ALTER VIEW %s RENAME TO shadow$snapshots", VIEW_NAME))
        .hasMessageContaining("Could not execute ALTER VIEW")
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot rename")
        .hasMessageContaining("metadata table");

    // the view was not touched by the failed attempt
    assertThat(sql("SHOW VIEWS")).containsExactly(Row.of(VIEW_NAME));
  }

  @TestTemplate
  public void testRenameViewToExistingObjectFails() {
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, "second_view"))
        .withSchema(VIEW_SCHEMA)
        .withDefaultNamespace(icebergNamespace)
        .withQuery("flink", "SELECT id FROM test_table")
        .create();

    assertThatThrownBy(() -> sql("ALTER VIEW %s RENAME TO second_view", VIEW_NAME))
        .hasMessageContaining("Could not execute ALTER VIEW")
        .cause()
        .isInstanceOf(TableAlreadyExistException.class)
        .hasMessageContaining("second_view");
  }

  @TestTemplate
  public void testAlterViewAs() {
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);
    sql("ALTER VIEW %s AS SELECT id FROM %s", VIEW_NAME, TABLE_NAME);

    assertSameElements(
        Lists.newArrayList(Row.of(1L), Row.of(2L), Row.of(3L)), sql("SELECT * FROM %s", VIEW_NAME));

    View view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    assertThat(view.versions()).hasSize(2);
    assertThat(view.schema().columns()).extracting(Types.NestedField::name).containsExactly("id");
    assertThat(view.currentVersion().defaultNamespace()).isEqualTo(icebergNamespace);
    assertThat(view.currentVersion().defaultCatalog()).isEqualTo(catalogName);
    // the replaced query is stored the same way a created one is: fully qualified
    assertThat(view.sqlFor("flink").sql())
        .containsIgnoringCase(
            String.format("FROM `%s`.`%s`.`%s`", catalogName, DATABASE, TABLE_NAME));
  }

  @TestTemplate
  public void testAlterViewNotSupportedByCatalog() throws Exception {
    // reaching the no-view-catalog branch of alterTable requires the catalog API: a view can
    // never exist in a Hadoop catalog, so SQL cannot get this far
    String noViewCatalog = catalogName + "_alter_nv";
    sql(
        "CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='file://%s/alter_nv')",
        noViewCatalog, warehouseRoot());
    try {
      assertThatThrownBy(
              () ->
                  getTableEnv()
                      .getCatalog(noViewCatalog)
                      .get()
                      .alterTable(
                          new ObjectPath(DATABASE, VIEW_NAME),
                          simpleResolvedView(),
                          false /* ignoreIfNotExists */))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("Altering a view is not supported by catalog")
          .hasMessageContaining(noViewCatalog);
    } finally {
      dropCatalog(noViewCatalog, true);
    }
  }

  @TestTemplate
  public void testAlterViewNotExists() throws Exception {
    // SQL pre-checks existence in the CatalogManager, so the catalog API is the only way to
    // exercise these branches
    Catalog flinkCatalog = getTableEnv().getCatalog(catalogName).get();
    ObjectPath path = new ObjectPath(DATABASE, "nonexistent_view");
    ResolvedCatalogView resolvedView = simpleResolvedView();

    assertThatThrownBy(
            () -> flinkCatalog.alterTable(path, resolvedView, false /* ignoreIfNotExists */))
        .isInstanceOf(TableNotExistException.class)
        .hasMessageContaining("nonexistent_view")
        .hasMessageContaining("does not exist");

    // with ignoreIfNotExists the missing view is silently skipped
    flinkCatalog.alterTable(path, resolvedView, true /* ignoreIfNotExists */);
  }

  @TestTemplate
  public void testAlterViewAsPreservesProperties() {
    sql("CREATE VIEW %s COMMENT 'keep me' AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);
    sql("ALTER VIEW %s AS SELECT id FROM %s", VIEW_NAME, TABLE_NAME);

    View view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    assertThat(view.properties()).containsEntry(ViewProperties.COMMENT, "keep me");
  }

  @TestTemplate
  public void testAlterViewPropertiesViaCatalogApi() throws Exception {
    // Flink's default SQL dialect has no ALTER VIEW ... SET syntax (only RENAME and AS);
    // property updates arrive through the catalog API, e.g. from the Hive dialect
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);

    Catalog flinkCatalog = getTableEnv().getCatalog(catalogName).get();
    ObjectPath path = new ObjectPath(DATABASE, VIEW_NAME);
    CatalogView current = (CatalogView) flinkCatalog.getTable(path);

    Map<String, String> newOptions = Maps.newHashMap(current.getOptions());
    newOptions.put("key1", "value1");
    flinkCatalog.alterTable(path, withOptions(current, newOptions), false /* ignoreIfNotExists */);

    View view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    assertThat(view.properties()).containsEntry("key1", "value1");
    // a property change must not create a new view version
    assertThat(view.versions()).hasSize(1);

    // keys absent from the new definition are removed, like alterTable does for tables
    flinkCatalog.alterTable(
        path, withOptions(current, current.getOptions()), false /* ignoreIfNotExists */);
    view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    assertThat(view.properties()).doesNotContainKey("key1");
    assertThat(view.versions()).hasSize(1);
  }

  @TestTemplate
  public void testAlterViewViaTableChangesApi() throws Exception {
    // the TableChange-based alterTable overload must route views the same way; newTable
    // already carries the fully altered definition
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);

    Catalog flinkCatalog = getTableEnv().getCatalog(catalogName).get();
    ObjectPath path = new ObjectPath(DATABASE, VIEW_NAME);
    CatalogView current = (CatalogView) flinkCatalog.getTable(path);

    Map<String, String> newOptions = Maps.newHashMap(current.getOptions());
    newOptions.put("key2", "value2");
    flinkCatalog.alterTable(
        path,
        withOptions(current, newOptions),
        Lists.newArrayList(TableChange.set("key2", "value2")),
        false);

    View view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    assertThat(view.properties()).containsEntry("key2", "value2");
    // a property change must not create a new view version
    assertThat(view.versions()).hasSize(1);
  }

  @TestTemplate
  public void testAlterViewAsDroppingOtherDialectFails() {
    // like Spark, only the flink representation is written on ALTER VIEW AS, and core refuses
    // a replace that loses another engine's dialect unless replace.drop-dialect.allowed=true
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, VIEW_NAME))
        .withSchema(VIEW_SCHEMA)
        .withDefaultNamespace(icebergNamespace)
        .withQuery("spark", "SELECT id, data FROM test_table")
        .withQuery("flink", "SELECT id, data FROM test_table")
        .create();

    assertThatThrownBy(() -> sql("ALTER VIEW %s AS SELECT id FROM %s", VIEW_NAME, TABLE_NAME))
        .hasMessageContaining("Could not execute AlterTable")
        .rootCause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot replace view due to loss of view dialects")
        .hasMessageContaining(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED);

    // the view was not touched by the failed attempt
    View view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    assertThat(view.versions()).hasSize(1);
    assertThat(view.currentVersion().representations()).hasSize(2);
    assertSameElements(expectedRows(), sql("SELECT * FROM %s", VIEW_NAME));
  }

  @TestTemplate
  public void testAlterViewAsDropsOtherDialectWhenAllowed() {
    // with replace.drop-dialect.allowed=true the same alter goes through and, like Spark,
    // resets the view to the altering engine's dialect only
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, VIEW_NAME))
        .withSchema(VIEW_SCHEMA)
        .withDefaultNamespace(icebergNamespace)
        .withQuery("spark", "SELECT id, data FROM test_table")
        .withQuery("flink", "SELECT id, data FROM test_table")
        .withProperty(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED, "true")
        .create();

    sql("ALTER VIEW %s AS SELECT id FROM %s", VIEW_NAME, TABLE_NAME);

    View view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    assertThat(view.versions()).hasSize(2);
    assertThat(view.currentVersion().representations()).hasSize(1);
    assertThat(view.sqlFor("flink").sql())
        .containsIgnoringCase(
            String.format("FROM `%s`.`%s`.`%s`", catalogName, DATABASE, TABLE_NAME));
    assertSameElements(
        Lists.newArrayList(Row.of(1L), Row.of(2L), Row.of(3L)), sql("SELECT * FROM %s", VIEW_NAME));
  }

  @TestTemplate
  public void testAlterViewAsOnViewWithoutFlinkDialect() {
    // a view without a flink representation is never treated as "query unchanged", even when
    // the new text matches the other dialect's SQL: the replace fires and core's dialect
    // guard makes the loss of the foreign representation explicit
    viewCatalog()
        .buildView(TableIdentifier.of(icebergNamespace, VIEW_NAME))
        .withSchema(new Schema(Types.NestedField.optional(1, "EXPR$0", Types.IntegerType.get())))
        .withDefaultNamespace(icebergNamespace)
        .withQuery("spark", "SELECT 1")
        .create();

    assertThatThrownBy(() -> sql("ALTER VIEW %s AS SELECT 1", VIEW_NAME))
        .hasMessageContaining("Could not execute AlterTable")
        .rootCause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot replace view due to loss of view dialects");
  }

  @TestTemplate
  public void testGetViewViaCatalogApi() throws Exception {
    createView("flink", "SELECT id, data FROM test_table");

    CatalogBaseTable catalogBaseTable =
        getTableEnv().getCatalog(catalogName).get().getTable(new ObjectPath(DATABASE, VIEW_NAME));

    assertThat(catalogBaseTable.getTableKind()).isEqualTo(CatalogBaseTable.TableKind.VIEW);
    assertThat(catalogBaseTable).isInstanceOf(CatalogView.class);
    CatalogView catalogView = (CatalogView) catalogBaseTable;
    assertThat(catalogView.getOriginalQuery()).isEqualTo("SELECT id, data FROM test_table");
    assertThat(catalogView.getExpandedQuery()).isEqualTo("SELECT id, data FROM test_table");
    assertThat(catalogView.getUnresolvedSchema().getColumns())
        .extracting(org.apache.flink.table.api.Schema.UnresolvedColumn::getName)
        .containsExactly("id", "data");
  }
}
