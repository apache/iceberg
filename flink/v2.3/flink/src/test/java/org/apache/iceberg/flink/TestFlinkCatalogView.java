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

import java.util.List;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
    org.apache.flink.table.catalog.Catalog flinkCatalog =
        getTableEnv().getCatalog(catalogName).get();
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
        .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
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
  public void testCreateViewViaSql() {
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);

    assertSameElements(expectedRows(), sql("SELECT * FROM %s", VIEW_NAME));

    View view = viewCatalog().loadView(TableIdentifier.of(icebergNamespace, VIEW_NAME));
    // Flink hands the catalog its normalized (unparsed) SQL; references must stay unexpanded
    assertThat(view.sqlFor("flink").sql())
        .containsIgnoringCase(String.format("FROM `%s`", TABLE_NAME))
        .doesNotContain(catalogName);
    assertThat(view.currentVersion().defaultNamespace()).isEqualTo(icebergNamespace);
    assertThat(view.currentVersion().defaultCatalog()).isNull();
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
    // without it, creation fails
    assertThatThrownBy(() -> sql("CREATE VIEW %s AS SELECT id FROM %s", VIEW_NAME, TABLE_NAME))
        .hasMessageContaining(VIEW_NAME);
  }

  @TestTemplate
  public void testCreateViewOverExistingTableFails() {
    assertThatThrownBy(() -> sql("CREATE VIEW %s AS SELECT id FROM %s", TABLE_NAME, TABLE_NAME))
        .hasMessageContaining(TABLE_NAME);
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
  public void testAlterViewAsIsRejected() {
    sql("CREATE VIEW %s AS SELECT id, data FROM %s", VIEW_NAME, TABLE_NAME);
    assertThatThrownBy(() -> sql("ALTER VIEW %s AS SELECT id FROM %s", VIEW_NAME, TABLE_NAME))
        .hasMessageContaining("Could not execute AlterTable")
        .rootCause()
        .hasMessageContaining("Altering a view is not supported");
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
