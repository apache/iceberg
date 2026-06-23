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
import org.apache.iceberg.spark.source.SparkMaterializedView;
import org.apache.iceberg.spark.source.SparkView;
import org.apache.iceberg.view.RefreshState;
import org.apache.iceberg.view.RefreshStateParser;
import org.apache.iceberg.view.SourceTableState;
import org.apache.iceberg.view.View;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMaterializedViews extends ExtensionsTestBase {
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
  public void testNeverRefreshedMvIsNotFresh() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    // A newly created MV has no snapshots on its storage table, so it's not fresh.
    // loadView should succeed (returns stale view)
    try {
      assertThat(sparkViewCatalog().loadView(viewIdentifier())).isInstanceOf(SparkView.class);
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

    // Fresh MV: loadView should throw since the engine should use loadTable instead
    assertThatThrownBy(() -> sparkViewCatalog().loadView(viewIdentifier()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("fresh");
  }

  @TestTemplate
  public void testFallbackToViewWhenStale() {
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    simulateRefresh();

    // Insert more data to invalidate the refresh
    sql("INSERT INTO %s VALUES (3, 'c')", tableName);

    // Stale MV: loadView should return SparkView (falls back to query execution)
    try {
      assertThat(sparkViewCatalog().loadView(viewIdentifier())).isInstanceOf(SparkView.class);
    } catch (NoSuchViewException e) {
      fail("Stale materialized view should be loadable as a view");
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
      assertThat(sparkViewCatalog().loadView(viewIdentifier())).isInstanceOf(SparkView.class);
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

  private void simulateRefresh() {
    View view = loadIcebergView();
    org.apache.iceberg.catalog.TableIdentifier storageTableId =
        view.currentVersion().storageTable();

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
                    "test-uuid",
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

  private Identifier viewIdentifier() {
    return Identifier.of(new String[] {NAMESPACE.toString()}, materializedViewName);
  }

  private SparkCatalog sparkCatalog() {
    return (SparkCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  private View loadIcebergView() {
    org.apache.iceberg.catalog.ViewCatalog icebergViewCatalog =
        (org.apache.iceberg.catalog.ViewCatalog) sparkCatalog().icebergCatalog();
    return icebergViewCatalog.loadView(TableIdentifier.of(NAMESPACE, materializedViewName));
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
