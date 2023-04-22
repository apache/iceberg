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
package org.apache.iceberg.gcp.biglake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.biglake.v1.Catalog;
import com.google.cloud.bigquery.biglake.v1.CatalogName;
import com.google.cloud.bigquery.biglake.v1.Database;
import com.google.cloud.bigquery.biglake.v1.DatabaseName;
import com.google.cloud.bigquery.biglake.v1.HiveDatabaseOptions;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.TableName;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class BigLakeCatalogTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "us";
  private static final String CATALOG_ID = "biglake";

  @Mock private BigLakeClient bigLakeClient;

  private BigLakeCatalog bigLakeCatalog;
  private String warehouseLocation;

  @Before
  public void before() throws Exception {
    this.bigLakeCatalog = new BigLakeCatalog();
    this.warehouseLocation = tempFolder.newFolder("hive-warehouse").toString();

    bigLakeCatalog.initialize(
        CATALOG_ID,
        /* properties= */ ImmutableMap.of(
            BigLakeCatalog.PROPERTIES_KEY_GCP_PROJECT,
            GCP_PROJECT,
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation),
        GCP_PROJECT,
        GCP_REGION,
        bigLakeClient);
  }

  @Test
  public void testDefaultWarehouseWithDatabaseLocation_asExpected() {
    when(bigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(HiveDatabaseOptions.newBuilder().setLocationUri("db_folder"))
                .build());

    assertEquals(
        "db_folder/table",
        bigLakeCatalog.defaultWarehouseLocation(TableIdentifier.of("db", "table")));
  }

  @Test
  public void testDefaultWarehouseeWithoutDatabaseLocation_asExpected() {
    when(bigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, "us", CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder().setHiveOptions(HiveDatabaseOptions.getDefaultInstance()).build());

    assertEquals(
        warehouseLocation + "/db.db/table",
        bigLakeCatalog.defaultWarehouseLocation(TableIdentifier.of("db", "table")));
  }

  @Test
  public void testCreateTable_succeedWhenNotExist() throws Exception {
    // The table to create does not exist.
    TableName tableName = TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db", "tbl");
    TableIdentifier tableIdent = TableIdentifier.of("db", "tbl");
    Schema schema = BigLakeTestUtils.getTestSchema();

    when(bigLakeClient.getTable(tableName))
        .thenThrow(new NoSuchTableException("error message getTable"));
    Table createdTable = BigLakeTestUtils.createTestTable(tempFolder, bigLakeCatalog, tableName);
    reset(bigLakeClient);
    when(bigLakeClient.getTable(tableName)).thenReturn(createdTable, createdTable);

    org.apache.iceberg.Table loadedTable = bigLakeCatalog.loadTable(tableIdent);
    assertEquals(SchemaParser.toJson(schema), SchemaParser.toJson(loadedTable.schema()));

    // Creates a table that already exists.
    Exception exception =
        assertThrows(
            AlreadyExistsException.class,
            () ->
                bigLakeCatalog
                    .buildTable(tableIdent, schema)
                    .withLocation(tempFolder.newFolder("new_tbl").toString())
                    .createTransaction()
                    .commitTransaction());
    assertTrue(exception.getMessage().contains("already exist"));
  }

  @Test
  public void testListTables_asExpected() {
    when(bigLakeClient.listTables(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db0")))
        .thenReturn(
            ImmutableList.of(
                Table.newBuilder()
                    .setName("projects/proj0/locations/us/catalogs/cat0/databases/db0/tables/tbl0")
                    .build(),
                Table.newBuilder()
                    .setName("projects/proj0/locations/us/catalogs/cat0/databases/db0/tables/tbl1")
                    .build()));

    List<TableIdentifier> result = bigLakeCatalog.listTables(Namespace.of("db0"));
    assertEquals(2, result.size());
    assertEquals(TableIdentifier.of("db0", "tbl0"), result.get(0));
    assertEquals(TableIdentifier.of("db0", "tbl1"), result.get(1));
  }

  @Test
  public void testListTables_emptyNamespace_checkCatalogEmptiness() {
    when(bigLakeClient.listDatabases(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID)))
        .thenReturn(ImmutableList.of(Database.getDefaultInstance()));

    List<TableIdentifier> result = bigLakeCatalog.listTables(Namespace.of());
    assertEquals(1, result.size());
    assertEquals(TableIdentifier.of("placeholder"), result.get(0));
  }

  @Test
  public void testListTables_emptyNamespace_noDatabase() {
    when(bigLakeClient.listDatabases(any(CatalogName.class))).thenReturn(ImmutableList.of());

    assertTrue(bigLakeCatalog.listTables(Namespace.of()).isEmpty());
  }

  @Test
  public void testDropTable_throwWhenTableNotFound() {
    TableName tableName = TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db", "tbl");
    when(bigLakeClient.getTable(tableName))
        .thenThrow(new NoSuchTableException("error message getTable"));
    doThrow(new NoSuchTableException("error message deleteTable"))
        .when(bigLakeClient)
        .deleteTable(tableName);

    Exception exception =
        assertThrows(
            NoSuchTableException.class,
            () -> bigLakeCatalog.dropTable(TableIdentifier.of("db", "tbl"), /* purge = */ false));
    assertEquals("error message deleteTable", exception.getMessage());
  }

  @Test
  public void testDropTable_succeedsWhenTableExists_deleteFiles() throws Exception {
    TableName tableName = TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db", "tbl");
    TableIdentifier tableIdent = TableIdentifier.of("db", "tbl");

    when(bigLakeClient.getTable(tableName))
        .thenThrow(new NoSuchTableException("error message getTable"));
    Table createdTable = BigLakeTestUtils.createTestTable(tempFolder, bigLakeCatalog, tableName);
    String tableDir = createdTable.getHiveOptions().getStorageDescriptor().getLocationUri();
    assertTrue(BigLakeTestUtils.getIcebergMetadataFilePath(tableDir).isPresent());

    reset(bigLakeClient);
    when(bigLakeClient.getTable(tableName)).thenReturn(createdTable, createdTable);
    when(bigLakeClient.deleteTable(tableName)).thenReturn(createdTable, createdTable);

    bigLakeCatalog.dropTable(tableIdent, /* purge = */ false);
    assertTrue(BigLakeTestUtils.getIcebergMetadataFilePath(tableDir).isPresent());

    bigLakeCatalog.dropTable(tableIdent, /* purge = */ true);
    assertFalse(BigLakeTestUtils.getIcebergMetadataFilePath(tableDir).isPresent());
  }

  @Test
  public void testRenameTable_sameDatabase_succeed() {
    when(bigLakeClient.renameTable(
            TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db0", "t1"),
            TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db0", "t2")))
        .thenReturn(
            Table.newBuilder()
                .setName(
                    String.format(
                        "projects/%s/locations/us/catalogs/%s/databases/db0/tables/t2",
                        GCP_PROJECT, CATALOG_ID))
                .build());

    bigLakeCatalog.renameTable(TableIdentifier.of("db0", "t1"), TableIdentifier.of("db0", "t2"));
  }

  @Test
  public void testRenameTable_differentDatabase_fail() {
    when(bigLakeClient.renameTable(
            TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db0", "t1"),
            TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db1", "t2")))
        .thenReturn(
            Table.newBuilder()
                .setName(
                    String.format(
                        "projects/%s/locations/us/catalogs/%s/databases/db1/tables/t2",
                        GCP_PROJECT, CATALOG_ID))
                .build());

    Exception exception =
        assertThrows(
            ValidationException.class,
            () ->
                bigLakeCatalog.renameTable(
                    TableIdentifier.of("db0", "t1"), TableIdentifier.of("db1", "t2")));
    assertEquals("New table name must be in the same database", exception.getMessage());
  }

  @Test
  public void testCreateNamespace_createCatalogWhenEmptyNamespace() throws Exception {
    when(bigLakeClient.createCatalog(
            CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID), Catalog.getDefaultInstance()))
        .thenReturn(Catalog.getDefaultInstance());

    bigLakeCatalog.createNamespace(Namespace.of(new String[] {}), ImmutableMap.of());
  }

  @Test
  public void testCreateNamespace_createDatabase() throws Exception {
    String dbId = "db";
    Map<String, String> metadata = ImmutableMap.of();
    String dbDir = warehouseLocation + String.format("/%s.db", dbId);
    Database.Builder builder = Database.newBuilder().setType(Database.Type.HIVE);
    builder.getHiveOptionsBuilder().putAllParameters(metadata).setLocationUri(dbDir);
    Database db = builder.build();
    when(bigLakeClient.createDatabase(
            DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, dbId), db))
        .thenReturn(db);

    bigLakeCatalog.createNamespace(Namespace.of(new String[] {dbId}), metadata);
    File f = new File(dbDir);
    assertTrue(f.exists());
    assertTrue(f.isDirectory());
  }

  @Test
  public void testCreateNamespace_failWhenInvalid() throws Exception {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                bigLakeCatalog.createNamespace(
                    Namespace.of(new String[] {"n0", "n1"}), ImmutableMap.of()));
    assertEquals(
        "BigLake catalog namespace can have zero (catalog) or one level (database), invalid"
            + " namespace: n0.n1",
        exception.getMessage());
  }

  @Test
  public void testListNamespaces_asExpected() {
    when(bigLakeClient.listDatabases(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID)))
        .thenReturn(
            ImmutableList.of(
                Database.newBuilder()
                    .setName("projects/proj0/locations/us/catalogs/cat0/databases/db0")
                    .build(),
                Database.newBuilder()
                    .setName("projects/proj0/locations/us/catalogs/cat0/databases/db1")
                    .build()));

    List<Namespace> result = bigLakeCatalog.listNamespaces(Namespace.of());
    assertEquals(2, result.size());
    assertEquals(Namespace.of("db0"), result.get(0));
    assertEquals(Namespace.of("db1"), result.get(1));
  }

  @Test
  public void testListNamespaces_emptyWhenInvalid() {
    assertTrue(bigLakeCatalog.listNamespaces(Namespace.of("db")).isEmpty());
  }

  @Test
  public void testDropNamespace_deleteCatalogWhenEmptyNamespace() {
    doNothing()
        .when(bigLakeClient)
        .deleteCatalog(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID));

    bigLakeCatalog.dropNamespace(Namespace.of(new String[] {}));
  }

  @Test
  public void testDropNamespace_deleteDatabase() {
    doNothing()
        .when(bigLakeClient)
        .deleteDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db"));

    bigLakeCatalog.dropNamespace(Namespace.of(new String[] {"db"}));
  }

  @Test
  public void testDropNamespace_failWhenInvalid() throws Exception {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigLakeCatalog.dropNamespace(Namespace.of(new String[] {"n0", "n1"})));
    assertEquals(
        "BigLake catalog namespace can have zero (catalog) or one level (database), invalid"
            + " namespace: n0.n1",
        exception.getMessage());
  }

  @Test
  public void testSetProperties_failWhenNamespacesAreInvalid() throws Exception {
    assertFalse(bigLakeCatalog.setProperties(Namespace.of(new String[] {}), ImmutableMap.of()));
    assertFalse(
        bigLakeCatalog.setProperties(Namespace.of(new String[] {"db", "tbl"}), ImmutableMap.of()));
  }

  @Test
  public void testSetProperties_succeedForDatabase() throws Exception {
    when(bigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(
                    HiveDatabaseOptions.newBuilder()
                        .putParameters("key1", "value1")
                        .putParameters("key2", "value2"))
                .build());
    when(bigLakeClient.updateDatabaseParameters(
            DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db"),
            ImmutableMap.of("key3", "value3")))
        .thenReturn(Database.getDefaultInstance());

    assertTrue(
        bigLakeCatalog.setProperties(
            Namespace.of(new String[] {"db"}),
            ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3")));
  }

  @Test
  public void testRemoveProperties_failWhenNamespacesAreInvalid() throws Exception {
    assertFalse(bigLakeCatalog.removeProperties(Namespace.of(new String[] {}), ImmutableSet.of()));
    assertFalse(
        bigLakeCatalog.removeProperties(
            Namespace.of(new String[] {"db", "tbl"}), ImmutableSet.of()));
  }

  @Test
  public void testRemoveProperties_succeedForDatabase() throws Exception {
    when(bigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(
                    HiveDatabaseOptions.newBuilder()
                        .putParameters("key1", "value1")
                        .putParameters("key2", "value2"))
                .build());
    when(bigLakeClient.updateDatabaseParameters(
            DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db"),
            ImmutableMap.of("key2", "value2")))
        .thenReturn(Database.getDefaultInstance());

    assertTrue(
        bigLakeCatalog.removeProperties(
            Namespace.of(new String[] {"db"}), ImmutableSet.of("key1", "key3")));
  }

  @Test
  public void testLoadNamespaceMetadata_catalogAsExpected() throws Exception {
    when(bigLakeClient.getCatalog(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID)))
        .thenReturn(Catalog.getDefaultInstance());

    assertTrue(bigLakeCatalog.loadNamespaceMetadata(Namespace.of(new String[] {})).isEmpty());
  }

  @Test
  public void testLoadNamespaceMetadata_databaseAsExpected() throws Exception {
    when(bigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(
                    HiveDatabaseOptions.newBuilder()
                        .setLocationUri("my location uri")
                        .putParameters("key1", "value1")
                        .putParameters("key2", "value2"))
                .build());

    assertEquals(
        ImmutableMap.of("location", "my location uri", "key1", "value1", "key2", "value2"),
        bigLakeCatalog.loadNamespaceMetadata(Namespace.of(new String[] {"db"})));
  }

  @Test
  public void testLoadNamespaceMetadata_failWhenInvalid() throws Exception {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigLakeCatalog.loadNamespaceMetadata(Namespace.of(new String[] {"n0", "n1"})));
    assertEquals(
        "BigLake catalog namespace can have zero (catalog) or one level (database), invalid"
            + " namespace: n0.n1",
        exception.getMessage());
  }

  @Test
  public void testSetBigLakeCatalogInProperties_asExpected() throws Exception {
    BigLakeCatalog catalog = new BigLakeCatalog();
    catalog.initialize(
        CATALOG_ID,
        /* properties= */ ImmutableMap.of(
            BigLakeCatalog.PROPERTIES_KEY_GCP_PROJECT,
            GCP_PROJECT,
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation,
            BigLakeCatalog.PROPERTIES_KEY_BLMS_CATALOG,
            "customized_catalog"),
        GCP_PROJECT,
        GCP_REGION,
        bigLakeClient);

    when(bigLakeClient.createCatalog(
            CatalogName.of(GCP_PROJECT, GCP_REGION, "customized_catalog"),
            Catalog.getDefaultInstance()))
        .thenReturn(Catalog.getDefaultInstance());

    bigLakeCatalog.createNamespace(Namespace.of(new String[] {}), ImmutableMap.of());
  }

  @Test
  public void testName_asExpected() throws Exception {
    assertEquals("biglake", bigLakeCatalog.name());
  }

  @Test
  public void testProperties_asExpected() throws Exception {
    assertEquals(
        ImmutableMap.of("gcp_project", GCP_PROJECT, "warehouse", warehouseLocation),
        bigLakeCatalog.properties());
  }

  @Test
  public void testNewTableOps_asExpected() throws Exception {
    assertNotNull(bigLakeCatalog.newTableOps(TableIdentifier.of("db", "tbl")));
  }

  @Test
  public void testNewTableOps_failedForInvalidNamespace() throws Exception {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigLakeCatalog.newTableOps(TableIdentifier.of(Namespace.of("n0", "n1"), "tbl")));
    assertEquals(
        "BigLake database namespace must use format <catalog>.<database>, invalid namespace: n0.n1",
        exception.getMessage());
  }
}
