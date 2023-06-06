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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.testing.LocalChannelProvider;
import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.pathtemplate.ValidationException;
import com.google.cloud.bigquery.biglake.v1.Catalog;
import com.google.cloud.bigquery.biglake.v1.CatalogName;
import com.google.cloud.bigquery.biglake.v1.Database;
import com.google.cloud.bigquery.biglake.v1.DatabaseName;
import com.google.cloud.bigquery.biglake.v1.HiveDatabaseOptions;
import com.google.cloud.bigquery.biglake.v1.MetastoreServiceSettings;
import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class BigLakeCatalogTest extends CatalogTests<BigLakeCatalog> {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @TempDir public Path temp;

  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "us";
  private static final String CATALOG_ID = "biglake";

  private static MockMetastoreService mockMetastoreService;
  private static MockServiceHelper mockServiceHelper;

  private LocalChannelProvider channelProvider;
  private BigLakeCatalog bigLakeCatalogUsingMockService;

  private String warehouseLocation;

  private BigLakeClient mockBigLakeClient;
  private BigLakeCatalog bigLakeCatalogUsingMockClient;

  @BeforeAll
  public static void setUpStaticBigLakeService() throws Exception {
    mockMetastoreService = new MockMetastoreService();
    mockServiceHelper =
        new MockServiceHelper(
            UUID.randomUUID().toString(), Arrays.<MockGrpcService>asList(mockMetastoreService));
    mockServiceHelper.start();
  }

  @AfterAll
  public static void stopStaticBigLakeService() {
    mockServiceHelper.stop();
  }

  @BeforeEach
  public void createCatalog() throws Exception {
    mockMetastoreService.reset();

    File warehouse = temp.toFile();
    warehouseLocation = warehouse.getAbsolutePath();

    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            BigLakeCatalog.PROPERTIES_KEY_GCP_PROJECT,
            GCP_PROJECT,
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation);

    channelProvider = mockServiceHelper.createChannelProvider();
    MetastoreServiceSettings settings =
        MetastoreServiceSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();

    bigLakeCatalogUsingMockService = new BigLakeCatalog();
    bigLakeCatalogUsingMockService.setConf(new Configuration());
    bigLakeCatalogUsingMockService.initialize(
        CATALOG_ID,
        properties,
        GCP_PROJECT,
        GCP_REGION,
        new BigLakeClient(settings, GCP_PROJECT, GCP_REGION));

    mockBigLakeClient = mock(BigLakeClient.class);
    bigLakeCatalogUsingMockClient = new BigLakeCatalog();
    bigLakeCatalogUsingMockClient.setConf(new Configuration());
    bigLakeCatalogUsingMockClient.initialize(
        CATALOG_ID, properties, GCP_PROJECT, GCP_REGION, mockBigLakeClient);
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected BigLakeCatalog catalog() {
    return bigLakeCatalogUsingMockService;
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    return false;
  }

  @Test
  public void testNamespaceWithSlash() {
    BigLakeCatalog catalog = catalog();

    Exception exception =
        assertThrows(
            InvalidArgumentException.class, () -> catalog.createNamespace(Namespace.of("new/db")));
    assertTrue(exception.getMessage().contains("Database ID is invalid"));
  }

  @Test
  public void testTableNameWithSlash() {
    BigLakeCatalog catalog = catalog();

    catalog.createNamespace(Namespace.of("ns"));
    TableIdentifier ident = TableIdentifier.of("ns", "tab/le");

    Exception exception =
        assertThrows(ValidationException.class, () -> catalog.buildTable(ident, SCHEMA).create());
    assertEquals("Invalid character \"/\" in path section \"tab/le\".", exception.getMessage());
  }

  @Test
  public void testDefaultWarehouseWithDatabaseLocation_asExpected() {
    when(mockBigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(HiveDatabaseOptions.newBuilder().setLocationUri("db_folder"))
                .build());

    assertEquals(
        "db_folder/table",
        bigLakeCatalogUsingMockClient.defaultWarehouseLocation(TableIdentifier.of("db", "table")));
  }

  @Test
  public void testDefaultWarehouseWithoutDatabaseLocation_asExpected() {
    when(mockBigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, "us", CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder().setHiveOptions(HiveDatabaseOptions.getDefaultInstance()).build());

    assertEquals(
        warehouseLocation + "/db.db/table",
        bigLakeCatalogUsingMockClient.defaultWarehouseLocation(TableIdentifier.of("db", "table")));
  }

  @Test
  public void testRenameTable_differentDatabase_fail() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                bigLakeCatalogUsingMockClient.renameTable(
                    TableIdentifier.of("db0", "t1"), TableIdentifier.of("db1", "t2")));
    assertEquals("New table name must be in the same database", exception.getMessage());
  }

  @Test
  public void testCreateNamespace_createCatalogWhenEmptyNamespace() throws Exception {
    bigLakeCatalogUsingMockClient.createNamespace(Namespace.of(new String[] {}), ImmutableMap.of());
    verify(mockBigLakeClient, times(1))
        .createCatalog(
            CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID), Catalog.getDefaultInstance());
  }

  @Test
  public void testCreateNamespace_failWhenInvalid() throws Exception {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                bigLakeCatalogUsingMockClient.createNamespace(
                    Namespace.of(new String[] {"n0", "n1"}), ImmutableMap.of()));
    assertEquals(
        "BigLake catalog namespace can have zero (catalog) or one level (database), invalid"
            + " namespace: n0.n1",
        exception.getMessage());
  }

  @Test
  public void testListNamespaces_emptyWhenInvalid() {
    assertTrue(bigLakeCatalogUsingMockClient.listNamespaces(Namespace.of("db")).isEmpty());
  }

  @Test
  public void testDropNamespace_deleteCatalogWhenEmptyNamespace() {
    bigLakeCatalogUsingMockClient.dropNamespace(Namespace.of(new String[] {}));
    verify(mockBigLakeClient, times(1))
        .deleteCatalog(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID));
  }

  // BigLake catalog plugin supports dropping a BigLake catalog resource. Spark calls listTables
  // with an empty namespace in this case, the purpose is verifying the namespace is empty. We
  // check whether there are databases in the BigLake catalog instead.
  @Test
  public void testListTables_emptyNamespace_noDatabase() {
    when(mockBigLakeClient.listDatabases(any(CatalogName.class))).thenReturn(ImmutableList.of());

    assertTrue(bigLakeCatalogUsingMockClient.listTables(Namespace.of()).isEmpty());
    verify(mockBigLakeClient, times(1))
        .listDatabases(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID));
  }

  @Test
  public void testListTables_emptyNamespace_checkCatalogEmptiness() {
    when(mockBigLakeClient.listDatabases(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID)))
        .thenReturn(ImmutableList.of(Database.getDefaultInstance()));

    List<TableIdentifier> result = bigLakeCatalogUsingMockClient.listTables(Namespace.of());
    assertEquals(1, result.size());
    assertEquals(TableIdentifier.of("placeholder"), result.get(0));
  }

  @Test
  public void testDropNamespace_failWhenInvalid() throws Exception {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                bigLakeCatalogUsingMockClient.dropNamespace(
                    Namespace.of(new String[] {"n0", "n1"})));
    assertEquals(
        "BigLake catalog namespace can have zero (catalog) or one level (database), invalid"
            + " namespace: n0.n1",
        exception.getMessage());
  }

  @Test
  public void testSetProperties_failWhenNamespacesAreInvalid() throws Exception {
    assertFalse(
        bigLakeCatalogUsingMockClient.setProperties(
            Namespace.of(new String[] {}), ImmutableMap.of()));
    assertFalse(
        bigLakeCatalogUsingMockClient.setProperties(
            Namespace.of(new String[] {"db", "tbl"}), ImmutableMap.of()));
  }

  @Test
  public void testSetProperties_succeedForDatabase() throws Exception {
    when(mockBigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(
                    HiveDatabaseOptions.newBuilder()
                        .putParameters("key1", "value1")
                        .putParameters("key2", "value2"))
                .build());

    assertTrue(
        bigLakeCatalogUsingMockClient.setProperties(
            Namespace.of(new String[] {"db"}),
            ImmutableMap.of("key2", "value222", "key3", "value3")));
    verify(mockBigLakeClient, times(1))
        .updateDatabaseParameters(
            DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db"),
            ImmutableMap.of("key1", "value1", "key2", "value222", "key3", "value3"));
  }

  @Test
  public void testRemoveProperties_failWhenNamespacesAreInvalid() throws Exception {
    assertFalse(
        bigLakeCatalogUsingMockClient.removeProperties(
            Namespace.of(new String[] {}), ImmutableSet.of()));
    assertFalse(
        bigLakeCatalogUsingMockClient.removeProperties(
            Namespace.of(new String[] {"db", "tbl"}), ImmutableSet.of()));
  }

  @Test
  public void testRemoveProperties_succeedForDatabase() throws Exception {
    when(mockBigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(
                    HiveDatabaseOptions.newBuilder()
                        .putParameters("key1", "value1")
                        .putParameters("key2", "value2"))
                .build());

    assertTrue(
        bigLakeCatalogUsingMockClient.removeProperties(
            Namespace.of(new String[] {"db"}), ImmutableSet.of("key1", "key3")));
    verify(mockBigLakeClient, times(1))
        .updateDatabaseParameters(
            DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db"),
            ImmutableMap.of("key2", "value2"));
  }

  @Test
  public void testLoadNamespaceMetadata_catalogAsExpected() throws Exception {
    assertTrue(
        bigLakeCatalogUsingMockClient
            .loadNamespaceMetadata(Namespace.of(new String[] {}))
            .isEmpty());
    verify(mockBigLakeClient, times(1))
        .getCatalog(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID));
  }

  @Test
  public void testLoadNamespaceMetadata_databaseAsExpected() throws Exception {
    when(mockBigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
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
        bigLakeCatalogUsingMockClient.loadNamespaceMetadata(Namespace.of(new String[] {"db"})));
  }

  @Test
  public void testLoadNamespaceMetadata_failWhenInvalid() throws Exception {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                bigLakeCatalogUsingMockClient.loadNamespaceMetadata(
                    Namespace.of(new String[] {"n0", "n1"})));
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
        mockBigLakeClient);

    catalog.createNamespace(Namespace.of(new String[] {}), ImmutableMap.of());
    verify(mockBigLakeClient, times(1))
        .createCatalog(
            CatalogName.of(GCP_PROJECT, GCP_REGION, "customized_catalog"),
            Catalog.getDefaultInstance());
  }

  @Test
  public void testNewTableOps_failedForInvalidNamespace() throws Exception {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                bigLakeCatalogUsingMockClient.newTableOps(
                    TableIdentifier.of(Namespace.of("n0", "n1"), "tbl")));
    assertEquals(
        "BigLake database namespace must use format <catalog>.<database>, invalid namespace: n0.n1",
        exception.getMessage());
  }
}
