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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.testing.LocalChannelProvider;
import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.cloud.bigquery.biglake.v1.Catalog;
import com.google.cloud.bigquery.biglake.v1.CatalogName;
import com.google.cloud.bigquery.biglake.v1.Database;
import com.google.cloud.bigquery.biglake.v1.DatabaseName;
import com.google.cloud.bigquery.biglake.v1.HiveDatabaseOptions;
import com.google.cloud.bigquery.biglake.v1.MetastoreServiceSettings;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.TableName;
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
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class BigLakeCatalogTest extends CatalogTests<BigLakeCatalog> {

  @TempDir private Path temp;

  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "us";
  private static final String CATALOG_ID = "biglake";

  private String warehouseLocation;

  // For tests using a BigLake catalog connecting to a mocked service.
  private MockMetastoreService mockMetastoreService;
  private MockServiceHelper mockServiceHelper;
  private LocalChannelProvider channelProvider;
  private BigLakeCatalog bigLakeCatalogUsingMockService;

  // For tests using a BigLake catalog with a mocked client.
  private BigLakeClient mockBigLakeClient = mock(BigLakeClient.class);;
  private BigLakeCatalog bigLakeCatalogUsingMockClient;

  @BeforeEach
  public void before() throws Exception {
    mockMetastoreService = new MockMetastoreService();
    mockServiceHelper =
        new MockServiceHelper(
            UUID.randomUUID().toString(), Arrays.<MockGrpcService>asList(mockMetastoreService));
    mockServiceHelper.start();

    File warehouse = temp.toFile();
    warehouseLocation = warehouse.getAbsolutePath();

    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            GCPProperties.BIGLAKE_PROJECT_ID,
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

    bigLakeCatalogUsingMockClient = new BigLakeCatalog();
    bigLakeCatalogUsingMockClient.setConf(new Configuration());
    bigLakeCatalogUsingMockClient.initialize(
        CATALOG_ID, properties, GCP_PROJECT, GCP_REGION, mockBigLakeClient);
  }

  @AfterEach
  public void after() throws Exception {
    bigLakeCatalogUsingMockService.close();
    bigLakeCatalogUsingMockClient.close();
    mockServiceHelper.stop();
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
  public void testDefaultWarehouseWithDatabaseLocation_asExpected() {
    when(mockBigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(HiveDatabaseOptions.newBuilder().setLocationUri("db_folder"))
                .build());

    assertThat(
            bigLakeCatalogUsingMockClient.defaultWarehouseLocation(
                TableIdentifier.of("db", "table")))
        .isEqualTo("db_folder/table");
  }

  @Test
  public void testDefaultWarehouseWithoutDatabaseLocation_asExpected() {
    when(mockBigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, "us", CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder().setHiveOptions(HiveDatabaseOptions.getDefaultInstance()).build());

    assertThat(
            bigLakeCatalogUsingMockClient.defaultWarehouseLocation(
                TableIdentifier.of("db", "table")))
        .isEqualTo(warehouseLocation + "/db.db/table");
  }

  @Test
  public void testRenameTable_differentDatabase_fail() {
    assertThatThrownBy(
            () ->
                bigLakeCatalogUsingMockClient.renameTable(
                    TableIdentifier.of("db0", "t1"), TableIdentifier.of("db1", "t2")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot rename table db0.t1 to db1.t2: database must match");
  }

  @Test
  public void testCreateNamespace_createCatalogWhenEmptyNamespace() throws Exception {
    bigLakeCatalogUsingMockClient.createNamespace(Namespace.of(new String[] {}), ImmutableMap.of());
    verify(mockBigLakeClient, times(1))
        .createCatalog(
            CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID), Catalog.getDefaultInstance());
  }

  @Test
  public void testCreateNamespaceShouldFailWhenInvalid() throws Exception {
    assertThatThrownBy(
            () ->
                bigLakeCatalogUsingMockClient.createNamespace(
                    Namespace.of(new String[] {"n0", "n1"}), ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace (too long): n0.n1");
  }

  @Test
  public void testListNamespacesShouldReturnEmptyWhenInvalid() {
    assertThat(bigLakeCatalogUsingMockClient.listNamespaces(Namespace.of("db"))).isEmpty();
  }

  @Test
  public void testDropNamespaceShouldDeleteCatalogWhenEmptyNamespace() {
    bigLakeCatalogUsingMockClient.dropNamespace(Namespace.of(new String[] {}));
    verify(mockBigLakeClient, times(1))
        .deleteCatalog(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID));
  }

  @Test
  public void testListTablesShouldListTablesInAllDbsWhenNamespaceIsEmpty() {
    DatabaseName db1Name = DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db1");
    DatabaseName db2Name = DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db2");

    TableName table1Name = TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db1", "tbl1");
    TableName table2Name = TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db1", "tbl2");
    TableName table3Name = TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db2", "tbl3");

    when(mockBigLakeClient.listDatabases(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID)))
        .thenReturn(
            ImmutableList.of(
                Database.newBuilder().setName(db1Name.toString()).build(),
                Database.newBuilder().setName(db2Name.toString()).build()));

    when(mockBigLakeClient.listTables(db1Name))
        .thenReturn(
            ImmutableList.of(
                Table.newBuilder().setName(table1Name.toString()).build(),
                Table.newBuilder().setName(table2Name.toString()).build()));
    when(mockBigLakeClient.listTables(db2Name))
        .thenReturn(ImmutableList.of(Table.newBuilder().setName(table3Name.toString()).build()));

    List<TableIdentifier> result = bigLakeCatalogUsingMockClient.listTables(Namespace.of());
    assertThat(result)
        .containsExactlyInAnyOrder(
            TableIdentifier.of("db1", "tbl1"),
            TableIdentifier.of("db1", "tbl2"),
            TableIdentifier.of("db2", "tbl3"));
  }

  @Test
  public void testDropNamespaceShouldFailWhenInvalid() throws Exception {
    assertThat(bigLakeCatalogUsingMockClient.dropNamespace(Namespace.of(new String[] {"n0", "n1"})))
        .isFalse();
  }

  @Test
  public void testSetPropertiesShouldFailWhenNamespacesAreInvalid() throws Exception {
    assertThatThrownBy(
            () ->
                bigLakeCatalogUsingMockClient.setProperties(
                    Namespace.of(new String[] {}), ImmutableMap.of()))
        .isInstanceOf(NoSuchNamespaceException.class);

    assertThatThrownBy(
            () ->
                bigLakeCatalogUsingMockClient.setProperties(
                    Namespace.of(new String[] {"db", "tbl"}), ImmutableMap.of()))
        .isInstanceOf(NoSuchNamespaceException.class);
  }

  @Test
  public void testSetPropertiesShouldSucceedForDatabase() throws Exception {
    when(mockBigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(
                    HiveDatabaseOptions.newBuilder()
                        .putParameters("key1", "value1")
                        .putParameters("key2", "value2"))
                .build());

    assertThat(
            bigLakeCatalogUsingMockClient.setProperties(
                Namespace.of(new String[] {"db"}),
                ImmutableMap.of("key2", "value222", "key3", "value3")))
        .isTrue();
    verify(mockBigLakeClient, times(1))
        .updateDatabaseParameters(
            DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db"),
            ImmutableMap.of("key1", "value1", "key2", "value222", "key3", "value3"));
  }

  @Test
  public void testRemovePropertiesShouldFailWhenNamespacesAreInvalid() throws Exception {
    assertThatThrownBy(
            () ->
                bigLakeCatalogUsingMockClient.removeProperties(
                    Namespace.of(new String[] {}), ImmutableSet.of()))
        .isInstanceOf(NoSuchNamespaceException.class);

    assertThatThrownBy(
            () ->
                bigLakeCatalogUsingMockClient.removeProperties(
                    Namespace.of(new String[] {"db", "tbl"}), ImmutableSet.of()))
        .isInstanceOf(NoSuchNamespaceException.class);
  }

  @Test
  public void testRemovePropertiesShouldSucceedForDatabase() throws Exception {
    when(mockBigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(
                    HiveDatabaseOptions.newBuilder()
                        .putParameters("key1", "value1")
                        .putParameters("key2", "value2"))
                .build());

    assertThat(
            bigLakeCatalogUsingMockClient.removeProperties(
                Namespace.of(new String[] {"db"}), ImmutableSet.of("key1", "key3")))
        .isTrue();
    verify(mockBigLakeClient, times(1))
        .updateDatabaseParameters(
            DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db"),
            ImmutableMap.of("key2", "value2"));
  }

  @Test
  public void testLoadNamespaceMetadataAsExpectedForCatalogs() throws Exception {
    assertThat(bigLakeCatalogUsingMockClient.loadNamespaceMetadata(Namespace.of(new String[] {})))
        .isEmpty();
    verify(mockBigLakeClient, times(1))
        .getCatalog(CatalogName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID));
  }

  @Test
  public void testLoadNamespaceMetadataAsExpectedForDatabases() throws Exception {
    when(mockBigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(
                    HiveDatabaseOptions.newBuilder()
                        .setLocationUri("my location uri")
                        .putParameters("key1", "value1")
                        .putParameters("key2", "value2"))
                .build());

    assertThat(
            bigLakeCatalogUsingMockClient.loadNamespaceMetadata(Namespace.of(new String[] {"db"})))
        .containsAllEntriesOf(
            ImmutableMap.of("location", "my location uri", "key1", "value1", "key2", "value2"));
  }

  @Test
  public void testLoadNamespaceMetadataShouldFailWhenInvalid() throws Exception {
    assertThatThrownBy(
            () ->
                bigLakeCatalogUsingMockClient.loadNamespaceMetadata(
                    Namespace.of(new String[] {"n0", "n1"})))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: n0.n1");
  }

  @Test
  public void testNewTableOpsShouldfailedForInvalidNamespace() throws Exception {
    assertThatThrownBy(
            () ->
                bigLakeCatalogUsingMockClient.newTableOps(
                    TableIdentifier.of(Namespace.of("n0", "n1"), "tbl")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage(
            "BigLake database namespace must use format <catalog>.<database>, invalid namespace: n0.n1");
  }
}
