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
package org.apache.iceberg.gcp.bigquery.metastore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableReference;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.bigquery.BigQueryClient;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class BigQueryMetastoreCatalogTest {

  @TempDir private File tempFolder;

  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "us";
  private static final String CATALOG_ID = "bqms";
  private static final String DATASET_ID = "db";
  private static final String TABLE_ID = "tbl";
  private static final DatasetReference DATASET_REFERENCE =
      new DatasetReference().setProjectId(GCP_PROJECT).setDatasetId(DATASET_ID);
  private static final TableReference TABLE_REFERENCE =
      new TableReference().setProjectId(GCP_PROJECT).setDatasetId(DATASET_ID).setTableId(TABLE_ID);

  private final BigQueryClient bigQueryClient = mock(BigQueryClient.class);

  private BigQueryMetastoreCatalog bigQueryMetastoreCatalog;
  private String warehouseLocation;

  @BeforeEach
  public void before() {
    this.bigQueryMetastoreCatalog = new BigQueryMetastoreCatalog();
    this.bigQueryMetastoreCatalog.setConf(new Configuration());
    this.warehouseLocation = tempFolder.toPath().resolve("hive-warehouse").toString();

    bigQueryMetastoreCatalog.initialize(
        CATALOG_ID,
        /* properties= */ ImmutableMap.of(
            GCPProperties.PROJECT_ID,
            GCP_PROJECT,
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation),
        GCP_PROJECT,
        GCP_REGION,
        bigQueryClient);
  }

  @Test
  public void testDefaultWarehouseWithDatabaseLocation_asExpected() {
    when(bigQueryClient.getDataset(DATASET_REFERENCE))
        .thenReturn(
            new Dataset()
                .setExternalCatalogDatasetOptions(
                    new ExternalCatalogDatasetOptions()
                        .setDefaultStorageLocationUri("build/db_folder")));

    assertEquals(
        "build/db_folder/table",
        bigQueryMetastoreCatalog.defaultWarehouseLocation(TableIdentifier.of(DATASET_ID, "table")));
  }

  @Test
  public void testDefaultWarehouseWithoutDatabaseLocation_asExpected() {
    when(bigQueryClient.getDataset(DATASET_REFERENCE))
        .thenReturn(
            new Dataset().setExternalCatalogDatasetOptions(new ExternalCatalogDatasetOptions()));

    assertEquals(
        warehouseLocation + "/db.db/table",
        bigQueryMetastoreCatalog.defaultWarehouseLocation(TableIdentifier.of(DATASET_ID, "table")));
  }

  @Test
  public void testCreateTable_succeedWhenNotExist() throws Exception {
    // The table to create does not exist.
    TableIdentifier tableIdentifier = TableIdentifier.of(DATASET_ID, TABLE_ID);
    Schema schema = BigQueryMetastoreTestUtils.getTestSchema();

    when(bigQueryClient.getTable(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    Table createdTable =
        BigQueryMetastoreTestUtils.createTestTable(
            tempFolder, bigQueryMetastoreCatalog, TABLE_REFERENCE);
    reset(bigQueryClient);
    when(bigQueryClient.getTable(TABLE_REFERENCE)).thenReturn(createdTable, createdTable);

    org.apache.iceberg.Table loadedTable = bigQueryMetastoreCatalog.loadTable(tableIdentifier);
    assertEquals(SchemaParser.toJson(schema), SchemaParser.toJson(loadedTable.schema()));

    // Creates a table that already exists.
    Exception exception =
        assertThrows(
            AlreadyExistsException.class,
            () ->
                bigQueryMetastoreCatalog
                    .buildTable(tableIdentifier, schema)
                    .withLocation(tempFolder.toPath().resolve("new_tbl").toString())
                    .createTransaction()
                    .commitTransaction());
    assertTrue(exception.getMessage().contains("already exist"));
  }

  @Test
  public void testListTables_asExpected() {
    when(bigQueryClient.listTables(DATASET_REFERENCE, false))
        .thenReturn(
            ImmutableList.of(
                new Tables()
                    .setTableReference(
                        new TableReference()
                            .setProjectId(GCP_PROJECT)
                            .setDatasetId(DATASET_ID)
                            .setTableId("tbl0")),
                new Tables()
                    .setTableReference(
                        new TableReference()
                            .setProjectId(GCP_PROJECT)
                            .setDatasetId(DATASET_ID)
                            .setTableId("tbl1"))));

    List<TableIdentifier> result = bigQueryMetastoreCatalog.listTables(Namespace.of(DATASET_ID));
    assertEquals(2, result.size());
    assertEquals(TableIdentifier.of(DATASET_ID, "tbl0"), result.get(0));
    assertEquals(TableIdentifier.of(DATASET_ID, "tbl1"), result.get(1));
  }

  @Test
  public void testListTables_throwsOnInvalidNamespace_emptyNamespace() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigQueryMetastoreCatalog.listTables(Namespace.of()));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"\" has 0"
            + " levels",
        exception.getMessage());
  }

  @Test
  public void testListTables_throwsOnInvalidNamespace_nestedNamespace() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigQueryMetastoreCatalog.listTables(Namespace.of("n0", "n1")));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"n0.n1\" has"
            + " 2 levels",
        exception.getMessage());
  }

  @Test
  public void testDropTable_failsWhenTableNotFound() {
    when(bigQueryClient.getTable(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    doThrow(new NoSuchTableException("error message deleteTable"))
        .when(bigQueryClient)
        .deleteTable(TABLE_REFERENCE);

    assertFalse(
        bigQueryMetastoreCatalog.dropTable(
            TableIdentifier.of(DATASET_ID, TABLE_ID), /* purge= */ false));
  }

  @Test
  public void testDropTable_succeedsWhenTableExists_deleteFiles() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of(DATASET_ID, TABLE_ID);

    when(bigQueryClient.getTable(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    Table createdTable =
        BigQueryMetastoreTestUtils.createTestTable(
            tempFolder, bigQueryMetastoreCatalog, TABLE_REFERENCE);
    String tableDir =
        createdTable.getExternalCatalogTableOptions().getStorageDescriptor().getLocationUri();
    assertTrue(BigQueryMetastoreTestUtils.getIcebergMetadataFilePath(tableDir).isPresent());

    reset(bigQueryClient);
    when(bigQueryClient.getTable(TABLE_REFERENCE)).thenReturn(createdTable, createdTable);

    bigQueryMetastoreCatalog.dropTable(tableIdentifier, /* purge= */ false);
    assertTrue(BigQueryMetastoreTestUtils.getIcebergMetadataFilePath(tableDir).isPresent());

    bigQueryMetastoreCatalog.dropTable(tableIdentifier, /* purge= */ true);
    assertFalse(BigQueryMetastoreTestUtils.getIcebergMetadataFilePath(tableDir).isPresent());
  }

  @Test
  public void testRenameTable_sameDatabase_unsupported() {
    when(bigQueryClient.renameTable(TABLE_REFERENCE, "newTableId"))
        .thenThrow(
            new AssertionError("You should not make this API call, it's unsupported anyway!"));

    ServiceFailureException exception =
        assertThrows(
            ServiceFailureException.class,
            () ->
                bigQueryMetastoreCatalog.renameTable(
                    TableIdentifier.of(DATASET_ID, TABLE_ID),
                    TableIdentifier.of(DATASET_ID, "newTableId")));
    assertEquals(
        "Table rename operation is unsupported. "
            + "Try the SQL operation directly on BigQuery: "
            + "\"ALTER TABLE tbl RENAME TO newTableId;\"",
        exception.getMessage());
  }

  @Test
  public void testRenameTable_differentDatabase_fail() {
    when(bigQueryClient.renameTable(TABLE_REFERENCE, "newTableId"))
        .thenReturn(
            new Table()
                .setTableReference(
                    new TableReference()
                        .setProjectId(TABLE_REFERENCE.getProjectId())
                        .setDatasetId(TABLE_REFERENCE.getDatasetId())
                        .setTableId("newTableId")));

    Exception exception =
        assertThrows(
            ValidationException.class,
            () ->
                bigQueryMetastoreCatalog.renameTable(
                    TableIdentifier.of(DATASET_ID, TABLE_ID),
                    TableIdentifier.of("A different DATASET_ID", "newTableId")));
    assertEquals("New table name must be in the same namespace", exception.getMessage());
  }

  @Test
  public void testCreateNamespace_createDatabase() {
    Map<String, String> metadata = ImmutableMap.of();
    String dbDir = warehouseLocation + String.format("/%s.db", DATASET_ID);
    Dataset dataset =
        new Dataset()
            .setDatasetReference(DATASET_REFERENCE)
            .setLocation(GCP_REGION)
            .setExternalCatalogDatasetOptions(
                new ExternalCatalogDatasetOptions()
                    .setParameters(metadata)
                    .setDefaultStorageLocationUri(dbDir));
    when(bigQueryClient.createDataset(dataset)).thenReturn(dataset);

    bigQueryMetastoreCatalog.createNamespace(Namespace.of(DATASET_ID), metadata);

    verify(bigQueryClient, times(1)).createDataset(dataset);
  }

  @Test
  public void testCreateNamespace_failsWhenInvalid_tooLong() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                bigQueryMetastoreCatalog.createNamespace(
                    Namespace.of("n0", "n1"), ImmutableMap.of()));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"n0.n1\" has"
            + " 2 levels",
        exception.getMessage());
  }

  @Test
  public void testCreateNamespace_failsWhenInvalid_emptyNamespace() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigQueryMetastoreCatalog.createNamespace(Namespace.of(), ImmutableMap.of()));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"\" has 0"
            + " levels",
        exception.getMessage());
  }

  @Test
  public void testListNamespaces_asExpected() {
    when(bigQueryClient.listDatasets(GCP_PROJECT))
        .thenReturn(
            ImmutableList.of(
                new Datasets()
                    .setDatasetReference(
                        new DatasetReference().setProjectId(GCP_PROJECT).setDatasetId("db0")),
                new Datasets()
                    .setDatasetReference(
                        new DatasetReference().setProjectId(GCP_PROJECT).setDatasetId("db1"))));

    List<Namespace> result = bigQueryMetastoreCatalog.listNamespaces(Namespace.of());

    assertEquals(2, result.size());
    assertEquals(Namespace.of("db0"), result.get(0));
    assertEquals(Namespace.of("db1"), result.get(1));
  }

  @Test
  public void testListNamespaces_emptyWhenInvalid() {
    assertTrue(bigQueryMetastoreCatalog.listNamespaces(Namespace.of(DATASET_ID)).isEmpty());
  }

  @Test
  public void testDropNamespace_deleteDataset_success() {
    doNothing().when(bigQueryClient).deleteDataset(DATASET_REFERENCE);

    bigQueryMetastoreCatalog.dropNamespace(Namespace.of(DATASET_ID));
  }

  @Test
  public void testDropNamespace_throwsOnInvalidNamespace_emptyNamespace() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigQueryMetastoreCatalog.dropNamespace(Namespace.of()));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"\" has 0"
            + " levels",
        exception.getMessage());
  }

  @Test
  public void testSetProperties_throwsOnInvalidNamespace_emptyNamespace() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigQueryMetastoreCatalog.setProperties(Namespace.of(), ImmutableMap.of()));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"\" has 0"
            + " levels",
        exception.getMessage());
  }

  @Test
  public void testDropNamespace_throwsOnInvalidNamespace_nestedNamespace() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigQueryMetastoreCatalog.dropNamespace(Namespace.of("n0", "n1")));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"n0.n1\" has"
            + " 2 levels",
        exception.getMessage());
  }

  @Test
  public void testSetProperties_throwsOnInvalidNamespace_nestedNamespace() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                bigQueryMetastoreCatalog.setProperties(
                    Namespace.of("n0", "n1"), ImmutableMap.of()));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"n0.n1\" has"
            + " 2 levels",
        exception.getMessage());
  }

  @Test
  public void testSetProperties_succeedForDataset() {
    when(bigQueryClient.getDataset(DATASET_REFERENCE))
        .thenReturn(
            new Dataset()
                .setExternalCatalogDatasetOptions(
                    new ExternalCatalogDatasetOptions()
                        .setParameters(ImmutableMap.of("key1", "value1", "key2", "value2"))));
    when(bigQueryClient.setDatasetParameters(DATASET_REFERENCE, ImmutableMap.of("key3", "value3")))
        .thenReturn(
            new Dataset()
                .setExternalCatalogDatasetOptions(
                    new ExternalCatalogDatasetOptions()
                        .setParameters(
                            ImmutableMap.of(
                                "key1", "value1", "key2", "value2", "key3", "value3"))));

    assertTrue(
        bigQueryMetastoreCatalog.setProperties(
            Namespace.of(DATASET_ID),
            ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3")));
  }

  @Test
  public void testRemoveProperties_throwsOnInvalidNamespace_emptyNamespace() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigQueryMetastoreCatalog.setProperties(Namespace.of(), ImmutableMap.of()));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"\" has 0"
            + " levels",
        exception.getMessage());
  }

  @Test
  public void testRemoveProperties_throwsOnInvalidNamespace_nestedNamespace() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                bigQueryMetastoreCatalog.removeProperties(
                    Namespace.of("n0", "n1"), ImmutableSet.of()));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"n0.n1\" has"
            + " 2 levels",
        exception.getMessage());
  }

  @Test
  public void testRemoveProperties_succeedForDatabase() {
    when(bigQueryClient.getDataset(DATASET_REFERENCE))
        .thenReturn(
            new Dataset()
                .setExternalCatalogDatasetOptions(
                    new ExternalCatalogDatasetOptions()
                        .setParameters(ImmutableMap.of("key1", "value1", "key2", "value2"))));
    when(bigQueryClient.removeDatasetParameters(DATASET_REFERENCE, ImmutableSet.of("key1")))
        .thenReturn(
            new Dataset()
                .setExternalCatalogDatasetOptions(
                    new ExternalCatalogDatasetOptions()
                        .setParameters(ImmutableMap.of("key1", "value1"))));

    assertTrue(
        bigQueryMetastoreCatalog.removeProperties(
            Namespace.of(DATASET_ID), ImmutableSet.of("key1", "key3")));
  }

  @Test
  public void testLoadNamespaceMetadata_catalogAsExpected() {
    when(bigQueryClient.getDataset(DATASET_REFERENCE)).thenReturn(new Dataset());

    assertTrue(bigQueryMetastoreCatalog.loadNamespaceMetadata(Namespace.of(DATASET_ID)).isEmpty());
  }

  @Test
  public void testLoadNamespaceMetadata_datasetAsExpected() {
    Map<String, String> metadata = ImmutableMap.of("key1", "value1", "key2", "value2");
    String dbDir = warehouseLocation + String.format("/%s.db", DATASET_ID);
    Dataset dataset =
        new Dataset()
            .setDatasetReference(DATASET_REFERENCE)
            .setType("BIGQUERY_METASTORE")
            .setExternalCatalogDatasetOptions(
                new ExternalCatalogDatasetOptions()
                    .setParameters(metadata)
                    .setDefaultStorageLocationUri(dbDir));
    when(bigQueryClient.createDataset(dataset)).thenReturn(dataset);

    when(bigQueryClient.getDataset(DATASET_REFERENCE)).thenReturn(dataset);

    assertEquals(
        ImmutableMap.of("location", dbDir, "key1", "value1", "key2", "value2"),
        bigQueryMetastoreCatalog.loadNamespaceMetadata(Namespace.of(DATASET_ID)));
  }

  @Test
  public void testLoadNamespaceMetadata_failWhenInvalid() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> bigQueryMetastoreCatalog.loadNamespaceMetadata(Namespace.of("n0", "n1")));
    assertEquals(
        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"n0.n1\" has"
            + " 2 levels",
        exception.getMessage());
  }

  @Test
  public void testName_asExpected() {
    assertEquals("bqms", bigQueryMetastoreCatalog.name());
  }

  @Test
  public void testProperties_asExpected() {
    assertEquals(
        ImmutableMap.of("gcp_project", GCP_PROJECT, "warehouse", warehouseLocation),
        bigQueryMetastoreCatalog.properties());
  }

  @Test
  public void testNewTableOps_asExpected() {
    assertNotNull(bigQueryMetastoreCatalog.newTableOps(TableIdentifier.of(DATASET_ID, TABLE_ID)));
  }

  @Test
  public void testNewTableOps_doesNotFailForInvalidNamespace() {
    assertNotNull(
        bigQueryMetastoreCatalog.newTableOps(
            TableIdentifier.of(Namespace.of("n0", TABLE_ID), TABLE_ID)));
  }
}
