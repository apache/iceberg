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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.bigquery.BigQueryClient;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

public class BigQueryTableOperationsTest {

  @TempDir private File tempFolder;

  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "us";
  private static final String DATASET_ID = "db";
  private static final String TABLE_ID = "tbl";
  private static final TableIdentifier SPARK_TABLE_ID = TableIdentifier.of(DATASET_ID, TABLE_ID);

  private static final TableReference TABLE_REFERENCE =
      new TableReference().setProjectId(GCP_PROJECT).setDatasetId(DATASET_ID).setTableId(TABLE_ID);

  private final BigQueryClient bigQueryClient = mock(BigQueryClient.class);

  private BigQueryMetastoreCatalog bigQueryMetastoreCatalog;
  private BigQueryTableOperations tableOps;

  @BeforeEach
  public void before() {
    this.bigQueryMetastoreCatalog = new BigQueryMetastoreCatalog();
    this.bigQueryMetastoreCatalog.setConf(new Configuration());
    String warehouseLocation = tempFolder.toPath().resolve("hive-warehouse").toString();

    bigQueryMetastoreCatalog.initialize(
        "CATALOG_ID",
        /* properties= */ ImmutableMap.of(
            GCPProperties.PROJECT_ID,
            GCP_PROJECT,
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation),
        GCP_PROJECT,
        GCP_REGION,
        bigQueryClient);
    this.tableOps = (BigQueryTableOperations) bigQueryMetastoreCatalog.newTableOps(SPARK_TABLE_ID);
  }

  @Test
  public void testDoFresh_fetchLatestMetadataFromBigQuery() throws Exception {
    Table createdTable = createTestTable();
    reset(bigQueryClient);
    when(bigQueryClient.getTable(TABLE_REFERENCE)).thenReturn(createdTable);

    tableOps.refresh();
    assertEquals(
        createdTable
            .getExternalCatalogTableOptions()
            .getParameters()
            .getOrDefault(BigQueryMetastoreTestUtils.METADATA_LOCATION_PROP, ""),
        tableOps.currentMetadataLocation());

    reset(bigQueryClient);
    when(bigQueryClient.getTable(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    // Refresh fails when table is not found but metadata already presents.
    assertThrows(NoSuchTableException.class, () -> tableOps.refresh());
  }

  @Test
  public void testDoFresh_failForNonIcebergTable() {
    when(bigQueryClient.getTable(TABLE_REFERENCE))
        .thenReturn(new Table().setTableReference(TABLE_REFERENCE));

    Exception exception = assertThrows(ValidationException.class, () -> tableOps.refresh());
    assertTrue(exception.getMessage().contains("metadata location not found"));
  }

  @Test
  public void testDoFresh_noOpWhenMetadataAndTableNotFound() {
    when(bigQueryClient.getTable(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    // Table not found won't cause errors when the metadata is null.
    assertNull(tableOps.currentMetadataLocation());
    tableOps.refresh();
  }

  @Test
  public void testTableName_asExpected() {
    assertEquals("db.tbl", tableOps.tableName());
  }

  @Test
  public void testDoCommit_useEtagForUpdateTable() throws Exception {
    Table tableWithEtag = createTestTable().setEtag("etag");
    reset(bigQueryClient);
    when(bigQueryClient.getTable(TABLE_REFERENCE)).thenReturn(tableWithEtag, tableWithEtag);

    org.apache.iceberg.Table loadedTable = bigQueryMetastoreCatalog.loadTable(SPARK_TABLE_ID);

    when(bigQueryClient.patchTable(any(), any())).thenReturn(tableWithEtag);
    loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ArgumentCaptor<TableReference> tableReferenceArgumentCaptor =
        ArgumentCaptor.forClass(TableReference.class);
    ArgumentCaptor<Table> tableArgumentCaptor = ArgumentCaptor.forClass(Table.class);
    verify(bigQueryClient, times(1))
        .patchTable(tableReferenceArgumentCaptor.capture(), tableArgumentCaptor.capture());
    assertEquals(TABLE_REFERENCE, tableReferenceArgumentCaptor.getValue());
    assertEquals("etag", tableArgumentCaptor.getValue().getEtag());
  }

  @Test
  public void testDoCommit_failWhenEtagMismatch() throws Exception {
    Table tableWithEtag = createTestTable().setEtag("etag");
    reset(bigQueryClient);
    when(bigQueryClient.getTable(TABLE_REFERENCE)).thenReturn(tableWithEtag, tableWithEtag);

    org.apache.iceberg.Table loadedTable = bigQueryMetastoreCatalog.loadTable(SPARK_TABLE_ID);

    when(bigQueryClient.patchTable(any(), any()))
        .thenThrow(new ValidationException("error message etag mismatch"));
    assertThrows(
        CommitFailedException.class,
        () -> loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit());
  }

  @Test
  public void testDoCommit_failWhenMetadataLocationDiff() throws Exception {
    Table tableWithEtag = createTestTable().setEtag("etag");
    Table tableWithNewMetadata =
        new Table()
            .setEtag("etag")
            .setExternalCatalogTableOptions(
                new ExternalCatalogTableOptions()
                    .setParameters(
                        ImmutableMap.of(
                            BigQueryMetastoreTestUtils.METADATA_LOCATION_PROP, "a/new/location")));

    reset(bigQueryClient);
    // Two invocations, for loadTable and commit.
    when(bigQueryClient.getTable(TABLE_REFERENCE)).thenReturn(tableWithEtag, tableWithNewMetadata);

    org.apache.iceberg.Table loadedTable = bigQueryMetastoreCatalog.loadTable(SPARK_TABLE_ID);

    when(bigQueryClient.patchTable(any(), any())).thenReturn(tableWithEtag);
    assertThrows(
        CommitFailedException.class,
        () -> loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit());
  }

  @Test
  public void testCreateTable_doCommitSucceeds() throws Exception {
    var testTable = createTestTable();
    when(bigQueryClient.createTable(any())).thenReturn(testTable);
    when(bigQueryClient.getDataset(
            new DatasetReference().setProjectId(GCP_PROJECT).setDatasetId(DATASET_ID)))
        .thenReturn(
            new Dataset()
                .setExternalCatalogDatasetOptions(
                    new ExternalCatalogDatasetOptions().setDefaultStorageLocationUri("db_folder")));

    Schema schema = BigQueryMetastoreTestUtils.getTestSchema();
    bigQueryMetastoreCatalog.createTable(SPARK_TABLE_ID, schema, PartitionSpec.unpartitioned());
  }

  /** Creates a test table to have Iceberg metadata files in place. */
  private Table createTestTable() throws Exception {
    when(bigQueryClient.getTable(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    return BigQueryMetastoreTestUtils.createTestTable(
        tempFolder, bigQueryMetastoreCatalog, TABLE_REFERENCE);
  }
}
