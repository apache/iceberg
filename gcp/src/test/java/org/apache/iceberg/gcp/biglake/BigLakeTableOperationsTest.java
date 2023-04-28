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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AbortedException;
import com.google.cloud.bigquery.biglake.v1.Database;
import com.google.cloud.bigquery.biglake.v1.DatabaseName;
import com.google.cloud.bigquery.biglake.v1.HiveDatabaseOptions;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.TableName;
import io.grpc.Status.Code;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class BigLakeTableOperationsTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "us";
  private static final String CATALOG_ID = "biglake";
  private static final String DB_ID = "db";
  private static final String TABLE_ID = "tbl";
  private static final TableName TABLE_NAME =
      TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, DB_ID, TABLE_ID);
  private static final TableIdentifier SPARK_TABLE_ID = TableIdentifier.of(DB_ID, TABLE_ID);

  @Mock private BigLakeClient bigLakeClient;

  private BigLakeCatalog bigLakeCatalog;
  private String warehouseLocation;
  private BigLakeTableOperations tableOps;

  @Before
  public void before() throws Exception {
    this.bigLakeCatalog = new BigLakeCatalog();
    this.warehouseLocation = tempFolder.newFolder("hive-warehouse").toString();

    bigLakeCatalog.setConf(new Configuration());
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
    this.tableOps = (BigLakeTableOperations) bigLakeCatalog.newTableOps(SPARK_TABLE_ID);
  }

  @Test
  public void testDoFresh_fetchLatestMetadataFromBigLake() throws Exception {
    Table createdTable = createTestTable();
    reset(bigLakeClient);
    when(bigLakeClient.getTable(TABLE_NAME)).thenReturn(createdTable);

    tableOps.refresh();
    assertEquals(
        createdTable
            .getHiveOptions()
            .getParametersOrDefault(BigLakeTestUtils.METADATA_LOCATION_PROP, ""),
        tableOps.currentMetadataLocation());

    reset(bigLakeClient);
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenThrow(new NoSuchTableException("error message getTable"));
    // Refresh fails when table is not found but metadata already presents.
    assertThrows(NoSuchTableException.class, () -> tableOps.refresh());
  }

  @Test
  public void testDoFresh_failForNonIcebergTable() throws Exception {
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenReturn(Table.newBuilder().setName(TABLE_NAME.toString()).build());

    Exception exception = assertThrows(IllegalArgumentException.class, () -> tableOps.refresh());
    assertTrue(exception.getMessage().contains("metadata location not found"));
  }

  @Test
  public void testDoFresh_noOpWhenMetadataAndTableNotFound() throws Exception {
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenThrow(new NoSuchTableException("error message getTable"));
    // Table not found won't cause errors when the metadata is null.
    assertEquals(null, tableOps.currentMetadataLocation());
    tableOps.refresh();
  }

  @Test
  public void testTableName_asExpected() throws Exception {
    assertEquals("biglake.db.tbl", tableOps.tableName());
  }

  @Test
  public void testDoCommit_useEtagForUpdateTable() throws Exception {
    Table createdTable = createTestTable();
    Table tableWithEtag = createdTable.toBuilder().setEtag("etag").build();
    reset(bigLakeClient);
    when(bigLakeClient.getTable(TABLE_NAME)).thenReturn(tableWithEtag, tableWithEtag);

    org.apache.iceberg.Table loadedTable = bigLakeCatalog.loadTable(SPARK_TABLE_ID);

    when(bigLakeClient.updateTableParameters(any(), any(), any())).thenReturn(tableWithEtag);
    loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ArgumentCaptor<TableName> nameCaptor = ArgumentCaptor.forClass(TableName.class);
    ArgumentCaptor<String> etagCaptor = ArgumentCaptor.forClass(String.class);
    verify(bigLakeClient, times(1))
        .updateTableParameters(nameCaptor.capture(), any(), etagCaptor.capture());
    assertEquals(TABLE_NAME, nameCaptor.getValue());
    assertEquals("etag", etagCaptor.getValue());
  }

  @Test
  public void testDoCommit_failWhenEtagMismatch() throws Exception {
    Table createdTable = createTestTable();
    Table tableWithEtag = createdTable.toBuilder().setEtag("etag").build();
    reset(bigLakeClient);
    when(bigLakeClient.getTable(TABLE_NAME)).thenReturn(tableWithEtag, tableWithEtag);

    org.apache.iceberg.Table loadedTable = bigLakeCatalog.loadTable(SPARK_TABLE_ID);

    when(bigLakeClient.updateTableParameters(any(), any(), any()))
        .thenThrow(
            new AbortedException(
                new RuntimeException("error message etag mismatch"),
                GrpcStatusCode.of(Code.ABORTED),
                false));
    assertThrows(
        CommitFailedException.class,
        () -> loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit());
  }

  @Test
  public void testDoCommit_failWhenMetadataLocationDiff() throws Exception {
    Table createdTable = createTestTable();
    Table tableWithEtag = createdTable.toBuilder().setEtag("etag").build();
    Table.Builder tableWithNewMetadata = tableWithEtag.toBuilder();
    tableWithNewMetadata
        .getHiveOptionsBuilder()
        .putParameters(BigLakeTestUtils.METADATA_LOCATION_PROP, "a new location");

    reset(bigLakeClient);
    // Two invocations, for loadTable and commit.
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenReturn(tableWithEtag, tableWithNewMetadata.build());

    org.apache.iceberg.Table loadedTable = bigLakeCatalog.loadTable(SPARK_TABLE_ID);

    when(bigLakeClient.updateTableParameters(any(), any(), any())).thenReturn(tableWithEtag);
    assertThrows(
        CommitFailedException.class,
        () -> loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit());
  }

  @Test
  public void testCreateTable_doCommitSucceeds() throws Exception {
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenThrow(new NoSuchTableException("error message getTable"));
    when(bigLakeClient.getDatabase(DatabaseName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, "db")))
        .thenReturn(
            Database.newBuilder()
                .setHiveOptions(HiveDatabaseOptions.newBuilder().setLocationUri("db_folder"))
                .build());

    Schema schema = BigLakeTestUtils.getTestSchema();
    bigLakeCatalog.createTable(SPARK_TABLE_ID, schema, PartitionSpec.unpartitioned());
    verify(bigLakeClient, times(1)).createTable(eq(TABLE_NAME), any());
  }

  /** Creates a test table to have Iceberg metadata files in place. */
  private Table createTestTable() throws Exception {
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenThrow(new NoSuchTableException("error message getTable"));
    return BigLakeTestUtils.createTestTable(tempFolder, bigLakeCatalog, TABLE_NAME);
  }
}
