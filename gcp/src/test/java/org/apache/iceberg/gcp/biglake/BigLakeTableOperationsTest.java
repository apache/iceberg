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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AbortedException;
import com.google.cloud.bigquery.biglake.v1.HiveTableOptions;
import com.google.cloud.bigquery.biglake.v1.HiveTableOptions.StorageDescriptor;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.TableName;
import io.grpc.Status.Code;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
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

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()));

  @Mock private BigLakeClient bigLakeClient;

  private BigLakeCatalog bigLakeCatalog;
  private String warehouseLocation;
  private BigLakeTableOperations tableOps;

  @Before
  public void before() throws Exception {
    warehouseLocation = tempFolder.newFolder("hive-warehouse").toString();
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            BigLakeCatalog.PROPERTIES_KEY_GCP_PROJECT,
            GCP_PROJECT,
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation);

    bigLakeCatalog = new BigLakeCatalog();
    bigLakeCatalog.setConf(new Configuration());
    bigLakeCatalog.initialize(CATALOG_ID, properties, GCP_PROJECT, GCP_REGION, bigLakeClient);
    this.tableOps = (BigLakeTableOperations) bigLakeCatalog.newTableOps(SPARK_TABLE_ID);
  }

  @Test
  public void testDoCommit_useEtagForUpdateTable() throws Exception {
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenThrow(new NoSuchTableException("error message getTable"));
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
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenThrow(new NoSuchTableException("error message getTable"));
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
  public void testDoFresh_refreshReturnNullForNonIcebergTable() throws Exception {
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenReturn(Table.newBuilder().setName(TABLE_NAME.toString()).build());

    assertEquals(null, tableOps.refresh());
  }

  private Table createTestTable() throws IOException {
    TableIdentifier tableIdent =
        TableIdentifier.of(TABLE_NAME.getDatabase(), TABLE_NAME.getTable());
    String tableDir = tempFolder.newFolder(TABLE_NAME.getTable()).toString();

    bigLakeCatalog
        .buildTable(tableIdent, SCHEMA)
        .withLocation(tableDir)
        .createTransaction()
        .commitTransaction();

    Optional<String> metadataLocation = getAnyIcebergMetadataFilePath(tableDir);
    assertTrue(metadataLocation.isPresent());
    return Table.newBuilder()
        .setName(TABLE_NAME.toString())
        .setHiveOptions(
            HiveTableOptions.newBuilder()
                .putParameters("metadata_location", metadataLocation.get())
                .setStorageDescriptor(StorageDescriptor.newBuilder().setLocationUri(tableDir)))
        .build();
  }

  private static Optional<String> getAnyIcebergMetadataFilePath(String tableDir)
      throws IOException {
    for (File file :
        FileUtils.listFiles(new File(tableDir), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)) {
      if (file.getCanonicalPath().endsWith(".json")) {
        return Optional.of(file.getCanonicalPath());
      }
    }
    return Optional.empty();
  }
}
